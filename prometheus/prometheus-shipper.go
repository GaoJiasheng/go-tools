package prometheus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/prometheus/prometheus/tsdb/fileutil"

	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore/s3"
)

var components = "prometheus-shipper"

type PrometheusShipper struct {
	DataDir   string
	StartTime int64
	EndTime   int64
	Bucket    *s3.Bucket
	Logger    log.Logger
}

/*
 */

func NewPrometheusShipper(logger log.Logger, dataDir string, start, end int64,
	bucketName, endpoint, accesskey, secretkey string) *PrometheusShipper {
	bucket, err := s3.NewBucketWithConfig(
		logger,
		s3.Config{
			Bucket:    bucketName,
			Endpoint:  endpoint,
			AccessKey: accesskey,
			SecretKey: secretkey,
		},
		components,
	)

	if err != nil {
		level.Error(logger).Log("module", components, "msg", fmt.Sprintf("create s3 bucket error: %v", err))
	}

	return &PrometheusShipper{
		DataDir:   dataDir,
		StartTime: start,
		EndTime:   end,
		Bucket:    bucket,
		Logger:    logger,
	}
}

func (shipper *PrometheusShipper) Start() error {
	err := IterBlockMetas(shipper.DataDir,
		func(m *metadata.Meta) error {
			m.Thanos.Labels = map[string]string{"receiver": "shipped-by-tools"}
			m.Thanos.Source = "shipper-tools"
			ctx, _ := context.WithCancel(context.Background())
			if m.MaxTime > shipper.StartTime*1000 && m.MinTime < shipper.EndTime*1000 {
				level.Info(shipper.Logger).Log("block matches : ", m.ULID)
				err := shipper.Upload(ctx, m)
				if err != nil {
					level.Error(shipper.Logger).Log("msg", fmt.Sprintf("block upload failed : %v", err), "block", m.ULID)
					return err
				}
			}
			return nil
		})
	return err
}

func (shipper *PrometheusShipper) Upload(ctx context.Context, meta *metadata.Meta) error {
	level.Info(shipper.Logger).Log("msg", "upload new block", "id", meta.ULID)

	updir := filepath.Join(shipper.DataDir, "thanos", "upload", meta.ULID.String())

	if err := os.RemoveAll(updir); err != nil {
		return errors.Wrap(err, "clean upload directory")
	}
	if err := os.MkdirAll(updir, 0777); err != nil {
		return errors.Wrap(err, "create upload dir")
	}
	defer func() {
		if err := os.RemoveAll(updir); err != nil {
			level.Error(shipper.Logger).Log("msg", "failed to clean upload directory", "err", err)
		}
	}()

	dir := filepath.Join(shipper.DataDir, meta.ULID.String())
	if err := hardlinkBlock(dir, updir); err != nil {
		return errors.Wrap(err, "hard link block")
	}
	/*
		// Attach current labels and write a new meta file with Thanos extensions.
		if lset := s.labels(); lset != nil {
			meta.Thanos.Labels = lset.Map()
		}
	*/
	meta.Thanos.Source = metadata.SourceType(components)

	if err := metadata.Write(shipper.Logger, updir, meta); err != nil {
		return errors.Wrap(err, "write meta file")
	}
	return block.Upload(ctx, shipper.Logger, shipper.Bucket, updir)
}

func hardlinkBlock(src, dst string) error {
	chunkDir := filepath.Join(dst, block.ChunksDirname)

	if err := os.MkdirAll(chunkDir, 0777); err != nil {
		return errors.Wrap(err, "create chunks dir")
	}

	files, err := fileutil.ReadDir(filepath.Join(src, block.ChunksDirname))
	if err != nil {
		return errors.Wrap(err, "read chunk dir")
	}
	for i, fn := range files {
		files[i] = filepath.Join(block.ChunksDirname, fn)
	}
	files = append(files, block.MetaFilename, block.IndexFilename)

	for _, fn := range files {
		if err := os.Link(filepath.Join(src, fn), filepath.Join(dst, fn)); err != nil {
			return errors.Wrapf(err, "hard link file %s", fn)
		}
	}
	return nil
}
