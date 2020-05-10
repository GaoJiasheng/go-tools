package prometheus

import (
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// get all blocks meta
func IterBlockMetas(dir string, f func(m *metadata.Meta) error) error {
	var metas []*metadata.Meta
	names, err := fileutil.ReadDir(dir)
	if err != nil {
		return errors.Wrap(err, "read tsdb dir")
	}
	for _, n := range names {
		if _, ok := block.IsBlockDir(n); !ok {
			continue
		}
		dir := filepath.Join(dir, n)

		fi, err := os.Stat(dir)
		if err != nil {
			continue
		}
		if !fi.IsDir() {
			continue
		}
		m, err := metadata.Read(dir)
		if err != nil {
			continue
		}
		metas = append(metas, m)
	}
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].BlockMeta.MinTime < metas[j].BlockMeta.MinTime
	})
	for _, m := range metas {

		if err := f(m); err != nil {
			return err
		}
	}
	return nil
}
