package prometheus

import (
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/prompb"
)

type PromToThanosTransporter struct {
	PromAddrs     []string
	ThanosAddr    string
	StartTs       int64
	EndTs         int64
	DataStep      int64
	MigrationStep int64
	Expression    string
}

func NewPromToThanosTransporter(
	promAddrs []string,
	thanosAddr string,
	start int64,
	end int64,
	dStep int64,
	mStep int64,
	exp string,
) *PromToThanosTransporter {
	return &PromToThanosTransporter{
		PromAddrs:     promAddrs,
		ThanosAddr:    thanosAddr,
		StartTs:       start,
		EndTs:         end,
		DataStep:      dStep,
		MigrationStep: mStep,
		Expression:    exp,
	}
}

func (t PromToThanosTransporter) Start(logger log.Logger) {
	dataQueue := make(chan *PromReaderOutput, 100)
	reader := NewPromReader(
		t.PromAddrs,
		t.StartTs,
		t.EndTs,
		t.DataStep,
		t.MigrationStep,
		t.Expression,
		dataQueue,
	)
	go reader.Read(logger)

	for data := range dataQueue {
		body := data
		splitSize := 20000
		wg := sync.WaitGroup{}
		for i := 0; i < len(*body.TimeSeries); i = i + splitSize {
			end := i + splitSize
			if end > len(*body.TimeSeries) {
				end = len(*body.TimeSeries)
			}

			wg.Add(1)
			go func(end, i int) {
				defer wg.Done()
				tmp := make([]prompb.TimeSeries, end-i)

				k := 0
				for j := i; j < end; j++ {
					tmp[k] = *(*body.TimeSeries)[j]
					k = k + 1
				}

				remoteWriteBody := &prompb.WriteRequest{
					Timeseries: tmp,
				}
				err := RemoteWrite(t.ThanosAddr, remoteWriteBody)

				if err != nil {
					level.Error(logger).Log(
						"module", "remote_write",
						"msg", err.Error(),
						"start", time.Unix(body.Start, 0).Format("2006-01-02 15:04:05"),
						"migrate_step", body.MigrationStep,
						"data_step", body.DataStep,
						"time_series_num", end-i,
					)
				} else {
					level.Info(logger).Log("module", "remote_write",
						"msg", "successful",
						"start", time.Unix(body.Start, 0).Format("2006-01-02 15:04:05"),
						"migrate_step", body.MigrationStep,
						"data_step", body.DataStep,
						"time_series_num", end-i,
					)
				}
			}(end, i)
		}
		wg.Wait()
	}
}
