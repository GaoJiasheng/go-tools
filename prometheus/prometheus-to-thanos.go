package prometheus

import (
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
	dataQueue := make(chan PromReaderOutput, 100)
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

	for body := range dataQueue {
		remoteWriteBody := &prompb.WriteRequest{
			Timeseries: *body.TimeSeries,
		}
		err := RemoteWrite(t.ThanosAddr, remoteWriteBody)

		if err != nil {
			level.Error(logger).Log("module", "remote_write", "msg", err.Error())
		} else {
			level.Info(logger).Log("module", "remote_write",
				"msg", "successful",
				"start", time.Unix(body.Start, 0).Format("2006-01-02 15:04:05"),
				"migrate_step", body.MigrationStep,
				"data_step", body.DataStep,
				"time_series_num", len(*body.TimeSeries),
			)
		}
	}
}
