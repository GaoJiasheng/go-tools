package prometheus

import (
	"github.com/go-kit/kit/log"
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
	dataQueue := make(chan *[]prompb.TimeSeries, 100)
	reader := NewPromReader(
		t.PromAddrs,
		t.StartTs,
		t.EndTs,
		t.DataStep,
		t.MigrationStep,
		t.Expression,
		dataQueue,
	)
	go reader.Read(nil)

	for timeSeries := range dataQueue {
		remoteWriteBody := &prompb.WriteRequest{
			Timeseries: *timeSeries,
		}
		RemoteWrite(t.ThanosAddr, remoteWriteBody)
	}
}
