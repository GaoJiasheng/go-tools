package prometheus

import (
	"sync"

	"github.com/gaojiasheng/go-tools/utils"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/prompb"
)

/*
 * Query and Aggregate Data from multi-prometheus Backend
 * Can specify time range, query expression, step, data cache DIR, multi-prometheus BE
 * Can support raw data
 */

type Reader struct {
	Address       []string
	StartTs       int64
	EndTs         int64
	DataStep      int64
	MigrationStep int64
	Expression    string
	outer         chan *[]*prompb.TimeSeries
}

func NewReader(address []string, start, end int64, dStep, mStep int64, expression string, outer chan *[]*prompb.TimeSeries) *Reader {
	return &Reader{
		Address:       address,
		StartTs:       start,
		EndTs:         end,
		DataStep:      dStep,
		MigrationStep: mStep,
		Expression:    expression,
		outer:         outer,
	}
}

func (r Reader) Read(logger log.Logger) {
	// long term time duration
	// split to a ts series in order to be start & end of time range
	tss := utils.TimeRangeSplit(r.StartTs, r.EndTs, r.MigrationStep)
	// multiple BE
	for i, _ := range tss {
		if i == len(tss)-1 {
			break
		}
		start := tss[i]
		end := tss[i+1]

		allSeries := make([]*prompb.TimeSeries, 0)
		mergeLock := sync.Mutex{}
		wg := sync.WaitGroup{}
		for i, _ := range r.Address {
			wg.Add(1)
			go func(addr string, start, end, dStep int64) {
				defer wg.Done()
				data, err := QueryRange(addr, r.Expression, start, end, dStep)
				if err != nil {
					level.Error(logger).Log("msg", "query range failed.", "err", err.Error())
					return
				}
				mergeLock.Lock()
				for i, _ := range data.Data.Result {
					allSeries = append(allSeries, data.Data.Result[i].TranstoStdTimeSeries())
				}
				mergeLock.Unlock()
			}(r.Address[i], start, end, r.DataStep)
		}
		wg.Wait()

		r.outer <- &allSeries
	}
	close(r.outer)
}
