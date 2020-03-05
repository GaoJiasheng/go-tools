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

type PromReader struct {
	Address       []string
	StartTs       int64
	EndTs         int64
	DataStep      int64
	MigrationStep int64
	Expression    string
	outer         chan PromReaderOutput
}
type PromReaderOutput struct {
	Start         int64
	End           int64
	MigrationStep int64
	DataStep      int64
	TimeSeries    *[]prompb.TimeSeries
}

func NewPromReader(address []string, start, end int64, dStep, mStep int64, expression string, outer chan PromReaderOutput) *PromReader {
	return &PromReader{
		Address:       address,
		StartTs:       start,
		EndTs:         end,
		DataStep:      dStep,
		MigrationStep: mStep,
		Expression:    expression,
		outer:         outer,
	}
}

func (r PromReader) Read(logger log.Logger) {
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

		allSeries := make([]prompb.TimeSeries, 0)
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
					t := data.Data.Result[i].TranstoStdTimeSeries()
					allSeries = append(allSeries, *t)
				}
				mergeLock.Unlock()
			}(r.Address[i], start, end, r.DataStep)
		}
		wg.Wait()

		//level.Info(logger).Log("module", "prom_read", "msg", fmt.Sprintf("[%s] data fetch successful. step:%d", time.Unix(start, 0).Format("2006-01-02 15:04:05"), end-start))

		r.outer <- PromReaderOutput{
			Start:         start,
			End:           end,
			MigrationStep: r.MigrationStep,
			DataStep:      r.DataStep,
			TimeSeries:    &allSeries,
		}
	}
	close(r.outer)
}
