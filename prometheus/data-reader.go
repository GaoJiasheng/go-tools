package prometheus

/*
 * Query and Aggregate Data from multi-prometheus BE
 * Can specify time range, query expression, step, data cache DIR, multi-prometheus BE
 */

import (
	"github.com/prometheus/prometheus/prompb"
)

type Reader struct {
	Address       []string
	Start         int
	End           int
	DataStep      int
	CacheDir      string
	TimeSeries    []*prompb.TimeSeries // Just fill in the Labels
	MigrationStep int
}

func NewReader(address []string) *Reader {
	return &Reader{
		Address: address,
	}
}

func (r *Reader) ReadTimeSeriesList() {
	data, _ := Query(r.Address[0], `{__name__=~".+"}`)
	timeSeries := make([]*prompb.TimeSeries, 0)
	for i, _ := range data.Data.Result {
		tmp := &prompb.TimeSeries{
			Labels: make([]prompb.Label, 0),
		}
		for lk, lv := range data.Data.Result[i].Metric {
			tmp.Labels = append(tmp.Labels, prompb.Label{
				Name:  lk,
				Value: lv,
			})
		}
		timeSeries = append(timeSeries, tmp)
	}
	r.TimeSeries = timeSeries
}
