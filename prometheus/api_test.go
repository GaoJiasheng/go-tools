package prometheus

import (
	"testing"
	"time"
)

func TestAPIQuery(t *testing.T) {
	ret, err := Query("http://localhost:9090", `{__name__=~".+"}[1m]`)

	if err != nil {
		t.Errorf("Test Failed: %+v", err)
	} else {
		t.Logf("Test Successful:%d time series", len(ret.Data.Result))
		for i, _ := range ret.Data.Result {
			t.Log(ret.Data.Result[i].TranstoStdTimeSeries())
		}
	}
}

func TestAPIQueryRange(t *testing.T) {
	ret, err := QueryRange("http://localhost:9090",
		`{__name__=~".+"}`,
		time.Now().Unix()-24*3600*7,
		time.Now().Unix()-24*3600*6,
		10000,
	)
	if err != nil {
		t.Errorf("Test Failed: %+v", err)
	} else {
		t.Logf("Test Successful:%d time series", len(ret.Data.Result))
		for i, _ := range ret.Data.Result {
			t.Log(ret.Data.Result[i].TranstoStdTimeSeries())
		}
	}
}
