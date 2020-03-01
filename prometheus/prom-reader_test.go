package prometheus

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

func TestReader(t *testing.T) {
	resultCh := make(chan *[]prompb.TimeSeries, 10)
	reader := NewPromReader(
		[]string{"http://localhost:9090"},
		time.Now().Unix()-3600*24*7,
		time.Now().Unix()-3600*24*6,
		15,
		60,
		`{__name__=~'.+'}`,
		resultCh,
	)

	go reader.Read(nil)
	for timeSeries := range resultCh {
		fmt.Println(len(*timeSeries))
	}
}
