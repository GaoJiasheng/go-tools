package prometheus

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func Test_Iterater(t *testing.T) {
	IterBlockMetas("/Users/gavin/Library/Mobile Documents/com~apple~CloudDocs/2code/work/Prometheus/prometheus/data/",
		func(m *metadata.Meta) error {
			fmt.Println(
				time.Unix(m.MinTime/1000, 0).Format("2006-01-02 03:04:05 PM"),
				" ------- ",
				time.Unix(m.MaxTime/1000, 0).Format("2006-01-02 03:04:05 PM"),
				" ------- ",
				m.ULID,
			)
			return nil
		})
}

func Test_Shipper(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stdout)

	startStr := "2020-03-14 06:00:00"
	endStr := "2020-03-15 15:00:00"

	loc, _ := time.LoadLocation("Local")
	startTime, _ := time.ParseInLocation("2006-01-02 15:04:05", startStr, loc)
	endTime, _ := time.ParseInLocation("2006-01-02 15:04:05", endStr, loc)

	shipper := NewPrometheusShipper(logger, "/Users/gavin/Library/Mobile Documents/com~apple~CloudDocs/2code/work/Prometheus/prometheus/data/",
		startTime.Unix(), endTime.Unix(), "bucket-name", "s3.amazon.com", "YOUR-ACCESS-KEY", "YOUR-SECRET-KEY")

	shipper.Start()
}
