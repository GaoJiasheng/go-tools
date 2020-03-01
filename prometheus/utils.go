package prometheus

import (
	"fmt"

	"github.com/prometheus/prometheus/prompb"
)

func MakeOneTimeSeriesExp(labels []prompb.Label) string {
	name := ""
	labelStr := ""
	for _, l := range labels {
		if l.Name == "__name__" {
			name = l.Value
		} else {
			if labelStr != "" {
				labelStr += ","
			}
			labelStr += fmt.Sprintf(`%s="%s"`, l.Name, l.Value)
		}
	}
	return fmt.Sprintf("%s{%s}", name, labelStr)
}
