package utils

import "testing"

func TestTimeRangeSplit(t *testing.T) {
	t.Log(TimeRangeSplit(1, 100000, 80))
	t.Log(TimeRangeSplit(23, 8888, 10))
}
