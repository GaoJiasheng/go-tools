package utils

// TimeRangeSplit return a splited timestamp slice between start and end
func TimeRangeSplit(start, end, step int64) []int64 {
	if start < 0 || end < 0 || start >= end {
		return []int64{}
	}
	timeSlice := make([]int64, 0)
	ts := start - start%step
	for ts < end+step {
		timeSlice = append(timeSlice, ts)
		ts = ts + step
	}
	return timeSlice
}
