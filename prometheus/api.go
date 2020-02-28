package prometheus

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/gaojiasheng/go-tools/utils"
	"github.com/parnurzeal/gorequest"
)

type ReadMetricsResponse struct {
	Status string                  `json:"status"`
	Data   ReadMetricsResponseData `json:"data"`
}

type ReadMetricsResponseData struct {
	Result []ReadMetricsResponseSeries `json:"result"`
}

type ReadMetricsResponseSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

func Query(address, expression string) (*ReadMetricsResponse, error) {
	target := utils.MakeURL(address, "/api/v1/query", map[string]string{})
	param := map[string]string{"query": expression}
	paramStr, _ := json.Marshal(param)

	data := &ReadMetricsResponse{}
	_, _, errs := gorequest.New().Post(target).Type("multipart").Send(string(paramStr)).EndStruct(data)
	if errs != nil {
		fmt.Println(errs)
		return nil, fmt.Errorf("%v", errs)
	}
	return data, nil
}

func QueryRange(address, expression string, start, end, step int64) (*ReadMetricsResponse, error) {
	target := utils.MakeURL(address, "/api/v1/query_range", map[string]string{})
	param := map[string]string{
		"query": expression,
		"start": strconv.Itoa(int(start)),
		"end":   strconv.Itoa(int(end)),
		"step":  strconv.Itoa(int(step)),
	}
	paramStr, _ := json.Marshal(param)

	data := &ReadMetricsResponse{}
	_, _, errs := gorequest.New().Post(target).Type("multipart").Send(string(paramStr)).EndStruct(data)
	if errs != nil {
		fmt.Println(errs)
		return nil, fmt.Errorf("%v", errs)
	}
	return data, nil
}
