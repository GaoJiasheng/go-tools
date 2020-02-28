package utils

import (
	`fmt`
	`net/url`
)

func MakeURL(addr, path string, params map[string]string) string {
	baseUrl, err := url.Parse(addr + path)
	if err != nil {
		fmt.Println("Malformed URL: ", err.Error())
		return ""
	}

	if len(params) > 0 {
		baseUrl.Path += "?"
		urlParam := url.Values{}
		for k, v := range params {
			urlParam.Add(k, v)
		}

		// Add Query Parameters to the URL
		baseUrl.RawQuery = urlParam.Encode() // Escape Query Parameters
	}
	return baseUrl.String()
}

