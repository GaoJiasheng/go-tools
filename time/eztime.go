package eztime

import "time"

func GetNowTimeString(format string) string {
	if format == "" {
		format = "-"
	}

	switch format[0] {
	case '/':
		return time.Now().Format("02/01/2006 15:04:05 PM")
	default:
		return time.Now().Format("2006-01-02 03:04:05 PM")
	}

}}
