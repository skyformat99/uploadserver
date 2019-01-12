package logger

import (
	"strings"
)

const (
	maxNameLength = 64
)

type logLevel uint8

const (
	//TRACE :logLevel TRACE
	TRACE logLevel = iota
	//DEBUG :logLevel DEBUG
	DEBUG
	//INFO :logLevel INFO
	INFO
	//WARNING :logLevel WARNING
	WARNING
	//ERROR :logLevel ERROR
	ERROR
	//FATAL :logLevel FATAL
	FATAL
)

var levelMap = map[logLevel]string{
	TRACE:   "TRACE",
	DEBUG:   "DEBUG",
	INFO:    "INFO",
	WARNING: "WARNING",
	ERROR:   "ERROR",
	FATAL:   "FATAL",
}

func (l logLevel) String() string {
	if _, ok := levelMap[l]; ok {
		return levelMap[l]
	}
	return "???"
}

func getLogLevel(l string) logLevel {
	switch strings.ToUpper(l) {
	case "TRACE":
		return TRACE
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARNING":
		return WARNING
	case "ERROR":
		return ERROR
	case "FATAL":
		return FATAL
	default:
		return ERROR
	}
}
