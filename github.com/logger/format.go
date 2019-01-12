package logger

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

const (
	defaultFormat = "[%D %T] [%L] (%S) %M"
	shortFormat   = "[%t %d] [%L] %M"
	didiFormat    = "[%L][%Z][%S]%M"

	normalTpl = "2006-01-02T15:04:05.000-0700"
)

type formatType struct {
	dateFormat string
	lastest    int64
	lymd       []byte
	symd       []byte
	lhsm       []byte
	shsm       []byte
	ztime      []byte
	level      map[logLevel][]byte
	mu         sync.RWMutex
}

const (
	_byteBlank = byte(' ')
	_byteLF    = byte('\n')
)

var ft = &formatType{}

func init() {
	ft.level = map[logLevel][]byte{
		DEBUG:   []byte(fmt.Sprintf("[%s]", DEBUG)),
		TRACE:   []byte(fmt.Sprintf("[%s]", TRACE)),
		INFO:    []byte(fmt.Sprintf("[%s]", INFO)),
		WARNING: []byte(fmt.Sprintf("[%s]", WARNING)),
		ERROR:   []byte(fmt.Sprintf("[%s]", ERROR)),
		FATAL:   []byte(fmt.Sprintf("[%s]", FATAL)),
	}
}

func formatLog(cell *logCell) []byte {
	now := time.Now()
	if now.Unix() != ft.lastest {
		ft.lymd = []byte(fmt.Sprintf("%04d/%02d/%02d", now.Year(), now.Month(), now.Day()))
		ft.symd = []byte(fmt.Sprintf("%02d/%02d/%02d", now.Month(), now.Day(), now.Year()%100))
		ft.lhsm = []byte(fmt.Sprintf("%02d:%02d:%02d %03d",
			now.Hour(),
			now.Minute(),
			now.Second(),
			now.Nanosecond()/1e6))
		ft.shsm = []byte(fmt.Sprintf("%02d:%02d",
			now.Hour(),
			now.Minute()))
		ft.ztime = []byte(now.Format(normalTpl))
	}
	buf := &bytes.Buffer{}
	pis := bytes.Split([]byte(cell.format), []byte{'%'})
	for _, pi := range pis {
		if len(pi) == 0 {
			continue
		}
		switch pi[0] {
		case 'D':
			buf.Write(ft.lymd)
		case 'd':
			buf.Write(ft.symd)
		case 'T':
			buf.Write(ft.lhsm)
		case 't':
			buf.Write(ft.shsm)
		case 'Z':
			buf.Write(ft.ztime)
		case 'L':
			buf.WriteString(cell.level.String())
		case 'S':
			buf.WriteString(cell.callInfo)
		case 'M':
			buf.WriteString(cell.msg)
		default:
			buf.Write(pi[:1])
		}
		if len(pi) > 0 {
			buf.Write(pi[1:])
		}
	}

	buf.WriteByte(_byteLF)
	return buf.Bytes()
}
