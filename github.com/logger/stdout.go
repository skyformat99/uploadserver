package logger

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/config"
)

type stdLog struct {
	maxLevel logLevel
	enable   bool
	format   string
	out      io.Writer
}

func newStdLog(sec config.Sectioner) Logger {
	level := strings.ToUpper(sec.GetStringMust("stdout.level", "DEBUG"))
	enable := sec.GetBoolMust("stdout.enable", false)
	format := sec.GetStringMust("stdout.format", defaultFormat)
	return &stdLog{
		maxLevel: getLogLevel(level),
		enable:   enable,
		out:      os.Stdout,
		format:   format,
	}
}

func newDefaultLog() Logger {
	return &stdLog{
		maxLevel: TRACE,
		enable:   true,
		out:      os.Stdout,
		format:   defaultFormat,
	}
}

func (s *stdLog) Debug(args ...interface{}) {
	s.do(DEBUG, "", args...)
}

func (s *stdLog) Debugf(format string, args ...interface{}) {
	s.do(DEBUG, format, args...)
}

func (s *stdLog) Trace(args ...interface{}) {
	s.do(TRACE, "", args...)
}

func (s *stdLog) Tracef(format string, args ...interface{}) {
	s.do(TRACE, format, args...)
}

func (s *stdLog) Info(args ...interface{}) {
	s.do(INFO, "", args...)
}

func (s *stdLog) Infof(format string, args ...interface{}) {
	s.do(INFO, format, args...)
}

func (s *stdLog) Warn(args ...interface{}) {
	s.do(WARNING, "", args...)
}

func (s *stdLog) Warnf(format string, args ...interface{}) {
	s.do(WARNING, format, args...)
}

func (s *stdLog) Error(args ...interface{}) {
	s.do(ERROR, "", args...)
}

func (s *stdLog) Errorf(format string, args ...interface{}) {
	s.do(ERROR, format, args...)
}

func (s *stdLog) Fatal(args ...interface{}) {
	s.do(FATAL, "", args...)
}

func (s *stdLog) Fatalf(format string, args ...interface{}) {
	s.do(FATAL, format, args...)
}

func (s *stdLog) Close() {}

func (s *stdLog) do(lev logLevel, format string, args ...interface{}) {
	if !s.enable || lev < s.maxLevel {
		return
	}
	msg := ""
	if format == "" {
		msg = fmt.Sprint(args...)
	} else {
		msg = fmt.Sprintf(format, args...)
	}
	lc := &logCell{
		level:    lev,
		callInfo: genCallInfo(depth),
		format:   s.format,
		msg:      msg,
	}
	s.out.Write(formatLog(lc))
}
