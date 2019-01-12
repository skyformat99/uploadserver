package logger

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/config"
)

//Logger :interface for log
type Logger interface {
	//Trace :Trace interface for log
	Trace(args ...interface{})
	//Tracef :Tracef interface for log
	Tracef(format string, args ...interface{})
	//Debug :Debug interface for log
	Debug(args ...interface{})
	//Debugf :Debugf interface for log
	Debugf(format string, args ...interface{})
	//Info :Info interface for log
	Info(args ...interface{})
	//Infof :Infof interface for log
	Infof(format string, args ...interface{})
	//Warn :Warn interface for log
	Warn(args ...interface{})
	//Warnf :Warnf interface for log
	Warnf(format string, args ...interface{})
	//Error :Error interface for log
	Error(args ...interface{})
	//Errorf :Errorf interface for log
	Errorf(format string, args ...interface{})
	//Fatal :Fatal interface for log
	Fatal(args ...interface{})
	//Fatalf :Fatalf interface for log
	Fatalf(format string, args ...interface{})
	//Close :Close interface for log
	Close()
}

var (
	depth      = 7
	allLog     Logger
	defaultLog mutilLog
)

type mutilLog map[LogType]Logger

func init() {
	defaultLog = make(map[LogType]Logger)
	defaultLog["stdout"] = newDefaultLog()
	allLog = defaultLog
}

type logCell struct {
	level    logLevel
	callInfo string
	format   string
	msg      string
}

// SetDepth :
func SetDepth(delt int) {
	depth += delt
}

func genCallInfo(dep int) string {
	pc, f, n, ok := runtime.Caller(dep)
	f = filepath.Base(f)
	if ok {
		name := runtime.FuncForPC(pc).Name()
		if len(name) > maxNameLength {
			name = ".." + name[(len(name)-maxNameLength):]
		}
		return fmt.Sprintf("%s:%s:%d", name, f, n)
	}
	return fmt.Sprintf("???:%s:%d", f, n)
}

//NewLoggerWithConfig :by cfg
func NewLoggerWithConfig(cfg config.Configer) error {
	sec, err := cfg.GetSection("log")
	if err != nil {
		return err
	}
	ts, err := sec.GetString("type")
	if err != nil {
		return err
	}

	types := strings.Split(ts, ",")
	var hasStdout bool
	for _, t := range types {
		switch strings.ToLower(t) {
		case "stdout":
			plog := newStdLog(sec)
			if plog != nil {
				defaultLog["stdout"] = plog
			} else {
				if _, ok := defaultLog["stdout"]; ok {
					delete(defaultLog, "stdout")
				}
			}
			hasStdout = true
		case "file":
			plog := newFileLog(sec)
			if plog != nil {
				defaultLog["file"] = plog
			}
		default:
			return fmt.Errorf("unsupported log type:%s", t)
		}
	}
	if !hasStdout {
		delete(defaultLog, "stdout")
	}
	if len(defaultLog) < 1 {
		return fmt.Errorf("bad log type:%s", ts)
	}
	if publicSecName, _ := sec.GetString("public_sec"); publicSecName != "" {
		if publicSec, nerr := cfg.GetSection(publicSecName); nerr == nil {
			InitTrack(publicSec)
		}
	}
	return nil
}

func trace(args ...interface{}) {
	allLog.Trace(args...)
}

func tracef(format string, args ...interface{}) {
	allLog.Tracef(format, args...)
}

func debug(args ...interface{}) {
	allLog.Debug(args...)
}
func debugf(format string, args ...interface{}) {
	allLog.Debugf(format, args...)
}

func info(args ...interface{}) {
	allLog.Info(args...)
}

func infof(format string, args ...interface{}) {
	allLog.Infof(format, args...)
}

func warn(args ...interface{}) {
	allLog.Warn(args...)
}

func warnf(format string, args ...interface{}) {
	allLog.Warnf(format, args...)
}

func errorc(args ...interface{}) {
	allLog.Error(args...)
}

func errorf(format string, args ...interface{}) {
	allLog.Errorf(format, args...)
}

func fatal(args ...interface{}) {
	allLog.Fatal(args...)
}

func fatalf(format string, args ...interface{}) {
	allLog.Fatalf(format, args...)
}

//PrintStack :print
func PrintStack() []byte {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	if n < 4096 {
		return buf[:n]
	}
	return buf
}

//Close :
func Close() {
	allLog.Close()
}

//InitTrack :Init
func InitTrack(sec config.Sectioner) {
	defaultOfflineFileLog = newOfflineFileLog(sec)
}

//Track :Track
func Track(prefix, msg string) {
	defaultOfflineFileLog.do(prefix, msg)
}

//Trace :mutilLog.Trace
func (ml mutilLog) Trace(args ...interface{}) {
	ml.doLog(TRACE, args...)
}

//Tracef :mutilLog.Tracef
func (ml mutilLog) Tracef(format string, args ...interface{}) {
	ml.dofLog(TRACE, format, args...)
}

//Debug :mutilLog.Debug
func (ml mutilLog) Debug(args ...interface{}) {
	ml.doLog(DEBUG, args...)
}

//Debugf :mutilLog.Debugf
func (ml mutilLog) Debugf(format string, args ...interface{}) {
	ml.dofLog(DEBUG, format, args...)
}

//Info :mutilLog.Info
func (ml mutilLog) Info(args ...interface{}) {
	ml.doLog(INFO, args...)
}

//Infof :mutilLog.Infof
func (ml mutilLog) Infof(format string, args ...interface{}) {
	ml.dofLog(INFO, format, args...)
}

//Warn :mutilLog.Warn
func (ml mutilLog) Warn(args ...interface{}) {
	ml.doLog(WARNING, args...)
}

//Warnf :mutilLog.Warnf
func (ml mutilLog) Warnf(format string, args ...interface{}) {
	ml.dofLog(WARNING, format, args...)
}

//Error :mutilLog.Error
func (ml mutilLog) Error(args ...interface{}) {
	ml.doLog(ERROR, args...)
}

//Errorf :mutilLog.Errorf
func (ml mutilLog) Errorf(format string, args ...interface{}) {
	ml.dofLog(ERROR, format, args...)
}

//Fatal :mutilLog.Fatal
func (ml mutilLog) Fatal(args ...interface{}) {
	ml.doLog(FATAL, args...)
}

//Fatalf :mutilLog.Fatalf
func (ml mutilLog) Fatalf(format string, args ...interface{}) {
	ml.dofLog(FATAL, format, args...)
}

//Close :mutilLog.Close
func (ml mutilLog) Close() {
	for i := range ml {
		ml[i].Close()
	}
}

func (ml mutilLog) doLog(level logLevel, args ...interface{}) {
	for i := range ml {
		switch level {
		case DEBUG:
			ml[i].Debug(args...)
		case TRACE:
			ml[i].Trace(args...)
		case INFO:
			ml[i].Info(args...)
		case WARNING:
			ml[i].Warn(args...)
		case ERROR:
			ml[i].Error(args...)
		case FATAL:
			ml[i].Fatal(args...)
		}
	}
}

func (ml mutilLog) dofLog(level logLevel, format string, args ...interface{}) {
	for i := range ml {
		switch level {
		case DEBUG:
			ml[i].Debugf(format, args...)
		case TRACE:
			ml[i].Tracef(format, args...)
		case INFO:
			ml[i].Infof(format, args...)
		case WARNING:
			ml[i].Warnf(format, args...)
		case ERROR:
			ml[i].Errorf(format, args...)
		case FATAL:
			ml[i].Fatalf(format, args...)
		}
	}
}

//LogType :Log Type
type LogType string

const (
	//LogTypeStdout :Log Type Stdout
	LogTypeStdout = "stdout"
	//LogTypeFile :Log Type File
	LogTypeFile = "file"
)

//GetLoggers :
func GetLoggers() Logger {
	return allLog
}

//GetLogger :
func GetLogger(t LogType) Logger {
	return defaultLog[t]
}
