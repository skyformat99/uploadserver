package logger

import (
	"context"
	"fmt"
	"strings"
	"time"
)

var ctxStringFunc func(ctx context.Context) string

func init() {
	ctxStringFunc = nilCtxString
}

var nilCtxString = func(ctx context.Context) string { return "ctx_format=unset" }

// RegisterLogger ...
func RegisterLogger(obj Logger) {
	allLog = obj
}

// RegisterContextFormat ...
func RegisterContextFormat(fn func(ctx context.Context) string) {
	ctxStringFunc = fn
}

// Debug ...
func Debug(tag TAIHETag, args ...interface{}) {
	debug(append([]interface{}{tag, "||"}, args...)...)
}

// Debugf ...
func Debugf(ctx context.Context, tag TAIHETag, format string, args ...interface{}) {
	debugf("%s||%s||"+format, append([]interface{}{tag, ctxStringFunc(ctx)}, args...)...)
}

// Trace ...
func Trace(tag TAIHETag, args ...interface{}) {
	trace(append([]interface{}{tag, "||"}, args...)...)
}

// Tracef ...
func Tracef(ctx context.Context, tag TAIHETag, format string, args ...interface{}) {
	tracef("%s||%s||"+format, append([]interface{}{tag, ctxStringFunc(ctx)}, args...)...)
}

// Info ...
func Info(tag TAIHETag, args ...interface{}) {
	info(append([]interface{}{tag, "||"}, args...)...)
}

// Infof ...
func Infof(ctx context.Context, tag TAIHETag, format string, args ...interface{}) {
	infof("%s||%s||"+format, append([]interface{}{tag, ctxStringFunc(ctx)}, args...)...)
}

// Warn ...
func Warn(tag TAIHETag, args ...interface{}) {
	warn(append([]interface{}{tag, "||"}, args...)...)
}

// Warnf ...
func Warnf(ctx context.Context, tag TAIHETag, format string, args ...interface{}) {
	warnf("%s||%s||"+format, append([]interface{}{tag, ctxStringFunc(ctx)}, args...)...)
}

// Error ...
func Error(tag TAIHETag, args ...interface{}) {
	errorc(append([]interface{}{tag, "||"}, args...)...)
}

// Errorf ...
func Errorf(ctx context.Context, tag TAIHETag, format string, args ...interface{}) {
	errorf("%s||%s||"+format, append([]interface{}{tag, ctxStringFunc(ctx)}, args...)...)
}

// Fatal ...
func Fatal(tag TAIHETag, args ...interface{}) {
	fatal(append([]interface{}{tag, "||"}, args...)...)
}

// Fatalf ...
func Fatalf(ctx context.Context, tag TAIHETag, format string, args ...interface{}) {
	fatalf("%s||%s||"+format, append([]interface{}{tag, ctxStringFunc(ctx)}, args...)...)
}

// Public ...
func Public(ctx context.Context, key string, pairs map[string]interface{}) {
	if defaultOfflineFileLog == nil {
		return
	}
	var kvs []string
	if !defaultOfflineFileLog.useAbnormal {
		kvs = append(kvs, key)
		//kvs = append(kvs, "[public]("+genCallInfo(depth-5)+")")
	} else {
		//kvs = append(kvs, "[PUBLIC]["+time.Now().Format("2006-01-02T15:04:05.999-0700")+"]["+genCallInfo(depth-5)+"] "+key)
	}
	kvs = append(kvs, "timestamp="+time.Now().Format("2006-01-02 15:04:05"))
	if !defaultOfflineFileLog.useAbnormal {
		kvs = append(kvs, ctxStringFunc(ctx))
		kvs = append(kvs, "opera_stat_key="+key)
	}
	for k, v := range pairs {
		kvs = append(kvs, k+"="+fmt.Sprint(v))
	}
	defaultOfflineFileLog.do("public", strings.Join(kvs, "||"))
}

// PublicString ...
func PublicString(ctx context.Context, public string) {
	if defaultOfflineFileLog == nil {
		return
	}
	defaultOfflineFileLog.do("public", public)
}
