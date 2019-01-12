// package ctxutil provide func
// To get and set context content
package ctxutil

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type keytype int

const (
	_ keytype = iota
	traceIDKey
	spanIDKey
	cspanIDKey
	userIDKey
	redisElapsedKey
	mysqlElapsedKey
	ssdbElapsedKey
	elasticElapsedKey
	requestInTsKey    // 请求进来的时间戳
	processTimeoutKey // 上游超时要求
	URIKey
	tokenKey
	callerKey
	httpRequestKey
)

var (
	// ErrNotExist specifies that the data you want to retrive is missed
	ErrNotExist = errors.New("key is not existed in the context")
	// ErrDurationNotSetted reports that duration must be setted before you use it to do accumulation
	ErrDurationNotSetted = errors.New("duration must be setted at the beginning of the goroutine")
	//ErrWrongType ...
	ErrWrongType = errors.New("wrong type")
)

// helper
func getString(ctx context.Context, key keytype) (string, error) {
	t := ctx.Value(key)
	if t == nil {
		return "", ErrNotExist
	}
	return t.(string), nil
}

func getInt64(ctx context.Context, key keytype) (int64, error) {
	t := ctx.Value(key)
	if t == nil {
		return 0, ErrNotExist
	}
	return t.(int64), nil
}

func getTimeDuration(ctx context.Context, key keytype) time.Duration {
	v := ctx.Value(key)
	if nil == v {
		return time.Duration(0)
	}
	hd, ok := v.(*int64)
	if !ok {
		return time.Duration(0)
	}
	return time.Duration(atomic.LoadInt64(hd))
}

func incTimeDuration(ctx context.Context, key keytype, delta time.Duration) (context.Context, error) {
	v := ctx.Value(key)
	if v == nil {
		return ctx, ErrDurationNotSetted
	}
	vd := v.(*int64)
	atomic.AddInt64(vd, delta.Nanoseconds())
	return ctx, nil
}

func newI64Pointer() *int64 {
	a := new(int64)
	*a = 0
	return a
}

// SetTraceID sets traceID into context
func SetTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

// GetTraceID gets traceID from context,
// if traceID not exists, ErrNotExist will be returned
func GetTraceID(ctx context.Context) (string, error) {
	return getString(ctx, traceIDKey)
}

// SetSpanID sets spanID into context
func SetSpanID(ctx context.Context, spanID string) context.Context {
	return context.WithValue(ctx, spanIDKey, spanID)
}

// GetSpanID gets spanID from context,
// if spanID not exists, ErrNotExist will be returned
func GetSpanID(ctx context.Context) (string, error) {
	return getString(ctx, spanIDKey)
}

// SetCspanID sets cspanID into context
func SetCspanID(ctx context.Context, cspanID string) context.Context {
	return context.WithValue(ctx, cspanIDKey, cspanID)
}

// GetCspanID gets cspanID from context,
// if cspanID not exists, ErrNotExist will be returned
func GetCspanID(ctx context.Context) (string, error) {
	return getString(ctx, cspanIDKey)
}

// SetMysqlElapsedTime initialize the time counter for database
func SetMysqlElapsedTime(ctx context.Context) context.Context {
	return context.WithValue(ctx, mysqlElapsedKey, newI64Pointer())
}

// IncMysqlElapsedTime accumulates the time for operating database
// you must call SetMysqlElapsedTime before use this function
func IncMysqlElapsedTime(ctx context.Context, delta time.Duration) (context.Context, error) {
	return incTimeDuration(ctx, mysqlElapsedKey, delta)
}

// GetMysqlElapsedTime returns the accumulated time for operating database
func GetMysqlElapsedTime(ctx context.Context) time.Duration {
	return getTimeDuration(ctx, mysqlElapsedKey)
}

// SetRedisElapsedTime initialize the time counter for redis
func SetRedisElapsedTime(ctx context.Context) context.Context {
	return context.WithValue(ctx, redisElapsedKey, newI64Pointer())
}

// IncRedisElapsedTime accumulates the time for operating redis
// you must call SetRedisElapsedTime before use this function
func IncRedisElapsedTime(ctx context.Context, delta time.Duration) (context.Context, error) {
	return incTimeDuration(ctx, redisElapsedKey, delta)
}

// GetRedisElapsedTime returns the accumulated time for operating redis
func GetRedisElapsedTime(ctx context.Context) time.Duration {
	return getTimeDuration(ctx, redisElapsedKey)
}

// SetSsdbElapsedTime initialize the time counter for redis
func SetSsdbElapsedTime(ctx context.Context) context.Context {
	return context.WithValue(ctx, ssdbElapsedKey, newI64Pointer())
}

// IncSsdbElapsedTime accumulates the time for operating redis
// you must call SetSsdbElapsedTime before use this function
func IncSsdbElapsedTime(ctx context.Context, delta time.Duration) (context.Context, error) {
	return incTimeDuration(ctx, ssdbElapsedKey, delta)
}

// GetSsdbElapsedTime returns the accumulated time for operating redis
func GetSsdbElapsedTime(ctx context.Context) time.Duration {
	return getTimeDuration(ctx, ssdbElapsedKey)
}

// SetElasticElapsedTime initialize the time counter for redis
func SetElasticElapsedTime(ctx context.Context) context.Context {
	return context.WithValue(ctx, ssdbElapsedKey, newI64Pointer())
}

// IncElasticElapsedTime accumulates the time for operating redis
// you must call SetElasticElapsedTime before use this function
func IncElasticElapsedTime(ctx context.Context, delta time.Duration) (context.Context, error) {
	return incTimeDuration(ctx, ssdbElapsedKey, delta)
}

// GetElasticElapsedTime returns the accumulated time for operating redis
func GetElasticElapsedTime(ctx context.Context) time.Duration {
	return getTimeDuration(ctx, ssdbElapsedKey)
}

// SetUserID sets userID into context
func SetUserID(ctx context.Context, userID int64) context.Context {
	return context.WithValue(ctx, userIDKey, userID)
}

// GetUserID returns userID from context
func GetUserID(ctx context.Context) (int64, error) {
	return getInt64(ctx, userIDKey)
}

// SetRequestInTs 请求的时间戳  毫秒位
func SetRequestInTs(ctx context.Context, ts int64) context.Context {
	return context.WithValue(ctx, requestInTsKey, ts)
}

// SetRequestTimeout 上游超时要求 毫秒位
func SetRequestTimeout(ctx context.Context, to int64) context.Context {
	return context.WithValue(ctx, processTimeoutKey, to)
}

// GetRequestInTs 获取请求的时间戳  毫秒位
func GetRequestInTs(ctx context.Context) (int64, error) {
	v := ctx.Value(requestInTsKey)
	if v == nil {
		return 0, ErrNotExist
	}
	val, ok := v.(int64)
	if !ok {
		return 0, ErrWrongType
	}
	return val, nil
}

// GetRequestTimeout 上游超时要求 毫秒位
func GetRequestTimeout(ctx context.Context) (int64, error) {
	v := ctx.Value(processTimeoutKey)
	if v == nil {
		return 0, ErrNotExist
	}
	val, ok := v.(int64)
	if !ok {
		return 0, ErrWrongType
	}
	return val, nil
}

// SetURI 设置URI
func SetURI(ctx context.Context, uri string) context.Context {
	return context.WithValue(ctx, URIKey, uri)
}

// GetURI 获取URI
func GetURI(ctx context.Context) (string, error) {
	v := ctx.Value(URIKey)
	if v == nil {
		return "", ErrNotExist
	}
	val, ok := v.(string)
	if !ok {
		return "", ErrWrongType
	}
	return val, nil
}

// SetToken sets token into context
func SetToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, tokenKey, token)
}

// GetToken ...
func GetToken(ctx context.Context) (string, error) {
	return getString(ctx, tokenKey)
}

// SetCaller ...
func SetCaller(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, callerKey, caller)
}

// GetCaller ...
func GetCaller(ctx context.Context) string {
	v := ctx.Value(callerKey)
	if v == nil {
		return ""
	}
	if val, ok := v.(string); ok {
		return val
	}
	return ""
}

//SetHTTPRequest ...
func SetHTTPRequest(ctx context.Context, r *http.Request) context.Context {
	return context.WithValue(ctx, httpRequestKey, r)
}

//GetHTTPRequest ...
func GetHTTPRequest(ctx context.Context) (*http.Request, error) {
	v := ctx.Value(httpRequestKey)
	if v == nil {
		return nil, ErrNotExist
	}
	val, ok := v.(*http.Request)
	if !ok {
		return nil, ErrWrongType
	}
	return val, nil
}

// SetAPPTraceInfo ...
func SetAPPTraceInfo(ctx context.Context, info string) context.Context {
	linkList := list.New()
	linkList.PushBack(info)
	return context.WithValue(ctx, TAIHEAPPTraceInfoKey, linkList)
}

// AppendAPPTraceInfo ...
func AppendAPPTraceInfo(ctx context.Context, info string) {
	v := ctx.Value(TAIHEAPPTraceInfoKey)
	if v != nil {
		if link, ok := v.(*list.List); ok {
			link.PushBack(info)
		}
	}
}

// GetAPPTraceInfo ...
func GetAPPTraceInfo(ctx context.Context) []string {
	v := ctx.Value(TAIHEAPPTraceInfoKey)
	if v == nil {
		return nil
	}
	link, ok := v.(*list.List)
	if !ok {
		return nil
	}
	h := link.Front()
	var rets []string
	for {
		if h == nil {
			break
		}
		rets = append(rets, fmt.Sprint(h.Value))
		h = h.Next()
	}
	return rets
}

var builtInKeys = []keytype{
	traceIDKey,
	spanIDKey,
	cspanIDKey,
	redisElapsedKey,
	mysqlElapsedKey,
	ssdbElapsedKey,
	elasticElapsedKey,
	userIDKey,
	requestInTsKey,
	processTimeoutKey,
	URIKey,
	tokenKey,
	callerKey,
}

var keyName = map[keytype]string{
	traceIDKey:        "traceid",
	spanIDKey:         "spanid",
	cspanIDKey:        "cspanid",
	redisElapsedKey:   "redis_elapsed",
	mysqlElapsedKey:   "mysql_elapsed",
	ssdbElapsedKey:    "ssdb_elapsed",
	elasticElapsedKey: "elastic_elapsed",
	userIDKey:         "user_id",
	requestInTsKey:    "request_in_ts",
	processTimeoutKey: "process_timeout",
	URIKey:            "uri",
	tokenKey:          "token",
	callerKey:         "caller",
}

func shouldConvert2Duration(k keytype) bool {
	return k == redisElapsedKey || k == mysqlElapsedKey || k == ssdbElapsedKey || k == elasticElapsedKey
}

func getVal(ctx context.Context, key keytype) (interface{}, error) {
	if ctx == nil {
		return nil, ErrNotExist
	}
	v := ctx.Value(key)
	if v == nil {
		return nil, ErrNotExist
	}
	return v, nil
}

const delimiter = "||"

// Format formats the context with only builtin keys
func Format(ctx context.Context) (result string, finalErr error) {
	defer func() {
		if e := recover(); nil != e {
			finalErr = bytes.ErrTooLarge
		}
	}()
	buf := new(bytes.Buffer)
	for _, key := range builtInKeys {
		val, err := getVal(ctx, key)
		if nil != err {
			continue
		}
		kname, ok := keyName[key]
		if !ok {
			continue
		}
		if shouldConvert2Duration(key) {
			i64 := val.(*int64)
			val = time.Duration(*i64)
		}
		_, finalErr = buf.WriteString(fmt.Sprintf(kname+"=%v"+delimiter, val))
	}
	return strings.TrimRight(buf.String(), delimiter), nil
}

// TraceString is the same as Format,but only return string
// that is, if there's an error occurs, TraceString will return "ctx_format=unset"
func TraceString(ctx context.Context) string {
	if ctx == nil {
		return "ctx=null"
	}
	s, err := Format(ctx)
	if nil != err || "" == s {
		return "ctx_format=unset"
	}
	return s
}
