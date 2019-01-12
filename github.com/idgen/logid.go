// Package idgen ...
package idgen

import (
	"context"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type idKey string

const (
	logIDKey idKey = "http-clientappid"
)

// GenLogID ...
func GenLogID(r *http.Request) int64 {
	now := time.Now()
	src := r.Header.Get(string(logIDKey))
	if src != "" {
		if id, err := strconv.ParseInt(src, 10, 64); err == nil {
			return id * 100
		}
	}
	addr := r.Header.Get("remote-addr")
	if addr == "" {
		addr = r.Header.Get("server-addr")
	}
	if addr == "" {
		addr = "127.0.0.1"
	}
	ipInt := int64(stringIP2IntIP(addr))
	ret := ipInt ^ (int64(now.Nanosecond()/1e3) + now.Unix())
	ret = ret & 0xFFFFFFFF
	return ret * 100
}

// SetLogID ...
func SetLogID(ctx context.Context, logID int64) context.Context {
	return context.WithValue(ctx, logIDKey, logID)
}

// GetLogID ...
func GetLogID(ctx context.Context) int64 {
	v := ctx.Value(logIDKey)
	if v == nil {
		return rand.Int63()
	}
	if id, ok := v.(int64); ok {
		return id
	}
	return rand.Int63()
}
