package ctxutil

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestFormat(t *testing.T) {
	var err error
	ctx := context.Background()
	ctx = SetTraceID(ctx, keyName[traceIDKey])
	ctx = SetSpanID(ctx, keyName[spanIDKey])
	ctx = SetCspanID(ctx, keyName[cspanIDKey])
	ctx = SetDBTime(ctx)
	ctx, err = IncDBTime(ctx, 5*time.Second)
	if err != nil {
		fmt.Printf("IncDBTime Failed. err:%+v", err)
		return
	}
	ctx = SetRedisTime(ctx)
	ctx, err = IncRedisTime(ctx, 2*time.Minute)
	if err != nil {
		fmt.Printf("IncRedisTime Failed. err:%+v", err)
		return
	}
	var userID int64 = 123456
	ctx = SetUserID(ctx, userID)
	s, err := Format(ctx)
	if err != nil {
		fmt.Printf("Format Failed. err:%+v", err)
		return
	}
	expect := `traceID=traceID||spanID=spanID||cspanID=cspanID||redisTime=2m0s||dbTime=5s||userID=123456`
	if s != expect {
		fmt.Printf("Format string not satisfy condition. err:%+v", err)
		return
	}
}
