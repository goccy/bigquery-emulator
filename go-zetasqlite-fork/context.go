package zetasqlite

import (
	"context"
	"time"

	"github.com/goccy/go-zetasqlite/internal"
)

// WithCurrentTime use to replace the current time with the specified time.
// To replace the time, you need to pass the returned context as an argument to QueryContext.
// `CURRENT_DATE`, `CURRENT_DATETIME`, `CURRENT_TIME`, `CURRENT_TIMESTAMP` functions are targeted.
func WithCurrentTime(ctx context.Context, now time.Time) context.Context {
	return internal.WithCurrentTime(ctx, now)
}

// CurrentTime gets the time specified by WithCurrentTime.
func CurrentTime(ctx context.Context) *time.Time {
	return internal.CurrentTime(ctx)
}
