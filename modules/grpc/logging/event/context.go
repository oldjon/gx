package event

import (
	"context"

	"github.com/oldjon/gx/service"
)

type ctxMarker struct{}

var (
	ctxMarkerKey = &ctxMarker{}
)

func FromContext(ctx context.Context) *service.EventLogger {
	t, ok := ctx.Value(ctxMarkerKey).(*service.EventLogger)
	if !ok {
		// TODO: caller will panic if no EventLogger is found, find better way to handle this
		return nil
	}
	return t
}

func WithEventLogger(ctx context.Context, logger *service.EventLogger) context.Context {
	return context.WithValue(ctx, ctxMarkerKey, logger)
}
