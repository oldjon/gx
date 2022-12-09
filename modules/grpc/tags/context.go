package tags

import (
	"context"
)

type ctxMarker struct{}

var (
	// ctxMarkerKey is the Context value marker used by *all* logging middleware.
	// The logging middleware object must interf
	ctxMarkerKey = &ctxMarker{}
)

// FromContext returns a pre-existing Tags object in the Context.
// If the context wasn't set in a tag interceptor, a no-op Tag storage is returned that will *not* be propagated in context.
func FromContext(ctx context.Context) Tags {
	t, ok := ctx.Value(ctxMarkerKey).(Tags)
	if !ok {
		return newMapTags(make(map[string]interface{}))
	}
	return t
}

// WithTags returns a new context that carries provided tags
func WithTags(ctx context.Context, tags Tags) context.Context {
	return context.WithValue(ctx, ctxMarkerKey, tags)
}
