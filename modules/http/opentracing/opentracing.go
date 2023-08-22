package opentracing

import (
	"fmt"
	"net/http"

	"github.com/felixge/httpsnoop"
	"github.com/oldjon/gutil/ip"
	"github.com/oldjon/gx/common"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

type Options struct {
	Tracer opentracing.Tracer
}

type Middleware struct {
	opts Options
}

func New(opts Options) *Middleware {
	return &Middleware{
		opts: opts,
	}
}

func (m *Middleware) Handler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var span opentracing.Span

		tracer := m.opts.Tracer
		op := r.URL.Path

		spanContext, err := tracer.Extract(
			opentracing.HTTPHeaders,
			opentracing.HTTPHeadersCarrier(r.Header))
		if err != nil {
			// TODO: log error
			span = tracer.StartSpan(op)
		} else {
			span = tracer.StartSpan(op, opentracing.ChildOf(spanContext))
		}
		defer span.Finish()

		// WriteHeaderFunc is not called everytime ,only have status other than http.StatusOK
		var statusCode = http.StatusOK
		var errBody string
		var wrapW = w
		if common.IsHTTPRichTraceEnabled() {
			wrapW = httpsnoop.Wrap(w, httpsnoop.Hooks{
				WriteHeader: func(headerFunc httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
					return func(code int) {
						statusCode = code
						headerFunc(code)
					}
				},
				Write: func(writeFunc httpsnoop.WriteFunc) httpsnoop.WriteFunc {
					return func(b []byte) (int, error) {
						if statusCode >= http.StatusBadRequest {
							errBody += string(b)
						}
						return writeFunc(b)
					}
				},
			})

			span.SetTag("http.method", r.Method)

			clientIP := ip.GetHTTPClientIP(r, ip.GetHTTPClientIPOptions{})
			if clientIP != "" {
				span.SetTag("http.client_ip", clientIP)
			}
		}

		ctx := opentracing.ContextWithSpan(r.Context(), span)
		nr := r.WithContext(ctx)

		h.ServeHTTP(wrapW, nr)

		if common.IsHTTPRichTraceEnabled() {
			span.SetTag("http.status_code", statusCode)

			if statusCode >= http.StatusBadRequest {
				span.LogFields(log.String("event", "error"), log.String("message", fmt.Sprintf("status code:[%d] - %s", statusCode, errBody)))
			}
		}
	})
}
