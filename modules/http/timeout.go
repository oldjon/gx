package http

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

type TimeOutOptions struct {
	Timeout      time.Duration
	ErrorHandler errorHandler
}

func OnError(w http.ResponseWriter, _ *http.Request, err string) {
	http.Error(w, err, http.StatusServiceUnavailable)
}

type Middleware struct {
	opts TimeOutOptions
}

func NewTimeOut(opts TimeOutOptions) *Middleware {
	if opts.ErrorHandler == nil {
		opts.ErrorHandler = OnError
	}
	return &Middleware{
		opts: opts,
	}
}

func (m *Middleware) Handler(h http.Handler) http.Handler {
	// shortcut without timeout
	if m.opts.Timeout == 0 {
		return h
	}

	return &timeoutHandler{
		handler:      h,
		errorHandler: m.opts.ErrorHandler,
		dt:           m.opts.Timeout,
	}
}

// The following code is copied from go source code net/http/server.go with the following modification
// 1. Set timeout on request context
// 2. User can provide error handler function to call when timeout
type timeoutHandler struct {
	handler      http.Handler
	errorHandler errorHandler
	dt           time.Duration

	// When set, no timer will be created and this channel will
	// be used instead.
	testTimeout <-chan time.Time
}

func (h *timeoutHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var t *time.Timer
	timeout := h.testTimeout
	if timeout == nil {
		t = time.NewTimer(h.dt)
		timeout = t.C
	}
	done := make(chan struct{})
	tw := &timeoutWriter{
		w: w,
		h: make(http.Header),
	}

	ctx, cancel := context.WithCancel(r.Context())

	go func() {
		h.handler.ServeHTTP(tw, r.WithContext(ctx))
		close(done)
	}()
	select {
	case <-done:
		tw.mu.Lock()
		defer tw.mu.Unlock()
		dst := w.Header()
		for k, vv := range tw.h {
			dst[k] = vv
		}
		if !tw.wroteHeader {
			tw.code = http.StatusOK
		}
		w.WriteHeader(tw.code)
		_, _ = w.Write(tw.writeBuff.Bytes())
		if t != nil {
			t.Stop()
		}
		cancel()
	case <-timeout:
		tw.mu.Lock()
		defer tw.mu.Unlock()
		h.errorHandler(w, r, ErrHandlerTimeout.Error())
		tw.timedOut = true
		cancel()
		return
	}
}

type timeoutWriter struct {
	w         http.ResponseWriter
	h         http.Header
	writeBuff bytes.Buffer

	mu          sync.Mutex
	timedOut    bool
	wroteHeader bool
	code        int
}

// ErrHandlerTimeout is returned on ResponseWriter Write calls
// in handlers which have timed out.
var ErrHandlerTimeout = errors.New("http: Handler timeout")

func (tw *timeoutWriter) Header() http.Header { return tw.h }

func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut {
		return 0, ErrHandlerTimeout
	}
	if !tw.wroteHeader {
		tw.writeHeader(http.StatusOK)
	}
	return tw.writeBuff.Write(p)
}

func (tw *timeoutWriter) WriteHeader(code int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.timedOut || tw.wroteHeader {
		return
	}
	tw.writeHeader(code)
}

func (tw *timeoutWriter) writeHeader(code int) {
	tw.wroteHeader = true
	tw.code = code
}
