package http

import (
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/oldjon/gx/common"
	commonPrometheus "github.com/oldjon/gx/common/prometheus"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	reqsName    = "http_requests_total"
	latencyName = "http_request_duration_seconds"
	sizeName    = "http_response_size_kb"
)

var (
	sizeBuckets = []float64{.1, .25, .5, 1, 5, 10, 50, 100, 500}
)

type MetricsOptions struct {
	HostName    string
	ModuleName  string
	ProcessName string
	Paths       []string

	Registerer prometheus.Registerer
}

type Metrics struct {
	reqs    *prometheus.CounterVec
	latency *prometheus.HistogramVec
	size    *prometheus.HistogramVec
	paths   []string

	hostName                string
	moduleName              string
	processName             string
	usingUnifiedMetricsName bool
}

func NewMetrics(opts MetricsOptions) *Metrics {
	if opts.Registerer == nil {
		opts.Registerer = prometheus.DefaultRegisterer
	}

	usingUnifiedMetricsName := common.UsingUnifiedMetricsName()

	labels := []string{"code", "method", "path"}
	if usingUnifiedMetricsName {
		labels = []string{
			commonPrometheus.HostNameLabel,
			commonPrometheus.ModuleNameLabel,
			commonPrometheus.ProcessNameLabel,
			"code",
			"method",
			"path",
		}
	}

	var reqs *prometheus.CounterVec
	if usingUnifiedMetricsName {
		reqs = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "fx",
				Subsystem: "",
				Name:      reqsName,
				Help:      "How many http requests processed, partitioned by status code, method and path",
			},
			labels,
		)
	} else {
		reqs = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: opts.HostName,
				Subsystem: opts.ModuleName,
				Name:      reqsName,
				Help:      "How many http requests processed, partitioned by status code, method and path",
			},
			labels,
		)
	}

	reqs = registerOrGet(opts.Registerer, reqs).(*prometheus.CounterVec)

	var latency *prometheus.HistogramVec
	if usingUnifiedMetricsName {
		latency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "fx",
				Subsystem: "",
				Name:      latencyName,
				Help:      "How long it took to process the request in second, partitioned by status code, method and path",
			},
			labels,
		)
	} else {
		latency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: opts.HostName,
				Subsystem: opts.ModuleName,
				Name:      latencyName,
				Help:      "How long it took to process the request in second, partitioned by status code, method and path",
			},
			labels,
		)
	}

	latency = registerOrGet(opts.Registerer, latency).(*prometheus.HistogramVec)

	var size *prometheus.HistogramVec
	if usingUnifiedMetricsName {
		size = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "fx",
				Subsystem: "",
				Name:      sizeName,
				Help:      "How many kb response take, partitioned by status code, method and path",
				Buckets:   sizeBuckets,
			},
			labels,
		)
	} else {
		size = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: opts.HostName,
				Subsystem: opts.ModuleName,
				Name:      sizeName,
				Help:      "How many kb response take, partitioned by status code, method and path",
				Buckets:   sizeBuckets,
			},
			labels,
		)
	}

	size = registerOrGet(opts.Registerer, size).(*prometheus.HistogramVec)

	return &Metrics{
		reqs:    reqs,
		latency: latency,
		size:    size,
		paths:   opts.Paths,

		hostName:                opts.HostName,
		moduleName:              opts.ModuleName,
		processName:             opts.ProcessName,
		usingUnifiedMetricsName: usingUnifiedMetricsName,
	}
}

func (m *Metrics) Handler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := commonPrometheus.NewResponseWriter(w)
		h.ServeHTTP(rw, r)
		latency := time.Since(start).Seconds()
		paths := m.paths

		path := r.URL.Path
		// filter out invalid utf8 path, which will make prometheus parse failure
		if utf8.ValidString(path) && match(paths, path) {
			var labels prometheus.Labels
			if m.usingUnifiedMetricsName {
				labels = prometheus.Labels{
					commonPrometheus.HostNameLabel:    m.hostName,
					commonPrometheus.ModuleNameLabel:  m.moduleName,
					commonPrometheus.ProcessNameLabel: m.processName,
					"code":                            strconv.Itoa(rw.Status),
					"method":                          r.Method,
					"path":                            path,
				}
			} else {
				labels = prometheus.Labels{
					"code":   strconv.Itoa(rw.Status),
					"method": r.Method,
					"path":   path,
				}
			}

			m.reqs.With(labels).Inc()
			m.latency.With(labels).Observe(latency)
			m.size.With(labels).Observe(float64(rw.Size >> 10)) // convert from bytes to kb
		}
	})
}

func match(paths []string, path string) bool {
	if len(paths) == 0 {
		return true
	}
	for _, p := range paths {
		if p == path {
			return true
		}
	}
	return false
}

func registerOrGet(r prometheus.Registerer, c prometheus.Collector) prometheus.Collector {
	if err := r.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector
		}
		panic(err)
	}
	return c
}
