package service

import "net/http"

type ProcessType uint8

const (
	ProcessHTTP ProcessType = iota
)

// WithProcess used to help add extra process to module
func WithProcess(processType ProcessType, name string, pcf ProcessHandlerCreator) ModuleOption {
	return func(o *moduleOptions) {
		po := ProcessOption{
			ProcessType: processType,
			Name:        name,
			Creator:     pcf,
		}

		o.processOptions = append(o.processOptions, po)
	}
}

type ProcessHandler interface {
}

// ProcessHTTPHandler is used to create http server
type ProcessHTTPHandler interface {
	http.Handler
}

// ProcessHTTPPather is used to limit the metrics path outputs
type ProcessHTTPPather interface {
	GetPaths() []string
}

type ProcessHandlerCreator func(module ModuleDriver) (ProcessHandler, error)

type ProcessOption struct {
	ProcessType ProcessType
	Name        string
	Creator     ProcessHandlerCreator
}

type ModuleProcesses interface {
	SetProcessOptions(options []ProcessOption)
}
