package resolver

import (
	"fmt"

	grpcresolver "google.golang.org/grpc/resolver"
)

var (
	errMetaNotMap = fmt.Errorf("metadata is not map")
)

var (
	_ Selector = (*MetaSelector)(nil)
	_ Selector = (*andSelector)(nil)
	_ Selector = (*orSelector)(nil)
	_ Selector = (*constSelector)(nil)
)

type Selector interface {
	Select(grpcresolver.Address) (bool, error)
}

type MetaSelector struct {
	MetaData map[string]string
}

func NewMetaSelector() *MetaSelector {
	return &MetaSelector{map[string]string{}}
}

func (ms MetaSelector) WithKV(k, v string) {
	ms.MetaData[k] = v
}

// And returns Selector that return true if all ss return true
func And(ss ...Selector) Selector {
	return andSelector{selectors: ss}
}

// Or returns Selector that return true if any ss return true, if there is any error
// returns the first error encounters
func Or(ss ...Selector) Selector {
	return orSelector{selectors: ss}
}

// True returns Selector that always return true
func True() Selector {
	return constSelector{true, nil}
}

// False returns Selector that always return false
func False() Selector {
	return constSelector{false, nil}
}

// Error returns Selector that always return error
func Error(err error) Selector {
	return constSelector{false, err}
}

func (ms MetaSelector) Select(addr grpcresolver.Address) (bool, error) {
	metaData, ok := addr.Metadata.(map[string]interface{})
	if !ok {
		return false, errMetaNotMap
	}

	for k, v := range ms.MetaData {
		fieldValue, ok := metaData[k]
		if !ok {
			return false, nil
		}

		value, ok := fieldValue.(string)
		if !ok {
			return false, nil
		}
		if value != v {
			return false, nil
		}
	}

	return true, nil
}

type andSelector struct {
	selectors []Selector
}

func (as andSelector) Select(addr grpcresolver.Address) (bool, error) {
	for _, s := range as.selectors {
		ok, err := s.Select(addr)
		if err != nil {
			return ok, err
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

type orSelector struct {
	selectors []Selector
}

func (os orSelector) Select(addr grpcresolver.Address) (bool, error) {
	for _, s := range os.selectors {
		ok, err := s.Select(addr)
		if err != nil {
			return ok, err
		}

		if ok {
			return true, nil
		}
	}

	return false, nil
}

type constSelector struct {
	ok  bool
	err error
}

func (cs constSelector) Select(addr grpcresolver.Address) (bool, error) {
	return cs.ok, cs.err
}
