package tags

import (
	"encoding/json"
	"errors"
)

var (
	ErrMalformedTags  = errors.New("grpctags: Failed to marshal/unmarshal tags")
	ErrInvalidCarrier = errors.New("grpctags: Invalid Inject/Extract carrier")
	ErrInvalidTagsVal = errors.New("grpctags: Invalid tagsVal")
	ErrTagsValNull    = errors.New("grpctags: tagsVal is string null")
)

type Injector interface {
	Inject(tags Tags, carrier interface{}) error
}

type Extractor interface {
	Extract(reader interface{}) (Tags, error)
}

type textMapReader interface {
	ForeachKey(f func(key string, val string) error) error
}

type textMapWriter interface {
	Set(key string, val string)
}

type textMapPropagator struct {
	tagsPrefix string
}

func (p *textMapPropagator) Inject(tags Tags, carrier interface{}) error {
	w, ok := carrier.(textMapWriter)
	if !ok {
		return ErrInvalidCarrier
	}

	// todo: tags concurrent control
	val, err := json.Marshal(tags.Values())
	if err != nil {
		return ErrMalformedTags
	}

	tagskey := p.key()
	w.Set(tagskey, string(val))
	return nil
}

func (p *textMapPropagator) Extract(carrier interface{}) (Tags, error) {
	r, ok := carrier.(textMapReader)
	if !ok {
		return nil, ErrInvalidCarrier
	}

	tagskey := p.key()
	var tagsval string
	_ = r.ForeachKey(func(key string, val string) error {
		if key == tagskey {
			tagsval = val
		}
		return nil
	})

	if len(tagsval) == 0 {
		return nil, ErrInvalidTagsVal
	}

	values := make(map[string]interface{})
	err := json.Unmarshal([]byte(tagsval), &values)
	if err != nil {
		return nil, ErrMalformedTags
	}
	if values == nil {
		return nil, ErrTagsValNull
	}
	return newMapTags(values), nil
}

func (p *textMapPropagator) key() string {
	return p.tagsPrefix + "-all-in-one"
}
