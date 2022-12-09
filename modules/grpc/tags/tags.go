package tags

var (
	NoopTags = &noopTags{}
)

// Tags is the interface used for storing request tags between Context calls.
// The default implementation is *not* thread safe, and should be handled only in the context of the request.
type Tags interface {
	// Set sets the given key in the metadata tags.
	Set(key string, value interface{}) Tags
	// Has checks if the given key exists.
	Has(key string) bool
	// Get returns value of given key
	Get(key string) interface{}
	// Values returns a map of key to values.
	// Do not modify the underlying map, please use Set instead.
	Values() map[string]interface{}

	// Foreach some kind like visitor pattern , each element pair will call f func
	//  and if accept return with error , the whole Foreach will return error
	// it some kind like each implementation in ruby https://ruby-doc.org/core-2.7.2/Hash.html#method-i-each
	//todo, require to review the necessary of Foreach to return error
	Foreach(f func(key string, val interface{}) error) error

	Len() int
}

type mapTags struct {
	values map[string]interface{}
}

// Set sets the given key in the metadata tags.
func (t *mapTags) Set(key string, value interface{}) Tags {
	t.values[key] = value
	return t
}

// Get returns value of given key
func (t *mapTags) Get(key string) interface{} {
	return t.values[key]
}

// Has checks if the given key exists.
func (t *mapTags) Has(key string) bool {
	_, ok := t.values[key]
	return ok
}

// Values returns a map of key to values.
// Do not modify the underlying map, please use Set instead.
func (t *mapTags) Values() map[string]interface{} {
	return t.values
}

func (t *mapTags) Foreach(f func(key string, val interface{}) error) error {
	for key, val := range t.values {
		if err := f(key, val); err != nil {
			return err
		}
	}
	return nil
}

func (t *mapTags) Len() int {
	return len(t.values)
}

func newMapTags(values map[string]interface{}) Tags {
	return &mapTags{values: values}
}

type noopTags struct{}

func (t *noopTags) Set(key string, value interface{}) Tags                  { return t }
func (t *noopTags) Get(key string) interface{}                              { return nil }
func (t *noopTags) Has(key string) bool                                     { return false }
func (t *noopTags) Values() map[string]interface{}                          { return nil }
func (t *noopTags) Len() int                                                { return 0 }
func (t *noopTags) Foreach(f func(key string, val interface{}) error) error { return nil }
