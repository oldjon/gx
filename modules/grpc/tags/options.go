package tags

const (
	DefaultTagsPrefix = "fx-tags"
)

type options struct {
	tagsPrefix string
}

func evaluateOptions(opts []Option) *options {
	os := &options{
		tagsPrefix: DefaultTagsPrefix,
	}
	for _, o := range opts {
		o(os)
	}
	return os
}

type Option func(*options)

// WithTagsPrefix set header tags name
func WithTagsPrefix(tagsPrefix string) Option {
	return func(o *options) {
		o.tagsPrefix = tagsPrefix
	}
}
