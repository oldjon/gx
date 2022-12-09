package service

type Builder struct {
	options []Option
	modules []moduleInfo
}

func SetupModule(provider ModuleProvider, options ...ModuleOption) *Builder {
	b := Builder{}
	return b.SetupModule(provider, options...)
}

func (b *Builder) WithOptions(options ...Option) *Builder {
	b.options = append(b.options, options...)
	return b
}

func (b *Builder) SetupModule(provider ModuleProvider, options ...ModuleOption) *Builder {
	b.modules = append(b.modules, moduleInfo{provider, options})
	return b
}

func (b *Builder) Build() (Host, error) {
	return newHost(*b)
}

type moduleInfo struct {
	provider ModuleProvider
	options  []ModuleOption
}
