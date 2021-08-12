package goks

type StreamProcessor interface {
	ID() string
	process(kvc KeyValueContext)
}

type StreamProcessorFn func(kvc KeyValueContext) (StreamProcessor, KeyValueContext)
