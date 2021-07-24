package goks

type Stream struct {
	topic   string
	name    string
	process func(kvc KeyValueContext)
}

func (s *Stream) Filter(fn func(kvc KeyValueContext) bool) *Stream {
	next := Stream{}

	s.process = func(kvc KeyValueContext) {
		if fn(kvc) {
			next.process(kvc)
		}
	}

	return &next
}

func (s *Stream) Map(fn func(kvc KeyValueContext) KeyValueContext) *Stream {
	next := Stream{}

	s.process = func(kvc KeyValueContext) {
		next.process(fn(kvc))
	}

	return &next
}

func (s *Stream) Peek(fn func(kvc KeyValueContext)) *Stream {
	s.process = func(kvc KeyValueContext) {
		fn(kvc)
	}
	return s
}

func (s *Stream) Branch(fns ...func(kvc KeyValueContext) bool) []Stream {
	branches := make([]Stream, len(fns))
	for i := range fns {
		branches[i] = Stream{}
	}

	s.process = func(kvc KeyValueContext) {
		for i, fn := range fns {
			if fn(kvc) {
				branches[i].process(kvc)
			}
		}
	}

	return branches
}
