package goks

import "log"

type Stream struct {
	A     string
	topic string

	process func(msg string)
}

func (s *Stream) Filter(fn func(msg string) bool) *Stream {
	next := Stream{}

	s.process = func(msg string) {
		log.Println("filtering: %s", msg)
		if fn(msg) {
			log.Println("true, proceed next")
			next.process(msg)
		}
	}

	return &next
}

func (s *Stream) Map(fn func(msg string) string) *Stream {
	next := Stream{}
	s.process = func(msg string) {
		next.process(fn(msg))
	}
	return &next
}

func (s *Stream) Peek(fn func(msg string)) {
	s.process = func(msg string) {
		fn(msg)
	}
}

func (s *Stream) Branch(fns ...func(msg string) bool) []Stream {
	branches := make([]Stream, len(fns))
	for i := range fns {
		branches[i] = Stream{}
	}

	s.process = func(msg string) {
		for i, fn := range fns {
			if fn(msg) {
				branches[i].process(msg)
			}
		}
	}

	return branches
}
