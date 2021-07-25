package goks

import (
	"github.com/ronaldsuwandi/goks/serde"
)

type Stream struct {
	topic     string
	name      string
	processFn func(kvc KeyValueContext)

	serializer   serde.Serializer
	deserializer serde.Deserializer
}

func (s *Stream) Filter(fn func(kvc KeyValueContext) bool) *Stream {
	next := NewStream()

	// deserialize
	s.processFn = func(kvc KeyValueContext) {
		if fn(kvc) {
			next.processFn(kvc)
		}
	}

	//serialize

	return &next
}

// FIXME Map should do repartition, need serializer
func (s *Stream) Map(fn func(kvc KeyValueContext) KeyValueContext) *Stream {
	next := NewStream()

	s.processFn = func(kvc KeyValueContext) {
		next.processFn(fn(kvc))
	}

	return &next
}

func (s *Stream) MapValues(fn func(kvc KeyValueContext) ValueContext) *Stream {
	next := NewStream()

	s.processFn = func(kvc KeyValueContext) {
		vc := fn(kvc)
		next.processFn(KeyValueContext{
			Key:          kvc.Key,
			ValueContext: vc,
		})
	}
	return &next
}

func (s *Stream) Peek(fn func(kvc KeyValueContext)) *Stream {
	next := NewStream()
	s.processFn = func(kvc KeyValueContext) {
		fn(kvc)
		next.processFn(kvc)
	}
	return &next
}

//
//func (s *Stream) Branch(fns ...func(kvc KeyValueContext) bool) []Stream {
//	branches := make([]Stream, len(fns))
//	for i := range fns {
//		branches[i] = Stream{}
//	}
//
//	s.processFn = func(kvc KeyValueContext) {
//		for i, fn := range fns {
//			if fn(kvc) {
//				branches[i].processFn(kvc)
//			}
//		}
//	}
//
//	return branches
//}

// repartition needs serde interaction

func NewStream() Stream {
	return Stream{processFn: noop}
}
