package goks

import (
	"github.com/ronaldsuwandi/goks/serde"
)

type streamProcessFn func(kvc KeyValueContext) (*Stream, KeyValueContext)

type Stream struct {
	topic      string
	name       string
	processFns []streamProcessFn

	serializer   serde.Serializer
	deserializer serde.Deserializer
}

func (s *Stream) Process(kvc KeyValueContext) ([]Stream, []KeyValueContext) {
	var nextStreams []Stream
	var nextKvcs []KeyValueContext

	for _, fn := range s.processFns {
		nextStream, nextKvc := fn(kvc)
		if s != nil {
			nextStreams = append(nextStreams, *nextStream)
			nextKvcs = append(nextKvcs, nextKvc)
		}
	}

	return nextStreams, nextKvcs
}

func (s *Stream) Filter(fn func(kvc KeyValueContext) bool) *Stream {
	next := NewStream()

	// deserialize
	s.processFns = append(s.processFns, func(kvc KeyValueContext) (*Stream, KeyValueContext) {
		if fn(kvc) {
			return &next, kvc
		} else {
			return nil, kvc
		}
	})

	//serialize ?
	return &next
}

//
//// FIXME Map should do repartition, need serializer
//func (s *Stream) Map(fn func(kvc KeyValueContext) KeyValueContext) *Stream {
//	next := NewStream()
//
//	s.processFn = func(kvc KeyValueContext) {
//		next.processFn(fn(kvc))
//	}
//
//	return &next
//}
//
func (s *Stream) MapValues(fn func(kvc KeyValueContext) ValueContext) *Stream {
	next := NewStream()

	s.processFns = append(s.processFns, func(kvc KeyValueContext) (*Stream, KeyValueContext) {
		vc := fn(kvc)
		return &next, KeyValueContext{Key: kvc.Key, ValueContext: vc}
	})
	return &next
}

func (s *Stream) Peek(fn func(kvc KeyValueContext)) *Stream {
	next := NewStream()

	s.processFns = append(s.processFns, func(kvc KeyValueContext) (*Stream, KeyValueContext) {
		fn(kvc)
		return &next, kvc
	})

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
	return Stream{
		processFns: []streamProcessFn{},
	}
}
