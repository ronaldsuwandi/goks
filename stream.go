package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ronaldsuwandi/goks/serde"
	"time"
)

type Stream struct {
	topic      string
	name       string
	processFns []StreamProcessorFn

	// TODO split key and value serializer
	serializer   serde.Serializer
	deserializer serde.Deserializer

	producerChan chan<- *kafka.Message
}

func (s *Stream) processHelper(kvc KeyValueContext) ([]StreamProcessor, []KeyValueContext) {
	var nextStreams []StreamProcessor
	var nextKvcs []KeyValueContext

	for _, fn := range s.processFns {
		nextStream, nextKvc := fn(kvc)
		if nextStream != nil {
			nextStreams = append(nextStreams, nextStream)
			nextKvcs = append(nextKvcs, nextKvc)
		}
	}
	return nextStreams, nextKvcs
}

func (s *Stream) process(kvc KeyValueContext) {
	nextStreams, nextKvcs := s.processHelper(kvc)
	for i := range nextStreams {
		nextStreams[i].process(nextKvcs[i])
	}
}

func (s *Stream) Filter(fn func(kvc KeyValueContext) bool) *Stream {
	next := NewStream(s.producerChan)

	// deserialize
	s.processFns = append(s.processFns, func(kvc KeyValueContext) (StreamProcessor, KeyValueContext) {
		if fn(kvc) {
			return &next, kvc
		} else {
			return nil, kvc
		}
	})

	//serialize ?
	return &next
}

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

func (s *Stream) MapValues(fn func(kvc KeyValueContext) ValueContext) *Stream {
	next := NewStream(s.producerChan)

	s.processFns = append(s.processFns, func(kvc KeyValueContext) (StreamProcessor, KeyValueContext) {
		vc := fn(kvc)
		return &next, KeyValueContext{Key: kvc.Key, ValueContext: vc}
	})
	return &next
}

func (s *Stream) Peek(fn func(kvc KeyValueContext)) *Stream {
	next := NewStream(s.producerChan)

	s.processFns = append(s.processFns, func(kvc KeyValueContext) (StreamProcessor, KeyValueContext) {
		fn(kvc)
		return &next, kvc
	})

	return &next
}

func (s *Stream) To(topic string, serializer serde.Serializer) {
	s.processFns = append(s.processFns, func(kvc KeyValueContext) (StreamProcessor, KeyValueContext) {
		sk := serializer.Serialize(kvc.Key)
		sv := serializer.Serialize(kvc.Value)

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value:         sv,
			Key:           sk,
			Timestamp:     time.Now(),
			TimestampType: kafka.TimestampCreateTime,
			Headers:       kvc.Headers(),
		}
		// produce
		s.producerChan <- msg

		return nil, kvc
	})
}

func (s *Stream) ID() string {
	return "stream"
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

func NewStream(producerChan chan<- *kafka.Message) Stream {
	return Stream{
		processFns:   []StreamProcessorFn{},
		producerChan: producerChan,
	}
}

// TODO
// stream to Table - how will goks figure out if the Table is an input table from the topology builder
// input table need to work with ticker in the main goks start method

// TODO
// identifier set on topology builder
// identify cacheable table, cacheable tables should somehow be
// found on goks main thread so that it can force cache flush on ticker
