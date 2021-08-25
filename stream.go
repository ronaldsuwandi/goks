package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ronaldsuwandi/goks/serde"
	"time"
)

type Stream struct {
	id              string
	internalCounter int

	topic string
	name  string

	processFn       NodeProcessorFn
	downstreamNodes []Node

	// TODO split key and value serializer
	serializer   serde.Serializer
	deserializer serde.Deserializer

	producerChan chan<- *kafka.Message
}

func (s *Stream) process(kvc KeyValueContext, src Node) {
	//deserialize + serialize?
	nextKvc, continueDownstream := s.processFn(kvc, src)

	if continueDownstream {
		for i := range s.downstreamNodes {
			s.downstreamNodes[i].process(nextKvc, s)
		}
	}
}

func (s *Stream) Filter(fn func(kvc KeyValueContext) bool) *Stream {
	next := NewStream(s.producerChan)

	next.processFn = func(kvc KeyValueContext, _ Node) (KeyValueContext, bool) {
		return kvc, fn(kvc)
	}

	s.downstreamNodes = append(s.downstreamNodes, &next)
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

	next.processFn = func(kvc KeyValueContext, _ Node) (KeyValueContext, bool) {
		vc := fn(kvc)
		return KeyValueContext{Key: kvc.Key, ValueContext: vc}, true
	}

	s.downstreamNodes = append(s.downstreamNodes, &next)
	return &next
}

func (s *Stream) Peek(fn func(kvc KeyValueContext)) *Stream {
	next := NewStream(s.producerChan)

	next.processFn = func(kvc KeyValueContext, src Node) (KeyValueContext, bool) {
		fn(kvc)
		return kvc, true
	}

	s.downstreamNodes = append(s.downstreamNodes, &next)
	return &next
}

func (s *Stream) Table(cached bool) *Table {
	next := NewTable(s.producerChan)
	next.internalCounter = s.internalCounter + 1
	next.id = generateID("STREAM-TO-TABLE", next.internalCounter)
	next.cached = cached
	if cached {
		next.commitCache = make(map[interface{}]KeyValueContext)
		next.stateStore = make(map[interface{}]KeyValueContext) // TODO separate cache and logging
	}
	s.downstreamNodes = append(s.downstreamNodes, &next)
	return &next
}

func (s *Stream) To(topic string, serializer serde.Serializer) {
	next := NewStream(s.producerChan)

	next.processFn = func(kvc KeyValueContext, _ Node) (KeyValueContext, bool) {
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

		return kvc, false
	}

	s.downstreamNodes = append(s.downstreamNodes, &next)
}

func (s *Stream) ID() string {
	return "stream"
}

func (s *Stream) Topic() string {
	return s.topic
}

func (s *Stream) DownstreamNodes() []Node {
	return s.downstreamNodes
}

func (s *Stream) Deserializer() serde.Deserializer {
	return s.deserializer
}

func (s *Stream) Serializer() serde.Serializer {
	return s.serializer
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
		processFn:       NoopProcessorFn(false),
		producerChan:    producerChan,
		downstreamNodes: []Node{},
	}
}

// TODO
// stream to Table - how will goks figure out if the Table is an input table from the topology builder
// input table need to work with ticker in the main goks start method

// TODO
// identifier set on topology builder
// identify cacheable table, cacheable tables should somehow be
// found on goks main thread so that it can force cache flush on ticker

// TODO implement join
