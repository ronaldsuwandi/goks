package goks

import "github.com/confluentinc/confluent-kafka-go/kafka"

type StreamJoiner struct {
	Stream

	source     Node
	joinerFn   NodeJoinerFn
	stateStore map[interface{}]KeyValueContext // generics for key and value]
}

func (s *StreamJoiner) Source() Node {
	return s.source
}

func (s *Stream) InnerJoinTable(t *Table, joinerFn NodeJoinerFn) *Stream {
	next := NewStreamJoiner(s.producerChan, s)

	next.processFn = func(kvc KeyValueContext, src Node) (KeyValueContext, bool) {
		if src != next.source {
			next.stateStore[kvc.Key] = kvc
			return kvc, false
		}

		right, ok := next.stateStore[kvc.Key]
		if ok {
			result := joinerFn(kvc, right)
			return result, true
		}

		return kvc, false
	}

	s.downstreamNodes = append(s.downstreamNodes, &next)
	t.downstreamNodes = append(t.downstreamNodes, &next)
	return &next.Stream
}

func NewStreamJoiner(producerChan chan<- *kafka.Message, source Node) StreamJoiner {
	return StreamJoiner{
		Stream: Stream{
			processFn:       NoopProcessorFn(false),
			producerChan:    producerChan,
			downstreamNodes: []Node{},
		},

		source:     source,
		stateStore: make(map[interface{}]KeyValueContext),
	}
}
