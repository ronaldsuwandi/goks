package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ronaldsuwandi/goks/serde"
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

// repartition needs serde interaction

func NewStream(producerChan chan<- *kafka.Message) Stream {
	return Stream{
		processFn:       NoopProcessorFn(false),
		producerChan:    producerChan,
		downstreamNodes: []Node{},
	}
}
