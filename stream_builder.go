package goks

import "log"

type StreamBuilder struct {
	input []Stream
}

func (sb *StreamBuilder) Stream(topic string) *Stream {
	// TODO mutex
	sb.input = append(sb.input, Stream{topic: topic})
	return &sb.input[len(sb.input)-1]
}

func (sb *StreamBuilder) Print() {
	log.Printf("sb addr: %+v\n", sb.input[0])
}

func (sb StreamBuilder) Build() Topology {
	return Topology{
		input: sb.input,
	}
}
