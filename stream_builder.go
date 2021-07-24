package goks

import (
	"log"
	"sync"
)

type StreamBuilder struct {
	mutex   *sync.Mutex
	streams []Stream
	//tables
	//global ktables
}

func (sb *StreamBuilder) Stream(topic string) *Stream {
	sb.mutex.Lock()
	sb.streams = append(sb.streams, Stream{
		topic:   topic,
		process: noop, // default do nothing
	})
	result := &sb.streams[len(sb.streams)-1]
	sb.mutex.Unlock()
	return result
}

func (sb *StreamBuilder) Print() {
	log.Printf("sb addr: %+v\n", sb.streams[0])
}

func (sb StreamBuilder) Build() Topology {
	return Topology{
		streams: sb.streams,
	}
}

func noop(_ KeyValueContext) {}

func NewStreamBuilder( /*config*/ ) StreamBuilder {
	return StreamBuilder{
		mutex: &sync.Mutex{},
	}
}
