package goks

import (
	"github.com/ronaldsuwandi/goks/serde"
	"log"
	"sync"
)

type StreamBuilder struct {
	mutex   *sync.Mutex
	streams []Stream
	//tables
	//global ktables
	// config
}

func (sb *StreamBuilder) Stream(topic string, deserializer serde.Deserializer) *Stream {
	sb.mutex.Lock()
	sb.streams = append(sb.streams, Stream{
		topic:        topic,
		deserializer: deserializer,
		processFn:    noop, // default do nothing
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
		// config: config,
	}
}
