package goks

import (
	"github.com/ronaldsuwandi/goks/serde"
	"log"
	"sync"
)

type StreamBuilder struct {
	mutex   *sync.Mutex
	streams []Stream
	tables  []Table
	//global ktables
	// config
}

func (sb *StreamBuilder) Stream(topic string, deserializer serde.Deserializer) *Stream {
	sb.mutex.Lock()
	sb.streams = append(sb.streams, Stream{
		topic:        topic,
		deserializer: deserializer,
		processFns:   []streamProcessFn{}, // default do nothing
	})
	result := &sb.streams[len(sb.streams)-1]
	sb.mutex.Unlock()
	return result
}

func (sb *StreamBuilder) Table(topic string, deserializer serde.Deserializer) *Table {
	sb.mutex.Lock()
	sb.tables = append(sb.tables, Table{
		topic:        topic,
		deserializer: deserializer,
		processFn:    noop, // default do nothing
	})
	result := &sb.tables[len(sb.tables)-1]
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
