package goks

import (
	"github.com/ronaldsuwandi/goks/serde"
	"log"
	"sync"
	"time"
)

type TopologyBuilder struct {
	mutex   *sync.Mutex
	streams []Stream
	tables  []Table
	//global ktables
	// config
}

func (tb *TopologyBuilder) Stream(topic string, deserializer serde.Deserializer) *Stream {
	tb.mutex.Lock()
	tb.streams = append(tb.streams, Stream{
		topic:        topic,
		deserializer: deserializer,
		processFns:   []streamProcessFn{}, // default do nothing
	})
	result := &tb.streams[len(tb.streams)-1]
	tb.mutex.Unlock()
	return result
}

func (tb *TopologyBuilder) Table(topic string, deserializer serde.Deserializer) *Table {
	tb.mutex.Lock()
	tb.tables = append(tb.tables, Table{
		topic:          topic,
		deserializer:   deserializer,
		processFns:     []tableProcessFn{}, // default do nothing
		input:          true,
		commitInterval: time.Second, // FIXME this should be only on the input level?

	})
	result := &tb.tables[len(tb.tables)-1]
	tb.mutex.Unlock()
	return result
}

func (tb *TopologyBuilder) Print() {
	log.Printf("tb addr: %+v\n", tb.streams[0])
}

func (tb TopologyBuilder) Build() Topology {
	return Topology{
		streams: tb.streams,
		tables:  tb.tables,
	}
}

func noop(_ KeyValueContext) {}

func NewTopologyBuilder( /*config*/ ) TopologyBuilder {
	return TopologyBuilder{
		mutex: &sync.Mutex{},
		// config: config,
	}
}
