package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ronaldsuwandi/goks/serde"
	"log"
	"sync"
)

type TopologyBuilder struct {
	mutex   *sync.Mutex
	streams []Stream
	tables  []Table
	//global ktables

	// config
	producerChan chan *kafka.Message
}

func (tb *TopologyBuilder) Stream(topic string, deserializer serde.Deserializer) *Stream {
	tb.mutex.Lock()
	tb.streams = append(tb.streams, Stream{
		topic:        topic,
		deserializer: deserializer,
		processFns:   []StreamProcessorFn{}, // default do nothing
		producerChan: tb.producerChan,
	})
	result := &tb.streams[len(tb.streams)-1]
	tb.mutex.Unlock()
	return result
}

func (tb *TopologyBuilder) Table(topic string, deserializer serde.Deserializer) *Table {
	tb.mutex.Lock()
	tb.tables = append(tb.tables, NewInputTable(topic, deserializer))
	result := &tb.tables[len(tb.tables)-1]
	tb.mutex.Unlock()
	return result
}

func (tb *TopologyBuilder) Print() {
	log.Printf("tb addr: %+v\n", tb.streams[0])
}

func (tb TopologyBuilder) Build() Topology {
	// FIXME iterate through topology...

	return Topology{
		streams:      tb.streams,
		tables:       tb.tables,
		producerChan: tb.producerChan,
	}
}

func noop(_ KeyValueContext) {}

func NewTopologyBuilder( /*config*/ ) TopologyBuilder {
	return TopologyBuilder{
		mutex:        &sync.Mutex{},
		producerChan: make(chan *kafka.Message),
		// config: config,
	}
}
