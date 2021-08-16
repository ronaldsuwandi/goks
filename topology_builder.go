package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ronaldsuwandi/goks/serde"
	"log"
)

type TopologyBuilder struct {
	streams []Stream
	tables  []Table
	//global ktables

	// config
	producerChan chan *kafka.Message

	counter int
}

func (tb *TopologyBuilder) Stream(topic string, deserializer serde.Deserializer) *Stream {
	tb.streams = append(tb.streams, Stream{
		topic:        topic,
		deserializer: deserializer,

		processFn: func(kvc KeyValueContext) (KeyValueContext, bool) {
			return kvc, true
		},

		producerChan: tb.producerChan,

		internalCounter: tb.counter,
	})
	tb.counter++
	result := &tb.streams[len(tb.streams)-1]
	return result
}

func (tb *TopologyBuilder) Table(topic string, deserializer serde.Deserializer) *Table {
	tb.tables = append(tb.tables, NewInputTable(topic, deserializer, tb.counter, true))
	tb.counter++
	result := &tb.tables[len(tb.tables)-1]
	return result
}

func (tb *TopologyBuilder) Print() {
	log.Printf("tb addr: %+v\n", tb.streams[0])
}

func findTables(n Node, result []Node) {
	for _, d := range n.DownstreamNodes() {
		t, ok := d.(*Table)
		if ok && t.cached {
			result = append(result, t)
		}

		findTables(d, result)
	}

}

func (tb TopologyBuilder) Build() Topology {
	// FIXME iterate through topology...
	//[]
	//tables := tb.tables
	//tablesFromStream
	//tablesFromTablee (table->stream->CACHEDtable)
	//for _, t := range tb.streams

	// TODO iterate through input streams, find if there's Table() method


	// FIXME validate topology



	return Topology{
		streams:      tb.streams,
		tables:       tb.tables,
		producerChan: tb.producerChan,
	}
}

func noop(_ KeyValueContext) {}

func NewTopologyBuilder( /*config*/ ) TopologyBuilder {
	return TopologyBuilder{
		producerChan: make(chan *kafka.Message),
		// config: config,
	}
}
