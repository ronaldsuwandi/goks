package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ronaldsuwandi/goks/serde"
)

type TopologyBuilder struct {
	nodes []Node
	//streams []Stream
	//tables  []Table
	//global ktables

	// config
	producerChan chan *kafka.Message

	counter int
}

func (tb *TopologyBuilder) Stream(topic string, deserializer serde.Deserializer) *Stream {
	s := Stream{
		topic:        topic,
		deserializer: deserializer,

		processFn: func(kvc KeyValueContext, _ Node) (KeyValueContext, bool) {
			return kvc, true
		},

		producerChan: tb.producerChan,

		internalCounter: tb.counter,
	}
	s.id = generateID("STREAM-INPUT", s.internalCounter)
	tb.nodes = append(tb.nodes, &s)
	tb.counter++
	return &s
}

func (tb *TopologyBuilder) Table(topic string, deserializer serde.Deserializer) *Table {
	t := NewInputTable(topic, deserializer, tb.counter, true, tb.producerChan)
	tb.nodes = append(tb.nodes, &t)
	tb.counter++
	return &t
}

func (tb *TopologyBuilder) Print() {
	//log.Printf("tb addr: %+v\n", tb.streams[0])
}

func findCachedTables(n Node) []*Table {
	t, ok := n.(*Table)
	var result []*Table
	if ok && t.cached {
		result = append(result, t)
	}

	for _, d := range n.DownstreamNodes() {
		downstreamCachedTables := findCachedTables(d)
		for i := range downstreamCachedTables {
			result = append(result, downstreamCachedTables[i])
		}
	}

	return result
}

func (tb TopologyBuilder) Build() Topology {
	// FIXME iterate through topology...
	var cachedTables []*Table
	for _, n := range tb.nodes {
		downstreamCachedTables := findCachedTables(n)
		for i := range downstreamCachedTables {
			cachedTables = append(cachedTables, downstreamCachedTables[i])
		}
	}

	// TODO iterate through input streams, find if there's Table() method

	// FIXME validate topology

	return Topology{
		nodes:        tb.nodes,
		cachedTables: cachedTables,
		producerChan: tb.producerChan,
	}
}

func NewTopologyBuilder( /*config*/ ) TopologyBuilder {
	return TopologyBuilder{
		producerChan: make(chan *kafka.Message),
		// config: config,
	}
}
