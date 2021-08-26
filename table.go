package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ronaldsuwandi/goks/serde"
)

type Table struct {
	id              string
	internalCounter int

	topic string
	name  string

	downstreamNodes []Node
	processFn       NodeProcessorFn

	input bool // flag to indicate if this Table is an input (from topic or from stream)

	serializer   serde.Serializer
	deserializer serde.Deserializer

	stateStore  map[interface{}]KeyValueContext // generics for key and value
	commitCache map[interface{}]KeyValueContext // for commit cache

	cached bool

	producerChan chan<- *kafka.Message

	// FIXME how to implement suppress?
}

func (t *Table) process(kvc KeyValueContext, src Node) {
	if t.cached {
		t.commitCache[kvc.Key] = kvc
	} else {
		t.downstream(kvc, src)
	}
}

func (t *Table) downstream(kvc KeyValueContext, src Node) {
	//deserialize + serialize?
	nextKvc, continueDownstream := t.processFn(kvc, src)

	if continueDownstream {
		for i := range t.downstreamNodes {
			t.downstreamNodes[i].process(nextKvc, t)
		}
	}
}

func (t *Table) flushCacheDownstream() {
	for i, kvc := range t.commitCache {
		t.downstream(kvc, nil)
		// add to state store and remove commit cache
		t.stateStore[i] = kvc
		delete(t.commitCache, i)
	}
}

// TODO commit interval to be configurable
func NewTable(producerChan chan<- *kafka.Message) Table {
	return Table{
		processFn:       NoopProcessorFn(true),
		downstreamNodes: []Node{},
		cached:          false,
		producerChan:    producerChan,
	}
}

func NewInputTable(topic string, deserializer serde.Deserializer, counter int, cached bool, producerChan chan<- *kafka.Message) Table {
	t := Table{
		topic:           topic,
		deserializer:    deserializer,
		processFn:       NoopProcessorFn(true),
		downstreamNodes: []Node{},
		cached:          cached,
		stateStore:      make(map[interface{}]KeyValueContext),
		commitCache:     make(map[interface{}]KeyValueContext),
		producerChan:    producerChan,
		internalCounter: counter,
	}

	t.id = generateID("TABLE", t.internalCounter)

	return t
}

func (t *Table) ID() string {
	return t.id
}

func (t *Table) Topic() string {
	return t.topic
}

func (t *Table) Deserializer() serde.Deserializer {
	return t.deserializer
}

func (t *Table) Serializer() serde.Serializer {
	return t.serializer
}

func (t *Table) DownstreamNodes() []Node {
	return t.downstreamNodes
}
