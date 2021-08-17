package goks

import "github.com/ronaldsuwandi/goks/serde"

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

	stateStore  map[interface{}]interface{}     // generics for key and value
	commitCache map[interface{}]KeyValueContext // for commit cache

	cached bool

	// FIXME how to implement suppress?
}

func (t *Table) process(kvc KeyValueContext) {
	if t.cached {
		t.commitCache[kvc.Key] = kvc
	} else {
		t.downstream(kvc)
	}
}

func (t *Table) downstream(kvc KeyValueContext) {
	//deserialize + serialize?
	nextKvc, continueDownstream := t.processFn(kvc)

	if continueDownstream {
		for i := range t.downstreamNodes {
			t.downstreamNodes[i].process(nextKvc)
		}
	}
}

func (t *Table) flushCacheDownstream() {
	for i, kvc := range t.commitCache {
		t.downstream(kvc)
		// add to state store and remove commit cache
		t.stateStore[i] = kvc
		delete(t.commitCache, i)
	}
}

func (t *Table) MapValues(fn func(kvc KeyValueContext) ValueContext) *Table {
	next := NewTable()

	next.internalCounter = t.internalCounter + 1
	next.id = generateID("TABLE-MAPVALUES", next.internalCounter)
	next.processFn = func(kvc KeyValueContext) (KeyValueContext, bool) {
		vc := fn(kvc)
		return KeyValueContext{Key: kvc.Key, ValueContext: vc}, true
	}
	t.downstreamNodes = append(t.downstreamNodes, &next)

	return &next
}

// TODO commit interval to be configurable
func NewTable() Table {
	return Table{
		processFn:       NoopProcessorFn(true),
		downstreamNodes: []Node{},
		cached:          false,
	}
}

func NewInputTable(topic string, deserializer serde.Deserializer, counter int, cached bool) Table {
	t := Table{
		topic:           topic,
		deserializer:    deserializer,
		processFn:       NoopProcessorFn(true),
		downstreamNodes: []Node{},
		cached:          cached,
		stateStore:      make(map[interface{}]interface{}),
		commitCache:     make(map[interface{}]KeyValueContext),
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

// FIXME
/**
for input Table:
1. start a goroutine or time interval
2. goroutine will then signal when it's time to push context forward
3. table should have another method to push cached context downstream

cache need info:
key, value, timestamp, timestamp pushed

REMEMBER how to clear the time interval
*/

// TODO functions
// filter, joins, suppress, toStream
