package goks

import (
	"github.com/ronaldsuwandi/goks/serde"
)

type Table struct {
	topic      string
	name       string
	processFns []StreamProcessorFn
	input      bool // flag to indicate if this Table is an input (from topic or from stream)

	serializer   serde.Serializer
	deserializer serde.Deserializer

	stateStore  map[interface{}]interface{}     // generics for key and value
	commitCache map[interface{}]KeyValueContext // for commit cache

	skipCommitCache bool // for proces that skips commit cache

	// FIXME how to implement suppress?
}

func (t *Table) process(kvc KeyValueContext) {
	if !t.skipCommitCache {
		t.commitCache[kvc.Key] = kvc
	} else {
		t.downstream(kvc)
	}
}

func (t *Table) downstreamHelper(kvc KeyValueContext) ([]StreamProcessor, []KeyValueContext) {
	var nextTables []StreamProcessor
	var nextKvcs []KeyValueContext
	for _, fn := range t.processFns {
		nextTable, nextKvc := fn(kvc)
		if t != nil {
			nextTables = append(nextTables, nextTable)
			nextKvcs = append(nextKvcs, nextKvc)
		}
	}
	return nextTables, nextKvcs
}

func (t *Table) downstream(kvc KeyValueContext) {
	nextTables, nextKvcs := t.downstreamHelper(kvc)
	for i := range nextTables {
		nextTables[i].process(nextKvcs[i])
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

	t.processFns = append(t.processFns, func(kvc KeyValueContext) (StreamProcessor, KeyValueContext) {
		vc := fn(kvc)
		return &next, KeyValueContext{Key: kvc.Key, ValueContext: vc}
	})
	return &next
}

// TODO commit interval to be configurable
func NewTable() Table {
	return Table{
		skipCommitCache: true,
	}
}

func NewInputTable(topic string, deserializer serde.Deserializer) Table {
	t := Table{
		topic:           topic,
		deserializer:    deserializer,
		processFns:      []StreamProcessorFn{}, // default do nothing
		skipCommitCache: false,
		stateStore:      make(map[interface{}]interface{}),
		commitCache:     make(map[interface{}]KeyValueContext),
	}

	return t
}

func (t *Table) ID() string {
	return "table"
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
// filter, mapValues, joins, suppress, toStream
