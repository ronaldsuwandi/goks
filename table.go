package goks

import (
	"github.com/ronaldsuwandi/goks/serde"
	"log"
	"time"
)

type tableProcessFn func(kvc KeyValueContext) (*Table, KeyValueContext)

type Table struct {
	topic      string
	name       string
	processFns []tableProcessFn
	input      bool // flag to indicate if this Table is an input (from topic or from stream)

	serializer   serde.Serializer
	deserializer serde.Deserializer

	// FIXME this should be only handled on input level - need to
	cache            map[interface{}]interface{} // generics for key and value
	initialTimestamp time.Time
	// ---

	stateStore  map[interface{}]interface{} // generics for key and value
	commitCache map[interface{}]KeyValueContext // for commit cache

	skipCommitCache bool // for proces that skips commit cache

	commitChan chan struct{}

	ticker *time.Ticker

	// FIXME how to implement suppress?
}

// TODO this is only applicable for Suppress
func (t *Table) shouldForward(kvc KeyValueContext) bool {
	// TODO also return true if cache filled up
	//afterCommit := kvc.Timestamp().Sub(t.initialTimestamp) >= t.commitInterval
	//return afterCommit
	return true
}

func (t *Table) processHelper(kvc KeyValueContext) ([]Table, []KeyValueContext) {
	var nextTables []Table
	var nextKvcs []KeyValueContext

	// ONLY CHECK THIS IF INPUT != ""
	if t.topic != "" {
		if t.initialTimestamp.IsZero() {
			log.Println("iszero!")
			t.initialTimestamp = kvc.Timestamp()
		} else {
			log.Println("notzero = " + t.initialTimestamp.String())
		}
	}

	if t.shouldForward(kvc) {
		//log.Println("diff=" + kvc.Timestamp().Sub(t.initialTimestamp).String() + " . commit interval=" + t.commitInterval.String())
		// reset timestamp for input topic only
		if t.topic != "" {
			t.initialTimestamp = kvc.Timestamp()
			log.Println("reset initial ts = " + t.initialTimestamp.String())
		}

		for _, fn := range t.processFns {
			nextTable, nextKvc := fn(kvc)
			if t != nil {
				nextTables = append(nextTables, *nextTable)
				nextKvcs = append(nextKvcs, nextKvc)
			}
		}
	}
	return nextTables, nextKvcs
}

func (t *Table) process(kvc KeyValueContext) {
	if !t.skipCommitCache {
		// FIXME race condition
		t.commitCache[kvc.Key] = kvc
	} else {
		t.downstream(kvc)
	}
}

func (t *Table) downstream(kvc KeyValueContext) {
	nextTables, nextKvcs := t.processHelper(kvc)
	for i := range nextTables {
		nextTables[i].process(nextKvcs[i])
	}
}

func (t *Table) flushCacheDownstream() {
	// FIXME high risk of race condition
	for i := range t.commitCache {
		t.downstream(t.commitCache[i])
	}
	// TODO clear commitCache and move it to stateStore
}

func (t *Table) MapValues(fn func(kvc KeyValueContext) ValueContext) *Table {
	next := NewTable()

	t.processFns = append(t.processFns, func(kvc KeyValueContext) (*Table, KeyValueContext) {
		vc := fn(kvc)
		return &next, KeyValueContext{Key: kvc.Key, ValueContext: vc}
	})
	return &next
}

// TODO commit interval to be configurable
func NewTable() Table {
	return Table{
	}
}

func NewInputTable(topic string, deserializer serde.Deserializer) Table {
	t := Table{
		topic:           topic,
		deserializer:    deserializer,
		processFns:      []tableProcessFn{}, // default do nothing
		skipCommitCache: false,
		commitChan:      make(chan struct{}),
	}

	return t
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

// FIXME

// decide to set chan or pass it like streams

// TODO functions
// filter, mapValues, joins, suppress, toStream
