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

	serializer   serde.Serializer
	deserializer serde.Deserializer

	// FIXME this should be only handled on input level - need to
	cache            map[interface{}]interface{} // generics for key and value
	initialTimestamp time.Time
	commitInterval   time.Duration
	// ---

	// FIXME how to implement suppress?
}

func (t *Table) shouldForward(kvc KeyValueContext) bool {
	// TODO also return true if cache filled up
	afterCommit := kvc.Timestamp().Sub(t.initialTimestamp) >= t.commitInterval
	return afterCommit

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
		log.Println("diff=" + kvc.Timestamp().Sub(t.initialTimestamp).String() + " . commit interval=" + t.commitInterval.String())
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
	nextTables, nextKvcs := t.processHelper(kvc)
	for i := range nextTables {
		nextTables[i].process(nextKvcs[i])
	}
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
		commitInterval: time.Second, // FIXME commit interval only to be used for initial?
	}
}
