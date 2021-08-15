package goks

//type Table struct {
//	id              string
//	internalCounter int
//
//	topic string
//	name  string
//
//	downstreamNodes []Node
//	processFns      []NodeProcessorFn
//	input           bool // flag to indicate if this Table is an input (from topic or from stream)
//
//	serializer   serde.Serializer
//	deserializer serde.Deserializer
//
//	stateStore  map[interface{}]interface{}     // generics for key and value
//	commitCache map[interface{}]KeyValueContext // for commit cache
//
//	cached bool
//
//	// FIXME how to implement suppress?
//}
//
//func (t *Table) process(kvc KeyValueContext) {
//	if !t.cached {
//		t.commitCache[kvc.Key] = kvc
//	} else {
//		t.downstream(kvc)
//	}
//}
//
//func (t *Table) downstreamHelper(kvc KeyValueContext) ([]Node, []KeyValueContext) {
//	var nextTables []Node
//	var nextKvcs []KeyValueContext
//	for _, fn := range t.processFns {
//		nextTable, nextKvc := fn(kvc)
//		if t != nil {
//			nextTables = append(nextTables, nextTable)
//			nextKvcs = append(nextKvcs, nextKvc)
//		}
//	}
//	return nextTables, nextKvcs
//}
//
//func (t *Table) downstream(kvc KeyValueContext) {
//	nextTables, nextKvcs := t.downstreamHelper(kvc)
//	for i := range nextTables {
//		nextTables[i].process(nextKvcs[i])
//	}
//}
//
//func (t *Table) flushCacheDownstream() {
//	for i, kvc := range t.commitCache {
//		t.downstream(kvc)
//		// add to state store and remove commit cache
//		t.stateStore[i] = kvc
//		delete(t.commitCache, i)
//	}
//}
//
//func (t *Table) MapValues(fn func(kvc KeyValueContext) ValueContext) *Table {
//	next := NewTable()
//
//	next.internalCounter = t.internalCounter+1
//	next.id = generateID("TABLE-MAPVALUES", next.internalCounter)
//
//	t.downstreamNodes = append(t.downstreamNodes, &next)
//	t.processFns = append(t.processFns, func(kvc KeyValueContext) (Node, KeyValueContext) {
//		vc := fn(kvc)
//		return &next, KeyValueContext{Key: kvc.Key, ValueContext: vc}
//	})
//	return &next
//}
//
//// TODO commit interval to be configurable
//func NewTable() Table {
//	return Table{
//		processFns:      []NodeProcessorFn{}, // default do nothing
//		downstreamNodes: []Node{},
//		cached:          true,
//	}
//}
//
//func NewInputTable(topic string, deserializer serde.Deserializer, counter int) Table {
//	t := Table{
//		topic:           topic,
//		deserializer:    deserializer,
//		processFns:      []NodeProcessorFn{}, // default do nothing
//		downstreamNodes: []Node{},
//		cached:          false,
//		stateStore:      make(map[interface{}]interface{}),
//		commitCache:     make(map[interface{}]KeyValueContext),
//		internalCounter: counter,
//	}
//
//	t.id = generateID("TABLE", t.internalCounter)
//
//	return t
//}
//
//func (t *Table) ID() string {
//	return t.id
//}
//
//func (t *Table) DownstreamNodes() []Node {
//	return t.downstreamNodes
//}

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
