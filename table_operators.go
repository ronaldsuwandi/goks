package goks

func (t *Table) MapValues(fn func(kvc KeyValueContext) ValueContext) *Table {
	next := NewTable(t.producerChan)

	next.internalCounter = t.internalCounter + 1
	next.id = generateID("TABLE-MAPVALUES", next.internalCounter)
	next.processFn = func(kvc KeyValueContext, _ Node) (KeyValueContext, bool) {
		vc := fn(kvc)
		return KeyValueContext{Key: kvc.Key, ValueContext: vc}, true
	}
	t.downstreamNodes = append(t.downstreamNodes, &next)

	return &next
}

func (t *Table) Stream() *Stream {
	next := NewStream(t.producerChan)
	next.internalCounter = t.internalCounter + 1
	next.id = generateID("TABLE-TO-STREAM", next.internalCounter)
	next.processFn = NoopProcessorFn(true)
	t.downstreamNodes = append(t.downstreamNodes, &next)
	return &next
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
