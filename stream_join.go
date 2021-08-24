package goks

type StreamJoiner struct {
	Stream

	source   Node
	joinerFn NodeJoinerFn
}

func (s *StreamJoiner) Source() Node {
	return s.source
}

func (s *StreamJoiner) InnerJoinTable(t *Table, joinerFn NodeJoinerFn) *Stream {
	next := NewStream(s.producerChan)

	next.processFn = func(kvc KeyValueContext, src Node) (KeyValueContext, bool) {
		joinerFn()
		vc := fn(kvc)
		return KeyValueContext{Key: kvc.Key, ValueContext: vc}, true
	}

	s.downstreamNodes = append(s.downstreamNodes, &next)
	return &next
}
