package goks

import (
	"fmt"
	"github.com/ronaldsuwandi/goks/serde"
)

type Node interface {
	ID() string
	Topic() string
	DownstreamNodes() []Node

	Deserializer() serde.Deserializer
	Serializer() serde.Serializer

	process(kvc KeyValueContext, src Node)
}

// NodeProcessorFn process the actual KeyValueContext and return both
// KeyValueContext and if the process continues downstream
type NodeProcessorFn func(kvc KeyValueContext, src Node) (newKvc KeyValueContext, continueDownstream bool)

func NoopProcessorFn(continueDownstream bool) NodeProcessorFn {
	return func(kvc KeyValueContext, _ Node) (KeyValueContext, bool) {
		return kvc, continueDownstream
	}
}

func generateID(prefix string, counter int) string {
	return fmt.Sprintf("%s-%d", prefix, counter)
}

type NodeJoiner interface {
	Node
	Source() Node
}

type NodeJoinerFn func(left KeyValueContext, right KeyValueContext) KeyValueContext
