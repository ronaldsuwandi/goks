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

	process(kvc KeyValueContext)
}

// NodeProcessorFn process the actual KeyValueContext and return both
// KeyValueContext and if the process continues downstream
type NodeProcessorFn func(kvc KeyValueContext) (newKvc KeyValueContext, continueDownstream bool)

func NoopProcessorFn(continueDownstream bool) NodeProcessorFn {
	return func(kvc KeyValueContext) (KeyValueContext, bool) {
		return kvc, continueDownstream
	}
}

func generateID(prefix string, counter int) string {
	return fmt.Sprintf("%s-%d", prefix, counter)
}
