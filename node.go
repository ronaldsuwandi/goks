package goks

import "fmt"

type Node interface {
	ID() string
	DownstreamNodes() []Node
	process(kvc KeyValueContext)
}

type NodeProcessorFn func(kvc KeyValueContext) (bool, KeyValueContext)

func generateID(prefix string, counter int) string {
	return fmt.Sprintf("%s-%d", prefix, counter)
}
