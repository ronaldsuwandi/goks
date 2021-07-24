package goks

import "context"

// FIXME generics? in go 1.17 (august 2021)
type KeyValueContext struct {
	Key   string
	Value string
	Ctx   context.Context
}
