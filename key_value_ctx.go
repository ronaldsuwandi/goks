package goks

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

// FIXME generics? 1.17 (august 2021) - official in go 1.18 (feb 2022)
type ValueContext struct {
	Value interface{} // will be using generics
	Ctx   context.Context
}

type KeyValueContext struct {
	Key interface{} // will be using generics
	ValueContext
}

const (
	Timestamp = iota
	TimestampType
	Headers
)

func (vc ValueContext) Timestamp() time.Time {
	return vc.Ctx.Value(Timestamp).(time.Time)
}

func (vc ValueContext) TimestampType() kafka.TimestampType {
	return vc.Ctx.Value(TimestampType).(kafka.TimestampType)
}

func (vc ValueContext) Headers() []kafka.Header {
	return vc.Ctx.Value(Headers).([]kafka.Header)
}
