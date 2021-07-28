package goks

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Topology struct {
	streams []Stream
	tables []Table
	//global ktables
}

func (t Topology) Describe() string {
	return "printed version of topology here"
}

func (t Topology) pipeStreams(msg *kafka.Message) {
	for i := range t.streams {

		// deserialize here
		dk := t.streams[i].deserializer.Deserialize(msg.Key)
		dv := t.streams[i].deserializer.Deserialize(msg.Value)

		t.streams[i].processFn(KeyValueContext{
			Key: dk,
			ValueContext: ValueContext{
				Value: dv,
				Ctx:   contextFrom(msg),
			},
		})
	}
}

func (t Topology) pipeTables(msg *kafka.Message) {
	for i := range t.tables {

		// deserialize here
		dk := t.tables[i].deserializer.Deserialize(msg.Key)
		dv := t.tables[i].deserializer.Deserialize(msg.Value)

		t.tables[i].processFn(KeyValueContext{
			Key: dk,
			ValueContext: ValueContext{
				Value: dv,
				Ctx:   contextFrom(msg),
			},
		})
	}
}


func contextFrom(msg *kafka.Message) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, Timestamp, msg.Timestamp)
	ctx = context.WithValue(ctx, TimestampType, msg.TimestampType)
	ctx = context.WithValue(ctx, Headers, msg.Headers)
	return ctx
}
