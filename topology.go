package goks

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Topology struct {
	streams []Stream
	//table
	//global ktables
}

func (t Topology) Describe() string {
	return "printed version of topology here"
}

func (t Topology) pipe(msg *kafka.Message) {
	for i := range t.streams {
		t.streams[i].process(KeyValueContext{
			Key:   string(msg.Key),
			Value: string(msg.Value),
			Ctx:   context.Background(),
		})
	}
}
