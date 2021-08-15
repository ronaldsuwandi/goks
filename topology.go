package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Topology struct {
	streams []Stream
	//tables  []Table //FIXME fix table
	//global ktables
	producerChan chan *kafka.Message

	tableChans []chan struct{}
}

func (t Topology) Describe() string {
	return "printed version of topology here"
}

func ToKeyValueContext(msg *kafka.Message) KeyValueContext {
	//return KeyValueContext{
	//	Key: msg.K,
	//
	//}
	return KeyValueContext{}
}
