package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Topology struct {
	nodes        []Node
	cachedTables []*Table
	//streams []Stream
	//tables  []Table
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
