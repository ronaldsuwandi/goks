package goks

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func Start(t Topology) {

	// listen to message
	log.Println("Start!")
	for _, msg := range []string{"abc", "def", "abc"} {
		t.pipe(&kafka.Message{
			Key:   []byte(msg),
			Value: []byte(msg),
		})
	}
	log.Println("Done!")
}
