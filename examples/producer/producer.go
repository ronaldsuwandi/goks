package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	producerConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	p, err := kafka.NewProducer(producerConf)
	if err != nil {
		log.Panicln("failed to create producer", err)
	}

	a, err := kafka.NewAdminClientFromProducer(p)
	if err != nil {
		log.Panicln("failed to create admin", err)
	}

	topic := "input"
	log.Println("trying to create topic")
	topicResult, err := a.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	})

	if err != nil {
		log.Panicln("failed to create topic", err)
	}

	for _, tr := range topicResult {
		if tr.Error.Code() == kafka.ErrTopicAlreadyExists {
			log.Println("topic already exists")
		} else if tr.Error.Code() != kafka.ErrNoError {
			log.Printf("unable to create topic: %+v\n", tr.Error)
		} else if tr.Error.Code() == kafka.ErrNoError {
			log.Println("created input topic")
		}
	}

	duration := 500 * time.Millisecond
	count := 0
	log.Printf("producing every %d ms\n", duration.Milliseconds())

	ticker := time.NewTicker(duration)
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Terminating: %v\n", sig)
			ticker.Stop()
			run = false
		case <-ticker.C:
			key := []byte(fmt.Sprintf("key-%d",count%2))
			value :=[]byte(fmt.Sprintf("%d", count))

			log.Printf("publish message %s=%s\n",key,value)

			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Value:         value,
				Key:           key,
				Timestamp:     time.Now(),
				TimestampType: kafka.TimestampCreateTime,
			}, nil)

			if err != nil {
				log.Printf("error producing message: %+v\n", err)
			}

			count++
		}
	}
	log.Printf("done. published %d messages\n", count)
}
