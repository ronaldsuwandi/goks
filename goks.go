package goks

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Goks struct {
	consumer *kafka.Consumer
	topology Topology
}

func New(t Topology) (Goks, error) {
	// FIXME use proper config
	consumerConf := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}

	c, err := kafka.NewConsumer(consumerConf)

	g := Goks{
		consumer: c,
		topology: t,
	}

	return g, err
}

func (g *Goks) Start() error {
	// TODO
	// - validate topology first
	// - go through all nodes, figure out if need to
	//   - restore from changelog/state store and restore table accordingly
	// - figure out if source topic is different (repartition topic)
	// - once table is restored
	// - read from repartition topic first, once all done
	// - set task as ready
	// - start consuming from original source topic

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	inputTopics := []string{}

	for _, s := range g.topology.streams {
		inputTopics = append(inputTopics, s.topic)
	}

	// TODO deal with rebalance
	err := g.consumer.SubscribeTopics(inputTopics, nil)
	if err != nil {
		return err
	}

	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Terminating: %v\n", sig)
			run = false
			g.Stop()
		default:
			ev := g.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				g.topology.pipe(e)
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	return nil
}

func (g *Goks) Stop() {
	log.Println("OEI")
	g.consumer.Close()
}
