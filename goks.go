package goks

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Goks struct {
	consumer *kafka.Consumer
	topology Topology

	commitInterval time.Duration
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

func (g *Goks) shouldForward(msg *kafka.Message, initialTimestamp time.Time) bool {
	// TODO also return true if cache filled up
	afterCommit := msg.Timestamp.Sub(initialTimestamp) >= g.commitInterval
	return afterCommit
}

func (g *Goks) ProcessStream(s Stream, kvc KeyValueContext) {
	nextStreams, nextKvcs := s.Process(kvc)
	for i := range nextStreams {
		g.ProcessStream(nextStreams[i], nextKvcs[i])
	}
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

	var initialTimestamp time.Time

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
				//fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
				//if e.Headers != nil {
				//	fmt.Printf("%% Headers: %v\n", e.Headers)
				//}

				for _, s := range g.topology.streams {
					// deserialize here
					dk := s.deserializer.Deserialize(e.Key)
					dv := s.deserializer.Deserialize(e.Value)

					kvc := KeyValueContext{
						Key: dk,
						ValueContext: ValueContext{
							Value: dv,
							Ctx:   contextFrom(e),
						},
					}

					g.ProcessStream(s, kvc)
				}

				if initialTimestamp.IsZero() {
					initialTimestamp = e.Timestamp
				}

				//e.Timestamp
				//TODO commit.interval.ms cache
				//g.topology.pipeTables(e)
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
