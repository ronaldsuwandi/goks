package goks

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

type Goks struct {
	consumer *kafka.Consumer
	producer *kafka.Producer
	topology Topology

	commitInterval time.Duration
	commitTicker *time.Ticker
}

func New(t Topology) (Goks, error) {
	// FIXME use proper config
	consumerConf := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true, // TODO handle commit
	}

	c, err := kafka.NewConsumer(consumerConf)

	if err != nil {
		return Goks{}, err
	}

	producerConf := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	p, err := kafka.NewProducer(producerConf)
	if err != nil {
		return Goks{}, err
	}

	g := Goks{
		consumer: c,
		producer: p,
		topology: t,

		commitInterval: 2 * time.Second, // FIXME use config
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

	for _, t := range g.topology.tables {
		inputTopics = append(inputTopics, t.topic)
	}

	// TODO deal with rebalance
	err := g.consumer.SubscribeTopics(inputTopics, nil)
	if err != nil {
		return err
	}

	run := true

	// TODO need this for produer?
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	g.commitTicker = time.NewTicker(g.commitInterval)

	go func() {
		log.Println("running producer code")
		for {
			select {
			case msg := <-g.topology.producerChan:
				log.Println("OEI")
				g.producer.Produce(msg, nil)
			}
		}
	}()

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Terminating: %v\n", sig)
			run = false
			g.Stop()

		case <-g.commitTicker.C:
			log.Println("tick. push downstream for tables")
			for i := range g.topology.tables {
				g.topology.tables[i].flushCacheDownstream()
			}

		default:
			ev := g.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				//fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
				for i, s := range g.topology.streams {
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

					g.topology.streams[i].process(kvc)
				}

				for i, t := range g.topology.tables {
					// deserialize here
					dk := t.deserializer.Deserialize(e.Key)
					dv := t.deserializer.Deserialize(e.Value)

					kvc := KeyValueContext{
						Key: dk,
						ValueContext: ValueContext{
							Value: dv,
							Ctx:   contextFrom(e),
						},
					}

					g.topology.tables[i].process(kvc)
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
	log.Println("OEI STOP")
	g.consumer.Close()
	g.commitTicker.Stop()
}

func contextFrom(msg *kafka.Message) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, Timestamp, msg.Timestamp)
	ctx = context.WithValue(ctx, TimestampType, msg.TimestampType)
	ctx = context.WithValue(ctx, Headers, msg.Headers)
	return ctx
}
