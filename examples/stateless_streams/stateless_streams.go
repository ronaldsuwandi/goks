package main

func main() {
	//sigchan := make(chan os.Signal, 1)
	//signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	//
	//consumerConf := &kafka.ConfigMap{
	//	"bootstrap.servers":   "localhost:9092",
	//	"group.id":           "group",
	//	"auto.offset.reset":  "earliest",
	//	"enable.auto.commit": false,
	//}
	//
	//c, err := kafka.NewConsumer(consumerConf)
	//defer c.Close()
	//if err != nil {
	//	panic(err)
	//}
	//
	//err = c.SubscribeTopics([]string{"input"}, nil)
	//
	//run := true
	//for run {
	//	select {
	//	case sig := <-sigchan:
	//		fmt.Printf("Terminating: %v\n", sig)
	//		run = false
	//	default:
	//		ev := c.Poll(100)
	//		if ev == nil {
	//			continue
	//		}
	//		switch e := ev.(type) {
	//		case *kafka.Message:
	//			fmt.Printf("%% Message on %s:\n%s\n",
	//				e.TopicPartition, string(e.Value))
	//			if e.Headers != nil {
	//				fmt.Printf("%% Headers: %v\n", e.Headers)
	//			}
	//		case kafka.Error:
	//			// Errors should generally be considered
	//			// informational, the client will try to
	//			// automatically recover.
	//			// But in this example we choose to terminate
	//			// the application if all brokers are down.
	//			fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
	//			if e.Code() == kafka.ErrAllBrokersDown {
	//				run = false
	//			}
	//		default:
	//			fmt.Printf("Ignored %v\n", e)
	//		}
	//	}
	//}

}
