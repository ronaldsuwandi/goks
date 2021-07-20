package main

import (
	"github.com/ronaldsuwandi/goks"
	"log"
)

func main() {
	s := goks.NewStream("topic")

	s.Filter(func(msg string) bool {
		return msg == "abc"
	}).Map(func (msg string) string {
		return msg + "-mapped"
	}).Peek(func (msg string) {
		log.Printf("peek -> %v\n", msg)
	})

	t := goks.Topology{
		S: s,
	}
	goks.Start(t)
}
