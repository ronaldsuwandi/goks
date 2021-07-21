package main

import (
	"github.com/ronaldsuwandi/goks"
	"log"
)

func main() {

	sb := goks.StreamBuilder{}

	//sb.Stream("topic").
	//	Filter(func(msg string) bool {
	//		return msg == "abc"
	//	}).
	//	Map(func(msg string) string {
	//		return msg + "-mapped"
	//	}).
	//	Peek(func(msg string) {
	//		log.Printf("peek -> %v\n", msg)
	//	})

	branches := sb.Stream("topic").Branch(
		func(msg string) bool {
			return true
		}, func(msg string) bool {
			return true
		})

	for i := range branches {
		k := i
		branches[i].Peek(func(msg string) {
			log.Printf("peek [%d] -> %v\n", k, msg)
		})
	}

	t := sb.Build()
	goks.Start(t)
}
