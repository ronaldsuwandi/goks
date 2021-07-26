package main

import (
	"github.com/ronaldsuwandi/goks"
	"github.com/ronaldsuwandi/goks/serde"
	"log"
)

func main() {
	sb := goks.NewStreamBuilder()
	sb.Stream("input", serde.StringDeserializer{}).
		Filter(func(kvc goks.KeyValueContext) bool {
			return true
		}).
		Map(func(kvc goks.KeyValueContext) goks.KeyValueContext {
			k := kvc.Key.(string)
			v := kvc.Value.(string) + "-mapped"

			return goks.KeyValueContext{
				Key: k,
				ValueContext: goks.ValueContext{
					Value: v,
					Ctx:   kvc.Ctx,
				},
			}
		}).
		Peek(func(kvc goks.KeyValueContext) {
			log.Printf("!!!!k=%v\tv=%v\n", kvc.Key, kvc.Value)
		}).
		MapValues(func(kvc goks.KeyValueContext) goks.ValueContext {
			v := kvc.Value.(string) + "-MAPVAL"
			return goks.ValueContext{
				Value: v,
				Ctx:   kvc.Ctx,
			}
		}).
		Peek(func(kvc goks.KeyValueContext) {
			log.Printf("k=%v\tv=%v\n", kvc.Key, kvc.Value)
		})

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

	//branches := sb.Stream("topic").Branch(
	//	func(msg string) bool {
	//		return true
	//	}, func(msg string) bool {
	//		return true
	//	})
	//
	//for i := range branches {
	//	k := i
	//	branches[i].Peek(func(msg string) {
	//		log.Printf("peek [%d] -> %v\n", k, msg)
	//	})
	//}

	t := sb.Build()
	g, err := goks.New(t)
	if err != nil {
		panic(err)
	}
	log.Fatal(g.Start())
}
