package main

import (
	"github.com/ronaldsuwandi/goks"
	"github.com/ronaldsuwandi/goks/serde"
	"log"
)

func main() {
	tb := goks.NewTopologyBuilder()

	//st := tb.Stream("input", serde.StringDeserializer{}).
	//	Filter(func(kvc goks.KeyValueContext) bool {
	//		return true
	//	})
	//st.
	//	//	//	//Map(func(kvc goks.KeyValueContext) goks.KeyValueContext {
	//	//	//	//	k := kvc.Key.(string)
	//	//	//	//	v := kvc.Value.(string) + "-mapped"
	//	//	//	//
	//	//	//	//	return goks.KeyValueContext{
	//	//	//	//		Key: k,
	//	//	//	//		ValueContext: goks.ValueContext{
	//	//	//	//			Value: v,
	//	//	//	//			Ctx:   kvc.Ctx,
	//	//	//	//		},
	//	//	//	//	}
	//	//	//	//}).
	//	MapValues(func(kvc goks.KeyValueContext) goks.ValueContext {
	//		return goks.ValueContext{
	//			Value: kvc.Value.(string) + "MAP1",
	//			Ctx:   kvc.Ctx, // TODO maybe get rid of ctx?
	//		}
	//	}).
	//	Peek(func(kvc goks.KeyValueContext) {
	//		log.Printf("!!!!k=%v\tv=%v\n", kvc.Key, kvc.Value)
	//	}).
	//	Table(true).
	//	MapValues(func(kvc goks.KeyValueContext) goks.ValueContext {
	//		log.Println("MAP FROM TABLE!")
	//		return goks.ValueContext{
	//			Value: kvc.Value.(string) + "WOAH TABLE",
	//			Ctx:   kvc.Ctx,
	//		}
	//	})
	//	To("output", serde.StringSerializer{})

	////st.
	////	//Map(func(kvc goks.KeyValueContext) goks.KeyValueContext {
	////	//	k := kvc.Key.(string)
	////	//	v := kvc.Value.(string) + "-mapped"
	////	//
	////	//	return goks.KeyValueContext{
	////	//		Key: k,
	////	//		ValueContext: goks.ValueContext{
	////	//			Value: v,
	////	//			Ctx:   kvc.Ctx,
	////	//		},
	////	//	}
	////	//}).
	////	MapValues(func(kvc goks.KeyValueContext) goks.ValueContext {
	////		return goks.ValueContext{
	////			Value: kvc.Value.(string) + "MAP2",
	////		}
	////	}).
	////	Peek(func(kvc goks.KeyValueContext) {
	////		log.Printf("!!!!k=%v\tv=%v\n", kvc.Key, kvc.Value)
	////	})
	////MapValues(func(kvc goks.KeyValueContext) goks.ValueContext {
	////	v := kvc.Value.(string) + "-MAPVAL"
	////	return goks.ValueContext{
	////		Value: v,
	////		Ctx:   kvc.Ctx,
	////	}
	////}).
	////Peek(func(kvc goks.KeyValueContext) {
	////	log.Printf("k=%v\tv=%v\n", kvc.Key, kvc.Value)
	////})
	//
	////tb.Stream("topic").
	////	Filter(func(msg string) bool {
	////		return msg == "abc"
	////	}).
	////	Map(func(msg string) string {
	////		return msg + "-mapped"
	////	}).
	////	Peek(func(msg string) {
	////		log.Printf("peek -> %v\n", msg)
	////	})
	//
	////branches := tb.Stream("topic").Branch(
	////	func(msg string) bool {
	////		return true
	////	}, func(msg string) bool {
	////		return true
	////	})
	////
	////for i := range branches {
	////	k := i
	////	branches[i].Peek(func(msg string) {
	////		log.Printf("peek [%d] -> %v\n", k, msg)
	////	})
	////}

	table := tb.Table("input", serde.StringDeserializer{})
	table.
		MapValues(func(kvc goks.KeyValueContext) goks.ValueContext {
			log.Println("KVC FROM TABLE" + kvc.Key.(string) + ": " + kvc.Value.(string))
			return goks.ValueContext{
				Value: kvc.Value.(string) + "MAP-FROM-TABLE",
				Ctx:   kvc.Ctx, // must include
			}
		}).
		Stream().
		Peek(func(kvc goks.KeyValueContext) {
			log.Println("PEEK FROM STREAM! " + kvc.Key.(string) + ": " + kvc.Value.(string))
		})

	topology := tb.Build()
	g, err := goks.New(topology)
	if err != nil {
		panic(err)
	}
	log.Println("starting")
	log.Fatal(g.Start())
}
