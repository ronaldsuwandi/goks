package goks

import "log"

func Start(t Topology) {

	// listen to message
	log.Println("Start!")
	for _, msg := range []string{"abc", "def", "abc"} {
		t.pipe(msg)
	}
	log.Println("Done!")
}
