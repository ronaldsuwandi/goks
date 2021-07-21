package goks

type Topology struct {
	input []Stream
}

func (t Topology) Describe() string {
	return "printed version of topology here"
}

func (t Topology) pipe(msg string) {
	for i := range t.input {
		t.input[i].process(msg)
	}
}
