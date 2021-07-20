package goks

type Topology struct {
	S Stream
}

func (t Topology) Describe() string {
	return "printed version of topology here"
}

func (t Topology) pipe(msg string) {
	t.S.process(msg)
}