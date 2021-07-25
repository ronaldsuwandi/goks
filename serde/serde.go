package serde

type Serializer interface {
	Serialize(data interface{}) []byte // TODO change interface with generic
}

type Deserializer interface {
	Deserialize(data []byte) interface{} // TODO change interface with generic
}
