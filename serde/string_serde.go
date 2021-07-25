package serde

type StringSerializer struct{}

func (s StringSerializer) Serialize(data interface{}) []byte {
	return data.([]byte)
}

type StringDeserializer struct{}

func (s StringDeserializer) Deserialize(data []byte) interface{} {
	return string(data)
}
