package serde

type StringSerializer struct{}

func (s StringSerializer) Serialize(data interface{}) []byte {
	str := data.(string)
	return []byte(str)
}

type StringDeserializer struct{}

func (s StringDeserializer) Deserialize(data []byte) interface{} {
	return string(data)
}
