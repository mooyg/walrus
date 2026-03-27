package broker

type ConsumerMessage struct {
	Offset int64
	Data   []byte
}

type Consumer struct {
	// since a consumer can subscribe to many topics
	offset map[TopicName]int64
	broker *Broker

	ch chan ConsumerMessage
}

func (b *Broker) Subscribe(name string) (*Consumer, error) {
	return nil, nil
}
