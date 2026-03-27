package broker

import "fmt"

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

func (b *Broker) Subscribe(topic string) (*Consumer, error) {
	b.mu.RLock()
	t, ok := b.topics[TopicName(topic)]
	b.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	c := &Consumer{
		ch:     make(chan ConsumerMessage, 256),
		offset: make(map[TopicName]int64),
	}

	t.mu.Lock()
	t.subs[c] = struct{}{}
	t.mu.Unlock()

	return c, nil
}
