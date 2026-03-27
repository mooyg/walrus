package broker

import (
	"context"
	"fmt"
	"time"
)

type ConsumerMessage struct {
	Offset int64
	Data   []byte
}

// Consume fetches up to max messages from the topic starting at offset. Blocks
// until at least one message is available or ctx is cancelled. Returns the
// messages and the next offset to pass on the following call.
func (b *Broker) Consume(ctx context.Context, topic string, offset int64, max int) ([]ConsumerMessage, int64, error) {
	tName := TopicName(topic)

	for {
		select {
		case <-ctx.Done():
			return nil, offset, ctx.Err()
		default:
		}

		b.mu.RLock()
		t, ok := b.topics[tName]
		b.mu.RUnlock()

		if !ok {
			return nil, offset, fmt.Errorf("topic %s not found", topic)
		}

		msgs, err := t.log.ReadFrom(offset, max)
		if err != nil {
			return nil, offset, err
		}

		if len(msgs) > 0 {
			out := make([]ConsumerMessage, len(msgs))
			for i, m := range msgs {
				out[i] = ConsumerMessage{Offset: m.Offset, Data: m.Data}
			}
			return out, msgs[len(msgs)-1].Offset + 1, nil
		}

		select {
		case <-ctx.Done():
			return nil, offset, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
