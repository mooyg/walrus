package broker

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	commitlog "github.com/mooyg/walrus/internal/commitlog"
)

const consumerOffsetTopic = "__consumer_offsets"

type ConsumerID string

type offsetKey struct {
	consumerID ConsumerID
	topic      TopicName
}

type OffsetRecord struct {
	ConsumerID ConsumerID `json:"consumer_id"`
	Topic      TopicName  `json:"topic"`
	Offset     int64      `json:"offset"`
}

func (b *Broker) ensureConsumersOffsetTopic() error {
	tName := TopicName(consumerOffsetTopic)

	if _, exists := b.topics[tName]; exists {
		return nil
	}

	logPath := filepath.Join(b.baseDir, consumerOffsetTopic)
	l, err := commitlog.Open(logPath)

	if err != nil {
		return err
	}

	b.topics[tName] = &Topic{log: l}

	return nil
}

func (b *Broker) replayCommittedOffsets() error {
	tName := TopicName(consumerOffsetTopic)
	t := b.topics[tName]

	head := t.log.HeadOffset()
	if head == 0 {
		return nil
	}

	msgs, err := t.log.ReadFrom(0, int(head))
	if err != nil {
		return fmt.Errorf("replaying committed offsets: %w", err)
	}

	for _, m := range msgs {
		var rec OffsetRecord
		if err := json.Unmarshal(m.Data, &rec); err != nil {
			return fmt.Errorf("replaying committed offsets: %w", err)
		}
		b.commitedOffsets[offsetKey{rec.ConsumerID, rec.Topic}] = rec
	}

	return nil
}

func (b *Broker) CommitOffset(consumerId, topic string, offset int64) error {
	rec := OffsetRecord{
		ConsumerID: ConsumerID(consumerId),
		Topic:      TopicName(topic),
		Offset:     offset,
	}

	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}

	if _, err := b.Produce(consumerOffsetTopic, data); err != nil {
		return fmt.Errorf("committing offset: %w", err)
	}

	b.mu.Lock()
	b.commitedOffsets[offsetKey{ConsumerID(consumerId), TopicName(topic)}] = rec
	b.mu.Unlock()

	return nil
}

func (b *Broker) GetOffset(consumerId, topic string) (int64, error) {
	b.mu.RLock()
	rec, ok := b.commitedOffsets[offsetKey{ConsumerID(consumerId), TopicName(topic)}]
	b.mu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("no committed offset for consumer %s on topic %s", consumerId, topic)
	}

	return rec.Offset, nil
}
