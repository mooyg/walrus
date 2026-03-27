package broker

import (
	"fmt"
	"os"
	"sync"

	commitlog "github.com/mooyg/walrus/internal/commitlog"
	logger "github.com/mooyg/walrus/internal/log"
)

type Broker struct {
	mu      sync.RWMutex
	topics  map[TopicName]*Topic
	baseDir string
}

func (b *Broker) Init() error {
	// TODO: allow changing baseDir through arg for now keeping everything in data on-purpose
	var baseDir string = "data"
	b.baseDir = baseDir

	entries, err := os.ReadDir(baseDir)

	if err != nil {
		logger.Error("Failed to read baseDir")
	}

	for _, e := range entries {
		// TODO: add guard-rails for directories etc
		l, err := commitlog.Open(e.Name())
		if err != nil {
			logger.Error("Failed to read commitlog")
		}
		t := &Topic{
			log:  l,
			subs: make(map[*Consumer]struct{}),
		}
		b.topics[TopicName(e.Name())] = t
	}

	return nil
}

func NewBroker() (*Broker, error) {
	b := &Broker{
		baseDir: "data",
		topics:  make(map[TopicName]*Topic),
	}

	if err := b.Init(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Broker) Publish(topic string, data []byte) (int64, error) {
	b.mu.RLock()
	t, ok := b.topics[TopicName(topic)]
	b.mu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("topic %s not found", topic)
	}

	t.mu.Lock()

	defer t.mu.Unlock()
	offset, err := t.log.Append(data)

	if err != nil {
		return 0, err
	}

	for c := range t.subs {
		select {
		case c.ch <- ConsumerMessage{Offset: offset, Data: data}:
		default:
		}
	}

	return offset, nil
}
