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

// Init scans the base directory and reopens commit logs for any topics that
// already exist on disk, restoring broker state across restarts.
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
		b.topics[TopicName(e.Name())] = &Topic{log: l}
	}

	return nil
}

// NewBroker creates a Broker and restores any topics persisted from a previous run.
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

// Produce appends data to the named topic's commit log and returns the assigned offset.
func (b *Broker) Produce(topic string, data []byte) (int64, error) {
	b.mu.RLock()
	t, ok := b.topics[TopicName(topic)]
	b.mu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("topic %s not found", topic)
	}

	return t.log.Append(data)
}
