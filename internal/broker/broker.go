package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	commitlog "github.com/mooyg/walrus/internal/commitlog"
	logger "github.com/mooyg/walrus/internal/log"
	"github.com/sirupsen/logrus"
)

type Broker struct {
	mu              sync.RWMutex
	topics          map[TopicName]*Topic
	commitedOffsets map[ConsumerID]OffsetRecord
	baseDir         string
}

// Init scans the base directory and reopens commit logs for any topics that
// already exist on disk, restoring broker state across restarts.
func (b *Broker) Init() error {
	if err := os.MkdirAll(b.baseDir, 0755); err != nil {
		logger.Error("Failed to create baseDir", logrus.Fields{"dir": b.baseDir, "err": err})
		return err
	}

	entries, err := os.ReadDir(b.baseDir)
	if err != nil {
		logger.Error("Failed to read baseDir", logrus.Fields{"dir": b.baseDir, "err": err})
		return err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		logPath := filepath.Join(b.baseDir, e.Name())
		l, err := commitlog.Open(logPath)
		if err != nil {
			logger.Error("Failed to open commit log", logrus.Fields{"path": logPath, "err": err})
			return err
		}
		b.topics[TopicName(e.Name())] = &Topic{log: l}
	}

	if err := b.ensureConsumersOffsetTopic(); err != nil {
		return err
	}

	return b.replayCommittedOffsets()
}

// NewBroker creates a Broker rooted at baseDir and restores any topics persisted from a previous run.
func NewBroker(baseDir string) (*Broker, error) {
	b := &Broker{
		baseDir:         baseDir,
		topics:          make(map[TopicName]*Topic),
		commitedOffsets: make(map[ConsumerID]OffsetRecord),
	}

	if err := b.Init(); err != nil {
		return nil, err
	}

	return b, nil
}

// Close closes all topic logs. Should be called on shutdown.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var firstErr error
	for name, t := range b.topics {
		if err := t.log.Close(); err != nil && firstErr == nil {
			logger.Error("Failed to close topic log", logrus.Fields{"topic": name, "err": err})
			firstErr = err
		}
	}
	return firstErr
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
