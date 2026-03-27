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

type Topic struct {
	log  *commitlog.FileLog
	subs map[*Consumer]struct{}

	mu sync.RWMutex
}

type TopicName string

func (b *Broker) CreateTopic(name string) error {
	tName := TopicName(name)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[tName]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	logPath := filepath.Join(b.baseDir, name)

	logger.Debug("Creating topic",
		logrus.Fields{"topic": name, "path": logPath},
	)

	l, err := commitlog.Open(logPath)
	if err != nil {
		logger.Error("Failed to open commit log",
			logrus.Fields{"path": logPath, "err": err},
		)
		return err
	}

	b.topics[tName] = &Topic{
		log:  l,
		subs: make(map[*Consumer]struct{}),
	}

	logger.Info("Topic created",
		logrus.Fields{"topic": name, "path": logPath},
	)

	return nil
}

func (b *Broker) DeleteTopic(name string) error {
	tName := TopicName(name)

	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.topics[tName]
	if !ok {
		return fmt.Errorf("topic %s not found", name)
	}

	topicPath := filepath.Join(b.baseDir, name)
	logger.Debug("Deleting topic",
		logrus.Fields{"topic": name, "path": topicPath},
	)

	if err := t.log.Close(); err != nil {
		logger.Error("Failed to close topic log",
			logrus.Fields{"topic": name, "err": err},
		)
		return err
	}

	if err := os.RemoveAll(topicPath); err != nil {
		logger.Error("Failed to delete topic directory",
			logrus.Fields{"dir": topicPath, "err": err},
		)
		return err
	}

	delete(b.topics, tName)

	logger.Info("Topic deleted",
		logrus.Fields{"topic": name, "dir": topicPath},
	)
	return nil
}
