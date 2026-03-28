package broker

import (
	"os"
	"path/filepath"

	"errors"
	commitlog "github.com/mooyg/walrus/internal/commitlog"
	logger "github.com/mooyg/walrus/internal/log"
	"github.com/sirupsen/logrus"
)

type Topic struct {
	log *commitlog.FileLog
}

type TopicName string

// CreateTopic creates a new topic backed by a commit log on disk.
// Returns an error if the topic already exists.
func (b *Broker) CreateTopic(name string) error {
	if name == consumerOffsetTopic {
		return errors.New("topic " + name + " is internal and cannot be created")
	}

	tName := TopicName(name)

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[tName]; exists {
		return errors.New("topic " + name + " already exists")
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

	b.topics[tName] = &Topic{log: l}

	logger.Info("Topic created",
		logrus.Fields{"topic": name, "path": logPath},
	)

	return nil
}

// DeleteTopic closes the topic's commit log and removes all associated data from disk.
func (b *Broker) DeleteTopic(name string) error {
	if name == consumerOffsetTopic {
		return errors.New("topic " + name + " is internal and cannot be deleted")
	}

	tName := TopicName(name)

	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.topics[tName]
	if !ok {
		return errors.New("topic " + name + " not found")
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
