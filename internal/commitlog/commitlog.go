package commitlog

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	logger "github.com/mooyg/walrus/internal/log"
	"github.com/sirupsen/logrus"
)

type Message struct {
	Offset int64
	Data   []byte
}

type Log interface {
	Append(data []byte) (int64, error)
	ReadFrom(offset int64, max int) ([]Message, error)
	Close() error
}

type FileLog struct {
	file   *os.File
	offset atomic.Int64
	// store every message position
	positions map[int64]int64

	writeChannel chan WriteRequest
	readChannel  chan ReadRequest
	done         chan struct{}
}

func rebuildIndex(f *os.File) (positions map[int64]int64, nextOffset int64, err error) {
	logger.Debug("Rebuilding offset from file start")

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	fileSize := fileInfo.Size()

	positions = make(map[int64]int64)
	var pos int64

	for {
		var lenBuf [4]byte
		_, err := f.ReadAt(lenBuf[:], pos)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			logger.Debug("Reached end or partial entry", logrus.Fields{"pos": pos})
			break
		}
		if err != nil {
			logger.Error("Failed to read entry size", logrus.Fields{"pos": pos, "err": err})
			break
		}

		entrySize := binary.BigEndian.Uint32(lenBuf[:])

		if entrySize == 0 {
			logger.Warn("Encountered zero-length entry", logrus.Fields{"pos": pos})
			break
		}

		dataEnd := pos + 4 + int64(entrySize)

		if dataEnd > fileSize {
			logger.Warn("Detected corrupted entry (overflow)", logrus.Fields{
				"pos": pos, "entrySize": entrySize, "fileSize": fileSize,
			})
			break
		}

		logger.Debug("Valid entry found", logrus.Fields{
			"pos": pos, "entrySize": entrySize, "nextPos": dataEnd,
		})

		positions[nextOffset] = pos
		pos = dataEnd
		nextOffset++
	}

	return positions, nextOffset, nil
}

func Open(path string) (*FileLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	positions, nextOffset, err := rebuildIndex(f)
	if err != nil {
		return nil, err
	}

	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		return nil, err
	}

	l := &FileLog{
		file:         f,
		positions:    positions,
		writeChannel: make(chan WriteRequest),
		readChannel:  make(chan ReadRequest),
		done:         make(chan struct{}),
	}
	l.offset.Store(nextOffset)

	go l.loop()

	return l, nil
}

func (l *FileLog) HeadOffset() int64 {
	return l.offset.Load()
}

func (l *FileLog) loop() {
	for {
		select {
		case req, ok := <-l.writeChannel:
			if !ok {
				return
			}
			l.handleWrite(req)
		case req, ok := <-l.readChannel:
			if !ok {
				return
			}
			l.handleRead(req)
		case <-l.done:
			return
		}
	}
}

func (l *FileLog) Close() error {
	close(l.done)
	return l.file.Close()
}
