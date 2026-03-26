package commitlog

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"

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
	offset int64
	// store every message position
	positions map[int64]int64
}

func Open(path string) (*FileLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		return nil, err
	}

	l := &FileLog{
		file:   f,
		offset: 0,
	}

	logger.Debug("Rebuilding offset from file start")

	var pos int64 = 0

	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()

	l.positions = make(map[int64]int64)
	for {
		// buffer holding length bytes
		// [message-length [4bytes] ][ message ]
		var lenBuf [4]byte
		_, err := f.ReadAt(lenBuf[:], pos)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			logger.Debug("Reached end or partial entry", logrus.Fields{
				"pos": pos,
			})
			break
		}
		if err != nil {
			logger.Error("Failed to read entry size", logrus.Fields{
				"pos": pos,
				"err": err,
			})
			break
		}

		entrySize := binary.BigEndian.Uint32(lenBuf[:])

		if entrySize == 0 {
			logger.Warn("Encountered zero-length entry", logrus.Fields{
				"pos": pos,
			})
			break
		}

		dataEnd := pos + 4 + int64(entrySize)

		if dataEnd > fileSize {
			logger.Warn("Detected corrupted entry (overflow)", logrus.Fields{
				"pos":       pos,
				"entrySize": entrySize,
				"fileSize":  fileSize,
			})
			break
		}

		logger.Debug("Valid entry found", logrus.Fields{
			"pos":       pos,
			"entrySize": entrySize,
			"nextPos":   dataEnd,
		})

		l.positions[l.offset] = pos
		pos = dataEnd
		l.offset++
	}

	_, err = f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (l *FileLog) Append(data []byte) (int64, error) {
	offset := l.offset

	pos, err := l.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	l.positions[l.offset] = pos

	logger.Debug("Appending entry", logrus.Fields{
		"offset": offset,
		"pos":    pos,
		"size":   len(data),
	})

	var lenBuf [4]byte

	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	// write lenBuf
	if _, err := l.file.Write(lenBuf[:]); err != nil {
		return 0, err
	}

	if _, err := l.file.Write(data); err != nil {
		return 0, err
	}

	if err := l.file.Sync(); err != nil {
		return 0, err
	}

	l.offset++

	return offset, nil
}

func (l *FileLog) ReadFrom(offset int64, max int) ([]Message, error) {
	if offset < 0 || offset >= l.offset {
		return nil, nil
	}

	fileInfo, err := l.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	maxOffset := offset + int64(max)
	if maxOffset > l.offset {
		maxOffset = l.offset
	}

	messages := make([]Message, 0, maxOffset-offset)

	for i := offset; i < maxOffset; i++ {
		pos := l.positions[i]

		var lenBuf [4]byte
		if _, err := l.file.ReadAt(lenBuf[:], pos); err != nil {
			logger.Debug("Failed to read entry size", logrus.Fields{
				"offset": i,
				"pos":    pos,
				"err":    err,
			})
			return messages, nil
		}

		entrySize := binary.BigEndian.Uint32(lenBuf[:])
		dataStart := pos + 4

		if dataStart+int64(entrySize) > fileSize {
			logger.Debug("Corrupted entry detected", logrus.Fields{
				"offset":    i,
				"entrySize": entrySize,
				"fileSize":  fileSize,
			})
			return messages, nil
		}

		data := make([]byte, entrySize)
		if _, err := l.file.ReadAt(data, dataStart); err != nil {
			logger.Debug("Failed to read entry data", logrus.Fields{
				"offset": i,
				"pos":    dataStart,
				"err":    err,
			})
			return messages, nil
		}

		messages = append(messages, Message{Offset: i, Data: data})
	}

	return messages, nil
}

func (l *FileLog) Close() error {
	return l.file.Close()
}
