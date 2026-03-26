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

	l.offset++

	return offset, nil
}

func (l *FileLog) Close() error {
	return l.file.Close()
}
