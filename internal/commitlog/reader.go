package commitlog

import (
	"encoding/binary"
	"io"

	logger "github.com/mooyg/walrus/internal/log"
	"github.com/sirupsen/logrus"
)

type ReadResponse struct {
	messages []Message
	err      error
}

type ReadRequest struct {
	offset int64
	max    int

	resp chan<- ReadResponse
}

func (l *FileLog) handleRead(req ReadRequest) {
	offset := req.offset

	if offset < 0 || offset >= l.offset {
		req.resp <- ReadResponse{messages: nil, err: nil}
		return
	}

	fileInfo, err := l.file.Stat()

	if err != nil {
		req.resp <- ReadResponse{messages: nil, err: err}
		return
	}

	fileSize := fileInfo.Size()

	maxOffset := min(offset+int64(req.max), l.offset)

	messages := make([]Message, 0, maxOffset-offset)

	var readErr error
	for i := offset; i < maxOffset; i++ {
		pos := l.positions[i]

		var lenBuf [4]byte
		if _, err := l.file.ReadAt(lenBuf[:], pos); err != nil {
			logger.Debug("Failed to read entry size", logrus.Fields{
				"offset": i,
				"pos":    pos,
				"err":    err,
			})
			messages = nil
			readErr = err
			break
		}

		entrySize := binary.BigEndian.Uint32(lenBuf[:])
		dataStart := pos + 4

		if dataStart+int64(entrySize) > fileSize {
			logger.Debug("Corrupted entry detected", logrus.Fields{
				"offset":    i,
				"entrySize": entrySize,
				"fileSize":  fileSize,
			})
			messages = nil
			readErr = io.ErrUnexpectedEOF
			break
		}

		data := make([]byte, entrySize)
		if _, err := l.file.ReadAt(data, dataStart); err != nil {
			logger.Debug("Failed to read entry data", logrus.Fields{
				"offset": i,
				"pos":    dataStart,
				"err":    err,
			})
			messages = nil
			readErr = err
			break
		}
		messages = append(messages, Message{Offset: i, Data: data})
	}

	req.resp <- ReadResponse{messages: messages, err: readErr}
}

func (l *FileLog) ReadFrom(offset int64, max int) ([]Message, error) {
	resp := make(chan ReadResponse, 1)

	l.readChannel <- ReadRequest{
		offset: offset,
		max:    max,
		resp:   resp,
	}

	result := <-resp

	return result.messages, result.err
}
