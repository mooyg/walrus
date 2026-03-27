package commitlog

import (
	"encoding/binary"
	"io"
)

type WriteResponse struct {
	offset int64
	err    error
}

type WriteRequest struct {
	data []byte
	resp chan<- WriteResponse
}

func (l *FileLog) handleWrite(req WriteRequest) {
	offset := l.offset
	pos, err := l.file.Seek(0, io.SeekEnd)

	if err != nil {
		req.resp <- WriteResponse{0, err}
		return
	}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(req.data)))

	_, err = l.file.Write(lenBuf[:])

	if err == nil {
		_, err = l.file.Write(req.data)
	}

	if err == nil {
		err = l.file.Sync()
	}

	if err == nil {
		l.positions[offset] = pos
		l.offset++
	}
	req.resp <- WriteResponse{offset, err}
}

func (l *FileLog) Append(data []byte) (int64, error) {
	resp := make(chan WriteResponse, 1)

	l.writeChannel <- WriteRequest{
		data: data,
		resp: resp,
	}

	result := <-resp

	return result.offset, result.err
}
