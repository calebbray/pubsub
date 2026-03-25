package eventlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type EventLogger interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Truncate(size int64) error

	Sync() error
}

type Log struct {
	EventLogger
	size uint64
}

func NewFileLog(path string) (*Log, error) {
	l := &Log{}

	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	l.EventLogger = fd
	l.size = uint64(info.Size())

	return l, nil
}

func (l *Log) Append(data []byte) (offset uint64, err error) {
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(data)))
	copy(buf[4:], data)

	logStart := l.size

	if _, err := l.EventLogger.WriteAt(buf, int64(l.size)); err != nil {
		return 0, fmt.Errorf("error appending log to file: %w", err)
	}

	l.size += uint64(len(buf))

	return logStart, nil
}

var ErrInvalidOffset = errors.New("invalid offset")

func (l *Log) Read(offset uint64) ([]byte, error) {
	if offset >= l.size || offset+4 > l.size {
		return nil, fmt.Errorf("%w: %d", ErrInvalidOffset, offset)
	}

	logLen := make([]byte, 4)

	if _, err := l.EventLogger.ReadAt(logLen, int64(offset)); err != nil {
		return nil, fmt.Errorf("error reading from log file: %w", err)
	}

	n := binary.BigEndian.Uint32(logLen)

	if offset+4+uint64(n) > l.size {
		return nil, fmt.Errorf("%w: %d", ErrInvalidOffset, offset)
	}

	buf := make([]byte, n)
	if _, err := l.EventLogger.ReadAt(buf, int64(offset+4)); err != nil {
		return nil, fmt.Errorf("error reading to buf: %w", err)
	}

	return buf, nil
}

func (l *Log) ResetSize() {
	l.size = 0
}

func (l *Log) Sync() error {
	return l.EventLogger.Sync()
}

func (l *Log) Close() error {
	if l.EventLogger != nil {
		l.EventLogger.Close()
	}

	l.EventLogger = nil

	return nil
}
