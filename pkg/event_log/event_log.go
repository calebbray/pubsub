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

	Sync() error
}

type Log struct {
	EventLogger
	size uint64
}

func NewFileLog(path string) (*Log, error) {
	l := &Log{}

	var fp *os.File
	var err error
	info, err := os.Stat(path)
	if err != nil {
		fp, err = os.Create(path)
		l.size = 0
	} else {
		fp, err = os.OpenFile(path, os.O_RDWR, 0o644)
		l.size = uint64(info.Size())
	}

	if err != nil {
		return nil, fmt.Errorf("error opening log file: %w", err)
	}

	l.EventLogger = fp
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
