package eventlog

import (
	"fmt"
	"os"
	"path"
	"strings"
)

type Segment struct {
	log        *Log
	baseOffset uint64
	size       uint64
}

type SegmentedLog struct {
	segments       []*Segment
	maxSegmentSize uint64
	newLogger      func(baseOffset uint64) (EventLogger, error)
}

func NewSegmentedLog(dir string, maxSegmentSize uint64) (*SegmentedLog, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	logs := []os.DirEntry{}
	for _, entry := range files {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".log") {
			logs = append(logs, entry)
		}
	}

	newLogger := func(baseOffset uint64) (EventLogger, error) {
		nextFile := fmt.Sprintf("%020d.log", baseOffset)
		p := path.Join(dir, nextFile)
		return os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
	}

	segments := make([]*Segment, len(logs))
	var offset uint64
	for i, log := range logs {
		l, err := NewFileLog(path.Join(dir, log.Name()))
		if err != nil {
			return nil, err
		}
		info, err := log.Info()
		if err != nil {
			return nil, err
		}

		segments[i] = &Segment{
			log:        l,
			baseOffset: offset,
			size:       uint64(info.Size()),
		}

		offset += uint64(info.Size())
	}

	l := &SegmentedLog{
		maxSegmentSize: maxSegmentSize,
		newLogger:      newLogger,
		segments:       segments,
	}

	if len(segments) == 0 {
		l.rotate(0)
	}

	return l, nil
}

func (l *SegmentedLog) Append(data []byte) (uint64, error) {
	segment := l.segments[len(l.segments)-1]
	remaining := l.maxSegmentSize - segment.size
	dataSize := uint64(len(data) + 4)

	if dataSize > remaining {
		nextBaseOffset := segment.baseOffset + segment.size
		if err := l.rotate(nextBaseOffset); err != nil {
			return 0, err
		}
	}
	active := l.segments[len(l.segments)-1]
	o, err := active.log.Append(data)
	if err != nil {
		return 0, err
	}

	active.size += dataSize
	return active.baseOffset + o, nil
}

func (l *SegmentedLog) Read(offset uint64) ([]byte, error) {
	var s *Segment
	for _, segment := range l.segments {
		if offset >= segment.baseOffset && offset < segment.baseOffset+segment.size {
			s = segment
			break
		}
	}
	if s == nil {
		return nil, ErrInvalidOffset
	}

	return s.log.Read(offset - s.baseOffset)
}

func (l *SegmentedLog) NewIterator(startOffset uint64) *Iterator {
	return NewIterator(l, startOffset)
}

func (l *SegmentedLog) rotate(baseOffset uint64) error {
	logger, err := l.newLogger(baseOffset)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, &Segment{
		log:        &Log{EventLogger: logger},
		baseOffset: baseOffset,
	})

	return nil
}
