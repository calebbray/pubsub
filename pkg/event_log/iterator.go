package eventlog

import "errors"

type Iterator struct {
	log     *Log
	currOff uint64
	currLog []byte
	err     error
}

func NewIterator(log *Log, startOffset uint64) *Iterator {
	return &Iterator{
		log:     log,
		currOff: startOffset,
	}
}

func (i *Iterator) Next() bool {
	data, err := i.log.Read(i.Offset())
	if err != nil {
		if errors.Is(err, ErrInvalidOffset) {
			return false
		}
		i.err = err
		return false
	}
	i.currLog = data
	i.currOff += uint64(len(data) + 4)
	return true
}

func (i *Iterator) Data() []byte {
	return i.currLog
}

func (i *Iterator) Offset() uint64 {
	return i.currOff
}

func (i *Iterator) Err() error {
	return i.err
}
