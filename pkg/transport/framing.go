package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const MaxFrameSize int = 32 * 1024 * 1024

var ErrMaxPayloadExceeded = errors.New("payload exceeded max size")

type Framer struct {
	maxSize uint32
}

func NewFramer(maxSize uint32) *Framer {
	return &Framer{maxSize: maxSize}
}

func DefaultFramer() *Framer {
	return &Framer{maxSize: uint32(MaxFrameSize)}
}

func (f *Framer) WriteFrame(w io.Writer, payload []byte) error {
	if uint32(len(payload)) > f.maxSize {
		return ErrMaxPayloadExceeded
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
		return fmt.Errorf("error writing to frame: %w", err)
	}

	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("error writing to frame: %w", err)
	}

	return nil
}

func (f *Framer) ReadFrame(r io.Reader) ([]byte, error) {
	var frameLen uint32
	if err := binary.Read(r, binary.BigEndian, &frameLen); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("error reading frame: %w", err)
	}

	if frameLen > uint32(f.maxSize) {
		return nil, ErrMaxPayloadExceeded
	}

	buf := make([]byte, frameLen)
	_, err := io.ReadFull(r, buf)

	if err == io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("connection closed mid-frame: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("error reading to data buf: %w", err)
	}

	return buf, nil
}

var defaultFramer = DefaultFramer()

func WriteFrame(w io.Writer, payload []byte) error {
	return defaultFramer.WriteFrame(w, payload)
}

func ReadFrame(r io.Reader) ([]byte, error) {
	return defaultFramer.ReadFrame(r)
}
