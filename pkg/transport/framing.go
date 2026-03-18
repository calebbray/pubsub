package transport

import (
	"encoding/binary"
	"fmt"
	"io"
)

func WriteFrame(w io.Writer, payload []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
		return fmt.Errorf("error writing to frame: %w", err)
	}

	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("error writing to frame: %w", err)
	}

	return nil
}

func ReadFrame(r io.Reader) ([]byte, error) {
	var frameLen uint32
	if err := binary.Read(r, binary.BigEndian, &frameLen); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("error reading frame: %w", err)
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
