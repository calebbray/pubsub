package transport

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const MaxFrameSize int = 32 * 1024 * 1024

var ErrMaxPayloadExceeded = errors.New("payload exceeded max size")

type Framer struct {
	maxSize         uint32
	DisableChecksum bool
}

var (
	ErrChecksumMismatch = errors.New("checksum mismatch")
	crcTable            = crc32.MakeTable(crc32.IEEE)
)

func NewFramer(maxSize uint32) *Framer {
	return &Framer{maxSize: maxSize}
}

// Checksums are ignored for backwards compatibility
func DefaultFramer() *Framer {
	return &Framer{maxSize: uint32(MaxFrameSize), DisableChecksum: true}
}

func (f *Framer) WriteFrame(w io.Writer, payload []byte) error {
	if uint32(len(payload)) > f.maxSize {
		return ErrMaxPayloadExceeded
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
		return fmt.Errorf("error writing to frame: %w", err)
	}

	if !f.DisableChecksum {
		checksum := crc32.Checksum(payload, crcTable)
		if err := binary.Write(w, binary.BigEndian, checksum); err != nil {
			return fmt.Errorf("error writing to frame: %w", err)
		}
	}

	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("error writing to frame: %w", err)
	}

	return nil
}

func (f *Framer) ReadFrame(r io.Reader) ([]byte, error) {
	length, checksum, err := f.readHeader(r)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(r, buf)

	if err == io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("connection closed mid-frame: %w", err)
	}

	if err != nil {
		return nil, fmt.Errorf("error reading to data buf: %w", err)
	}

	if !f.DisableChecksum {
		got := crc32.Checksum(buf, crcTable)
		if got != checksum {
			return nil, fmt.Errorf("%w: got %08x want %08x", ErrChecksumMismatch, got, checksum)
		}
	}

	return buf, nil
}

func (f *Framer) readHeader(r io.Reader) (length, checksum uint32, err error) {
	headerSize := 8
	if f.DisableChecksum {
		headerSize = 4
	}
	header := make([]byte, headerSize)
	n, err := io.ReadFull(r, header)
	if err != nil {
		if err == io.EOF {
			return 0, 0, err
		}
		return 0, 0, fmt.Errorf("error reading frame header: %w", err)
	}

	length = binary.BigEndian.Uint32(header[0:4])
	if n == 8 {
		checksum = binary.BigEndian.Uint32(header[4:8])
	}
	return length, checksum, nil
}

var defaultFramer = DefaultFramer()

func WriteFrame(w io.Writer, payload []byte) error {
	return defaultFramer.WriteFrame(w, payload)
}

func ReadFrame(r io.Reader) ([]byte, error) {
	return defaultFramer.ReadFrame(r)
}
