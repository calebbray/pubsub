package transport

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadWriteFrame(t *testing.T) {
	var buf Buffer
	data := []byte("my super special data")

	require.NoError(t, WriteFrame(&buf, data))
	read, err := ReadFrame(&buf)
	require.NoError(t, err)

	assert.Equal(t, string(data), string(read))
}

func TestWritingMultipleFrames(t *testing.T) {
	var buf Buffer
	frame1 := []byte("frame1")
	frame2 := []byte("frame2")
	frame3 := []byte("frame3")

	require.NoError(t, WriteFrame(&buf, frame1))
	require.NoError(t, WriteFrame(&buf, frame2))
	require.NoError(t, WriteFrame(&buf, frame3))

	var err error
	var data []byte
	i := 0
	for {
		data, err = ReadFrame(&buf)
		if err != nil {
			break
		}

		assert.Equal(t, fmt.Sprintf("frame%d", i+1), string(data))
		i++
	}

	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 3, i)
}

func TestChunkedReading(t *testing.T) {
	buf := &Buffer{chunkSize: 1}
	data := []byte("my super special data")
	require.NoError(t, WriteFrame(buf, data))

	frame, err := ReadFrame(buf)
	require.NoError(t, err)
	assert.Equal(t, string(data), string(frame))
}

func TestReadEmptyBuf(t *testing.T) {
	_, err := ReadFrame(&Buffer{})
	require.Error(t, err)
	assert.Equal(t, io.EOF, err)
}

func TestZeroLenPayload(t *testing.T) {
	buf := &Buffer{}
	require.NoError(t, WriteFrame(buf, []byte{}))

	frame, err := ReadFrame(buf)
	require.NoError(t, err)

	assert.Equal(t, 0, len(frame))
}

type Buffer struct {
	data      []byte
	pos       int
	chunkSize int
}

func (b *Buffer) Write(p []byte) (int, error) {
	b.data = append(b.data, p...)
	return len(p), nil
}

func (b *Buffer) Read(p []byte) (int, error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}

	bytesToRead := len(b.data) - b.pos

	if b.chunkSize > 0 {
		bytesToRead = b.chunkSize
	}

	if bytesToRead > len(p) {
		bytesToRead = len(p)
	}

	n := copy(p, b.data[b.pos:b.pos+bytesToRead])
	b.pos += n

	return n, nil
}
