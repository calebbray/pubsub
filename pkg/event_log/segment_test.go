package eventlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendWithSingleSegment(t *testing.T) {
	msg := []byte("hello there")
	logger := newTestSegmentedLog(t, 4096)
	// require.Nil(t, logger)
	offset, err := logger.Append(msg)
	require.NoError(t, err)

	data, err := logger.Read(offset)
	require.NoError(t, err)
	assert.Equal(t, msg, data)
}

func TestAppendPastSegmentBoundary(t *testing.T) {
	msg := []byte("hello there")
	logLen := 4 + len(msg)
	logger := newTestSegmentedLog(t, logLen*2)

	_, err := logger.Append(msg)
	require.NoError(t, err)
	_, err = logger.Append(msg)
	require.NoError(t, err)
	_, err = logger.Append(msg)
	require.NoError(t, err)

	assert.Equal(t, 2, len(logger.segments))
}

func TestReadOldSegmentAfterRotate(t *testing.T) {
	msg := []byte("hello there")
	logLen := 4 + len(msg)
	logger := newTestSegmentedLog(t, logLen*2)

	o, err := logger.Append(msg)
	require.NoError(t, err)
	_, err = logger.Append(msg)
	require.NoError(t, err)
	_, err = logger.Append(msg)
	require.NoError(t, err)

	assert.Equal(t, 2, len(logger.segments))

	data, err := logger.Read(o)
	require.NoError(t, err)
	assert.Equal(t, msg, data)
}

func TestReopenSegmentedLog(t *testing.T) {
	dir := t.TempDir()
	msg := []byte("hello there")
	logLen := uint64(4 + len(msg))

	// create, write, close
	l1, err := NewSegmentedLog(dir, logLen*2)
	require.NoError(t, err)

	o1, err := l1.Append(msg)
	require.NoError(t, err)
	_, err = l1.Append(msg)
	require.NoError(t, err)
	_, err = l1.Append(msg) // triggers rotation
	require.NoError(t, err)

	require.Equal(t, 2, len(l1.segments), "2 segments")

	// reopen
	l2, err := NewSegmentedLog(dir, logLen*2)
	require.NoError(t, err)

	require.Equal(t, 2, len(l2.segments), "2 segments after reopen")

	data, err := l2.Read(o1)
	require.NoError(t, err)
	assert.Equal(t, msg, data)
}

func newTestSegmentedLog(t testing.TB, maxSize int) *SegmentedLog {
	t.Helper()

	i := 0
	l := &SegmentedLog{
		maxSegmentSize: uint64(maxSize),
		newLogger: func(baseOffset uint64) (EventLogger, error) {
			i++
			return &TestLog{data: make([]byte, maxSize*2)}, nil
		},
		segments: []*Segment{},
	}

	l.rotate(0)
	return l
}
