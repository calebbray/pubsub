package eventlog

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogWriteRead(t *testing.T) {
	l := createTestLog(t)
	defer l.Close()

	msg := "foo"
	offset, err := l.Append([]byte(msg))
	require.NoError(t, err)

	data, err := l.Read(offset)
	require.NoError(t, err)

	assert.Equal(t, msg, string(data))
}

func TestAppendMultipleLogs(t *testing.T) {
	l := createTestLog(t)
	defer l.Close()

	msg1 := []byte("foo")
	msg2 := []byte("bar")
	msg3 := []byte("baz")

	off1, err := l.Append(msg1)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), off1)

	off2, err := l.Append(msg2)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(msg1)+4), off2)

	off3, err := l.Append(msg3)
	require.NoError(t, err)

	log1, err := l.Read(off1)
	require.NoError(t, err)

	log2, err := l.Read(off2)
	require.NoError(t, err)

	log3, err := l.Read(off3)
	require.NoError(t, err)

	assert.Equal(t, msg1, log1)
	assert.Equal(t, msg2, log2)
	assert.Equal(t, msg3, log3)
}

func TestReadingInvalidOffsetErrors(t *testing.T) {
	l := createTestLog(t)

	_, err := l.Read(0)
	require.Error(t, err)

	_, err = l.Append([]byte("my super log message"))
	require.NoError(t, err)

	_, err = l.Read(1)
	assert.Error(t, err)
}

func createTestLog(t *testing.T) *Log {
	t.Helper()
	file := path.Join(t.TempDir(), "test.log")

	l, err := NewLog(file)
	require.NoError(t, err)
	return l
}
