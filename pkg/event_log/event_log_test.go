package eventlog

import (
	"fmt"
	"io"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogWriteRead(t *testing.T) {
	l := NewTestLog(1024)
	defer l.Close()

	msg := "foo"
	offset, err := l.Append([]byte(msg))
	require.NoError(t, err)

	data, err := l.Read(offset)
	require.NoError(t, err)

	assert.Equal(t, msg, string(data))
}

func TestAppendMultipleLogs(t *testing.T) {
	l := NewTestLog(1024)
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
	l := NewTestLog(1024)

	_, err := l.Read(0)
	require.Error(t, err)

	_, err = l.Append([]byte("my super log message"))
	require.NoError(t, err)

	_, err = l.Read(1)
	assert.Error(t, err)
}

func TestSyncOnHealthyFile(t *testing.T) {
	file := path.Join(t.TempDir(), "SyncTest")

	log, err := NewFileLog(file)
	require.NoError(t, err)
	defer log.Close()

	msgs := []string{"foo", "bar", "baz"}
	fillLogs(t, log, msgs...)

	require.NoError(t, log.Sync())
}

func TestPersistenceFileLog(t *testing.T) {
	file := path.Join(t.TempDir(), "PersistenceTest")

	log, err := NewFileLog(file)
	require.NoError(t, err)

	msgs := []string{"foo", "bar", "baz"}
	fillLogs(t, log, msgs...)

	require.NoError(t, log.Sync())
	require.NoError(t, log.Close())
	assert.Nil(t, log.EventLogger)

	log, err = NewFileLog(file)
	require.NoError(t, err)

	data, err := log.Read(0)
	require.NoError(t, err)

	assert.Equal(t, msgs[0], string(data))
}

func TestReopenedFileAppends(t *testing.T) {
	file := path.Join(t.TempDir(), "AppendAfterReopen")

	log, err := NewFileLog(file)
	require.NoError(t, err)

	msgs := []string{"foo", "bar", "baz"}
	fillLogs(t, log, msgs...)

	require.NoError(t, log.Sync())
	require.NoError(t, log.Close())
	assert.Nil(t, log.EventLogger)

	log, err = NewFileLog(file)
	require.NoError(t, err)

	msg := "last message"
	_, err = log.Append([]byte(msg))
	require.NoError(t, err)

	msgs = append(msgs, msg)
	i := NewIterator(log, 0)
	idx := 0
	for i.Next() {
		assert.Equal(t, msgs[idx], string(i.Data()))
		idx++
	}

	assert.NoError(t, i.Err())
}

func NewTestLog(size int64) *Log {
	return &Log{
		EventLogger: &TestLog{
			data: make([]byte, size),
		},
	}
}

type TestLog struct {
	data []byte
}

func (l *TestLog) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset >= int64(len(l.data)) {
		return 0, io.EOF
	}

	if offset < 0 {
		return 0, fmt.Errorf("negative offset")
	}

	available := int64(len(l.data)) - offset
	if int64(len(p)) < available {
		n = len(p)
	} else {
		n = int(available)
		err = io.EOF
	}

	copy(p[:n], l.data[offset:offset+int64(n)])
	return n, err
}

func (l *TestLog) WriteAt(p []byte, offset int64) (n int, err error) {
	if offset < 0 || offset > int64(len(l.data)) {
		return 0, fmt.Errorf("invalid offset %d", offset)
	}

	remainingSpace := int64(len(l.data)) - offset
	if int64(len(p)) > remainingSpace {
		p = p[:remainingSpace]
		err = io.EOF
	}

	copy(l.data[offset:], p)
	n = len(p)
	return n, err
}

func (l *TestLog) Close() error {
	return nil
}

func (l *TestLog) Sync() error {
	return nil
}
