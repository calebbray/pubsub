package eventlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIterateFromStart(t *testing.T) {
	l := NewTestLog(1024)
	msgs := []string{"foo", "bar", "baz"}
	fillLogs(t, l, msgs...)

	i := NewIterator(l, 0)

	idx := 0
	for i.Next() {
		assert.Equal(t, msgs[idx], string(i.Data()))
		idx++
	}

	assert.NoError(t, i.Err())
}

func TestIterateFromMiddle(t *testing.T) {
	l := NewTestLog(1024)
	msgs := []string{"foo", "bar", "baz"}
	offsets := fillLogs(t, l, msgs...)

	i := NewIterator(l, offsets[1])
	idx := 1
	for i.Next() {
		assert.Equal(t, msgs[idx], string(i.Data()))
		idx++
	}
	assert.NoError(t, i.Err())
}

func TestIterateEmptyLog(t *testing.T) {
	l := NewTestLog(1024)
	i := NewIterator(l, 0)
	assert.False(t, i.Next())
}

func TestIterateEndAfterAppend(t *testing.T) {
	l := NewTestLog(1024)
	msgs := []string{"foo", "bar", "baz"}
	fillLogs(t, l, msgs...)

	i := NewIterator(l, 0)
	idx := 0
	for i.Next() {
		assert.Equal(t, msgs[idx], string(i.Data()))
		idx++
	}

	nextMsgs := []string{"caleb", "joseph", "bray"}
	fillLogs(t, l, nextMsgs...)
	idx = 0
	for i.Next() {
		assert.Equal(t, nextMsgs[idx], string(i.Data()))
		idx++
	}
}

func fillLogs(t *testing.T, l *Log, messages ...string) []uint64 {
	t.Helper()
	offsets := make([]uint64, len(messages))
	for i, msg := range messages {
		o, err := l.Append([]byte(msg))
		require.NoError(t, err)

		offsets[i] = o
	}

	return offsets
}
