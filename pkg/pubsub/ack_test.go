package pubsub

import (
	"testing"

	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAckUpdatesSubscriptionOffset(t *testing.T) {
	r := NewRegistry()
	e := "test-event"
	s := "caleb"

	sub, _ := r.Subscribe(s, e, DefaultDeliverFunc)
	// just testing ack actually updates a subscription pointer
	// no orchestrating of a bus / logging

	require.NoError(t, r.Ack(sub.ID, 42069))
	assert.Equal(t, uint64(42069), sub.lastAckOffset)
}

func TestPublishPassesCorrectOffsetToDeliver(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	testEvent := NewEvent(e, p, d)
	encoded, err := encodeEvent(testEvent)
	require.NoError(t, err)
	encodedLen := len(encoded)

	var size uint64
	_, err = r.Subscribe(s, e, deliverOffsetChecker(DefaultDeliverFunc, &size))
	require.NoError(t, err)

	require.NoError(t, b.Publish(testEvent))
	assert.Equal(t, uint64(0), size)

	require.NoError(t, b.Publish(testEvent))
	// +4 for the length prefix header
	assert.Equal(t, uint64(encodedLen+4), size)
}

func TestReplayDeliversAllEventsNotAcked(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	e1 := NewEvent(e, p, d)
	e2 := NewEvent(e, p, d)
	e3 := NewEvent(e, p, d)
	e4 := NewEvent(e, p, d)
	e5 := NewEvent(e, p, d)
	encoded, err := encodeEvent(e1)
	require.NoError(t, err)
	offset := uint64(len(encoded) + 4)

	// Publish 5 events we will Ack the first one
	require.NoError(t, b.Publish(e1))
	require.NoError(t, b.Publish(e2))
	require.NoError(t, b.Publish(e3))
	require.NoError(t, b.Publish(e4))
	require.NoError(t, b.Publish(e5))

	var count int
	sub, err := r.Subscribe(s, e, deliverFuncCounter(DefaultDeliverFunc, &count))
	require.NoError(t, err)

	r.Ack(sub.ID, offset)
	// resetting count to 0 so we can verify the four unacked events are replayed

	b.Replay(sub.ID)
	assert.Equal(t, 4, count)
}

func TestReplayFromStart(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	e1 := NewEvent(e, p, d)
	e2 := NewEvent(e, p, d)
	e3 := NewEvent(e, p, d)
	e4 := NewEvent(e, p, d)
	e5 := NewEvent(e, p, d)

	require.NoError(t, b.Publish(e1))
	require.NoError(t, b.Publish(e2))
	require.NoError(t, b.Publish(e3))
	require.NoError(t, b.Publish(e4))
	require.NoError(t, b.Publish(e5))

	var count int
	sub, err := r.Subscribe(s, e, deliverFuncCounter(DefaultDeliverFunc, &count))
	require.NoError(t, err)
	b.Replay(sub.ID)
	assert.Equal(t, 5, count)
}

func deliverOffsetChecker(fn DeliverFunc, size *uint64) DeliverFunc {
	return func(e Event, o uint64) error {
		*size = o
		return fn(e, o)
	}
}
