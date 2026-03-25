package pubsub

import (
	"sync"
	"testing"

	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testOnError(d delivery, err error) {}

func TestAckUpdatesSubscriptionOffset(t *testing.T) {
	r := NewRegistry()
	e := "test-event"
	s := "caleb"

	sub, _ := r.Subscribe(s, e, DefaultDeliverFunc, Drop, testOnError)
	// just testing ack actually updates a subscription pointer
	// no orchestrating of a bus / logging

	require.NoError(t, r.Ack(sub.ID, 42069))
	assert.Equal(t, uint64(42069), sub.LastAckOffset)
}

func TestPublishPassesCorrectOffsetToDeliver(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{PoolWorkers: 10})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	testEvent := NewEvent(e, p, d)
	encoded, err := encodeEvent(testEvent)
	require.NoError(t, err)
	encodedLen := len(encoded)

	var wg sync.WaitGroup
	var size uint64
	_, err = r.Subscribe(s, e, deliverOffsetChecker(&size, &wg), Drop, testOnError)
	require.NoError(t, err)

	wg.Add(1)
	require.NoError(t, b.Publish(testEvent))
	wg.Wait()
	assert.Equal(t, uint64(0), size)

	wg.Add(1)
	require.NoError(t, b.Publish(testEvent))
	wg.Wait()
	// +4 for the length prefix header
	assert.Equal(t, uint64(encodedLen+4), size)
}

func TestReplayDeliversAllEventsNotAcked(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{PoolWorkers: 10})
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

	var wg sync.WaitGroup
	// Publish 5 events we will Ack the first one
	require.NoError(t, b.Publish(e1))
	require.NoError(t, b.Publish(e2))
	require.NoError(t, b.Publish(e3))
	require.NoError(t, b.Publish(e4))
	require.NoError(t, b.Publish(e5))

	var count int
	sub, err := r.Subscribe(s, e, deliverFuncCounter(&count, &wg), Drop, testOnError)
	require.NoError(t, err)

	r.Ack(sub.ID, offset)
	// resetting count to 0 so we can verify the four unacked events are replayed

	wg.Add(4)
	b.Replay(sub.ID)
	wg.Wait()
	assert.Equal(t, 4, count)
}

func TestReplayFromStart(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{PoolWorkers: 10})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	e1 := NewEvent(e, p, d)
	e2 := NewEvent(e, p, d)
	e3 := NewEvent(e, p, d)
	e4 := NewEvent(e, p, d)
	e5 := NewEvent(e, p, d)

	var wg sync.WaitGroup
	require.NoError(t, b.Publish(e1))
	require.NoError(t, b.Publish(e2))
	require.NoError(t, b.Publish(e3))
	require.NoError(t, b.Publish(e4))
	require.NoError(t, b.Publish(e5))

	var count int
	sub, err := r.Subscribe(s, e, deliverFuncCounter(&count, &wg), Drop, testOnError)
	require.NoError(t, err)
	wg.Add(5)
	b.Replay(sub.ID)
	wg.Wait()
	assert.Equal(t, 5, count)
}

func deliverOffsetChecker(size *uint64, wg *sync.WaitGroup) DeliverFunc {
	return func(e Event, o uint64) error {
		*size = o
		wg.Done()
		return nil
	}
}
