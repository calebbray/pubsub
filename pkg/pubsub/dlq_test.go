package pubsub

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliverySucceedsAfterRetry(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Retry: RetryPolicy{
			MaxRetries: 3,
			Delay:      100 * time.Millisecond,
		},
	})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	ev := NewEvent(e, p, d)

	var wg sync.WaitGroup
	failed := false
	// count := 0
	_, err := r.Subscribe(s, e,
		failOnceDeliverFunc(
			&failed,
			&wg,
		),
		Drop,
		testOnError,
	)
	require.NoError(t, err)

	wg.Add(1)
	require.NoError(t, b.Publish(ev))
	wg.Wait()

	assert.True(t, failed)
}

func TestDeliveryFailsRetriesSendsToDLQ(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Retry: RetryPolicy{
			MaxRetries: 3,
			Delay:      10 * time.Millisecond,
		},
		Dlq:         &DeadLetterQueue{utils.NewTestLog(1024)},
		PoolWorkers: 10,
	})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	ev := NewEvent(e, p, d)

	sub, err := b.Subscribe(s, e, failAlwaysDeliverFunc(), Drop)
	require.NoError(t, err)

	require.NoError(t, b.Publish(ev))

	var data []byte
	require.Eventually(t, func() bool {
		data, err = b.Dlq.Read(0)
		return err == nil
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, err)

	l, err := decodeDlqLog(data)
	require.NoError(t, err)
	assert.Equal(t, "failed", l.Reason)
	assert.Equal(t, sub.ID, l.SubscriberId)
}

func TestSuccessfulDeliveryEmptyDLQ(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Retry: RetryPolicy{
			MaxRetries: 3,
			Delay:      10 * time.Millisecond,
		},
		Dlq:         &DeadLetterQueue{utils.NewTestLog(1024)},
		PoolWorkers: 10,
	})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	ev := NewEvent(e, p, d)

	var wg sync.WaitGroup
	count := 0
	_, err := r.Subscribe(s, e, deliverFuncCounter(&count, &wg), Drop, testOnError)
	require.NoError(t, err)

	wg.Add(1)
	require.NoError(t, b.Publish(ev))
	wg.Wait()

	assert.Equal(t, 1, count)

	_, err = b.Dlq.Read(0)
	require.Error(t, err)
}

func TestZeroRetriesSendToDLQ(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Dlq:         &DeadLetterQueue{utils.NewTestLog(1024)},
		PoolWorkers: 10,
	})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	ev := NewEvent(e, p, d)

	failed := false
	sub, err := b.Subscribe(s, e, failOnceDeliverFunc(&failed, nil), Drop)
	require.NoError(t, err)

	require.NoError(t, b.Publish(ev))

	var data []byte
	require.Eventually(t, func() bool {
		data, err = b.Dlq.Read(0)
		return err == nil
	}, time.Second, 10*time.Millisecond)

	assert.True(t, failed)
	require.NoError(t, err)

	l, err := decodeDlqLog(data)
	require.NoError(t, err)
	assert.Equal(t, "failed once", l.Reason)
	assert.Equal(t, sub.ID, l.SubscriberId)
}

func TestDLQInspect(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Dlq:         &DeadLetterQueue{utils.NewTestLog(1024)},
		Retry:       RetryPolicy{MaxRetries: 0},
		PoolWorkers: 10,
	})

	e := "test-event"
	ev1 := NewEvent(e, "publisher", []byte("event 1"))
	ev2 := NewEvent(e, "publisher", []byte("event 2"))

	sub1, err := b.Subscribe("caleb", e, failAlwaysDeliverFunc(), Drop)
	require.NoError(t, err)

	require.NoError(t, b.Publish(ev1))
	require.NoError(t, b.Publish(ev2))

	require.Eventually(t, func() bool {
		entries, _ := b.Dlq.Inspect()
		return len(entries) == 2
	}, time.Second, 10*time.Millisecond)

	entries, err := b.Dlq.Inspect()
	require.NoError(t, err)
	require.Equal(t, 2, len(entries))
	assert.Equal(t, sub1.ID, entries[0].Log.SubscriberId)
	assert.Equal(t, sub1.ID, entries[1].Log.SubscriberId)
	assert.Equal(t, "failed", entries[0].Log.Reason)
	assert.Equal(t, "failed", entries[1].Log.Reason)
}

func TestDLQReplay(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Dlq:         &DeadLetterQueue{utils.NewTestLog(1024)},
		Retry:       RetryPolicy{MaxRetries: 0},
		PoolWorkers: 10,
	})

	e := "test-event"
	ev := NewEvent(e, "publisher", []byte("replay me"))

	// first subscriber always fails — goes to DLQ
	_, err := b.Subscribe("caleb", e, failAlwaysDeliverFunc(), Drop)
	require.NoError(t, err)

	require.NoError(t, b.Publish(ev))

	require.Eventually(t, func() bool {
		entries, _ := b.Dlq.Inspect()
		return len(entries) == 1
	}, time.Second, 10*time.Millisecond)

	// now register a working subscriber and replay
	var wg sync.WaitGroup
	var count atomic.Int32
	_, err = r.Subscribe("recovery", e, func(ev Event, offset uint64) error {
		count.Add(1)
		wg.Done()
		return nil
	}, Drop, testOnError)
	require.NoError(t, err)

	wg.Add(1)
	require.NoError(t, b.Dlq.Replay(b))
	wg.Wait()

	assert.Equal(t, int32(1), count.Load())
}

func TestDLQClear(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Dlq:         &DeadLetterQueue{utils.NewTestLog(1024)},
		Retry:       RetryPolicy{MaxRetries: 0},
		PoolWorkers: 10,
	})

	e := "test-event"

	_, err := b.Subscribe("caleb", e, failAlwaysDeliverFunc(), Drop)
	require.NoError(t, err)

	require.NoError(t, b.Publish(NewEvent(e, "publisher", nil)))

	require.Eventually(t, func() bool {
		entries, _ := b.Dlq.Inspect()
		return len(entries) == 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, b.Dlq.Clear())

	entries, err := b.Dlq.Inspect()
	require.NoError(t, err)
	assert.Equal(t, 0, len(entries))
}

func TestDLQReplayAfterClear(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Dlq:         &DeadLetterQueue{utils.NewTestLog(1024)},
		Retry:       RetryPolicy{MaxRetries: 0},
		PoolWorkers: 10,
	})

	e := "test-event"

	_, err := b.Subscribe("caleb", e, failAlwaysDeliverFunc(), Drop)
	require.NoError(t, err)

	require.NoError(t, b.Publish(NewEvent(e, "publisher", nil)))

	require.Eventually(t, func() bool {
		entries, _ := b.Dlq.Inspect()
		return len(entries) == 1
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, b.Dlq.Clear())
	require.NoError(t, b.Dlq.Replay(b))

	entries, err := b.Dlq.Inspect()
	require.NoError(t, err)
	assert.Equal(t, 0, len(entries))
}

func failOnceDeliverFunc(failed *bool, wg *sync.WaitGroup) DeliverFunc {
	return func(e Event, offset uint64) error {
		if wg != nil {
			wg.Done()
		}
		if !*failed {
			*failed = true
			return fmt.Errorf("failed once")
		}

		return nil
	}
}

func failAlwaysDeliverFunc() DeliverFunc {
	return func(e Event, offset uint64) error {
		return fmt.Errorf("failed")
	}
}
