package pubsub

import (
	"fmt"
	"sync"
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
