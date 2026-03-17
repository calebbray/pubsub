package pubsub

import (
	"fmt"
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

	failed := false
	count := 0
	_, err := r.Subscribe(s, e,
		failOnceDeliverFunc(
			deliverFuncCounter(DefaultDeliverFunc, &count),
			&failed,
		),
	)
	require.NoError(t, err)

	require.NoError(t, b.Publish(ev))

	assert.True(t, failed)
	assert.Equal(t, 1, count)
}

func TestDeliveryFailsRetriesSendsToDLQ(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Retry: RetryPolicy{
			MaxRetries: 3,
			Delay:      10 * time.Millisecond,
		},
		Dlq: &DeadLetterQueue{utils.NewTestLog(1024)},
	})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	ev := NewEvent(e, p, d)

	sub, err := r.Subscribe(s, e, failAlwaysDeliverFunc(DefaultDeliverFunc))
	require.NoError(t, err)

	require.NoError(t, b.Publish(ev))

	data, err := b.Dlq.Read(0)
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
		Dlq: &DeadLetterQueue{utils.NewTestLog(1024)},
	})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	ev := NewEvent(e, p, d)

	count := 0
	_, err := r.Subscribe(s, e, deliverFuncCounter(DefaultDeliverFunc, &count))
	require.NoError(t, err)

	require.NoError(t, b.Publish(ev))

	assert.Equal(t, 1, count)

	_, err = b.Dlq.Read(0)
	require.Error(t, err)
}

func TestZeroRetriesSendToDLQ(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{
		Dlq: &DeadLetterQueue{utils.NewTestLog(1024)},
	})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	ev := NewEvent(e, p, d)

	failed := false
	sub, err := r.Subscribe(s, e, failOnceDeliverFunc(DefaultDeliverFunc, &failed))
	require.NoError(t, err)

	require.NoError(t, b.Publish(ev))

	assert.True(t, failed)

	data, err := b.Dlq.Read(0)
	require.NoError(t, err)

	l, err := decodeDlqLog(data)
	require.NoError(t, err)
	assert.Equal(t, "failed once", l.Reason)
	assert.Equal(t, sub.ID, l.SubscriberId)
}

func failOnceDeliverFunc(fn DeliverFunc, failed *bool) DeliverFunc {
	return func(e Event, offset uint64) error {
		if !*failed {
			*failed = true
			return fmt.Errorf("failed once")
		}

		return fn(e, offset)
	}
}

func failAlwaysDeliverFunc(DeliverFunc) DeliverFunc {
	return func(e Event, offset uint64) error {
		return fmt.Errorf("failed")
	}
}
