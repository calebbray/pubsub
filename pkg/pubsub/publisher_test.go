package pubsub

import (
	"fmt"
	"testing"

	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishEvent(t *testing.T) {
	e := "test-event"
	n := "caleb"
	r := NewRegistry()

	testEvent := NewEvent(e, "publisher", []byte("Hello, world"))

	count := 0
	_, err := r.Subscribe(n, e, deliverFuncCounter(DefaultDeliverFunc, &count))
	require.NoError(t, err)

	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})
	require.NoError(t, b.Publish(testEvent))

	assert.Equal(t, 1, count)
}

func TestPublishEventPersistsToLog(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s := "caleb"

	testEvent := NewEvent(e, p, d)

	count := 0
	_, err := r.Subscribe(s, e, deliverFuncCounter(DefaultDeliverFunc, &count))
	require.NoError(t, err)

	require.NoError(t, b.Publish(testEvent))

	logBytes, err := b.log.Read(0)
	require.NoError(t, err)

	decodeEventLog, err := decodeEvent(logBytes)
	require.NoError(t, err)

	assert.Equal(t, p, decodeEventLog.ProducerId)
	assert.Equal(t, d, decodeEventLog.Payload)
	assert.Equal(t, e, decodeEventLog.Type)
}

func TestPublishEventWithNoSubscribers(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})
	e := "fake-event"
	p := "publisher"
	d := []byte("Hello, World")

	fakeEvent := NewEvent(e, p, d)
	require.NoError(t, b.Publish(fakeEvent))

	logBytes, err := b.log.Read(0)
	require.NoError(t, err)

	decodeEventLog, err := decodeEvent(logBytes)
	require.NoError(t, err)

	assert.Equal(t, p, decodeEventLog.ProducerId)
	assert.Equal(t, d, decodeEventLog.Payload)
	assert.Equal(t, e, decodeEventLog.Type)
}

func TestPublishToMultipleSubscribers(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s1 := "caleb"
	s2 := "bray"

	testEvent := NewEvent(e, p, d)

	count := 0
	_, err := r.Subscribe(s1, e, deliverFuncCounter(DefaultDeliverFunc, &count))
	require.NoError(t, err)
	_, err = r.Subscribe(s2, e, deliverFuncCounter(DefaultDeliverFunc, &count))
	require.NoError(t, err)

	require.NoError(t, b.Publish(testEvent))

	assert.Equal(t, 2, count)
}

func TestPublishSubsciberFailsToOneSub(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s1 := "caleb"
	s2 := "bray"

	testEvent := NewEvent(e, p, d)

	count := 0
	errCount := 0
	_, err := r.Subscribe(s1, e, deliverFuncCounter(DefaultDeliverFunc, &count))
	_, err = r.Subscribe(s2, e, errDeliverCounter(&errCount))
	require.NoError(t, err)

	require.NoError(t, b.Publish(testEvent))

	assert.Equal(t, 1, count)
	assert.Equal(t, 1, errCount)
}

func deliverFuncCounter(fn DeliverFunc, count *int) DeliverFunc {
	return func(e Event, o uint64) error {
		*count++
		return fn(e, o)
	}
}

func errDeliverCounter(count *int) DeliverFunc {
	return func(e Event, o uint64) error {
		*count++
		return fmt.Errorf("simluating error")
	}
}
