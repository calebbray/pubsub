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

func TestPublishEvent(t *testing.T) {
	e := "test-event"
	n := "caleb"
	r := NewRegistry()

	testEvent := NewEvent(e, "publisher", []byte("Hello, world"))

	var wg sync.WaitGroup
	count := 0
	_, err := r.Subscribe(n, e, deliverFuncCounter(&count, &wg), Drop, testOnError)
	require.NoError(t, err)

	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{})

	wg.Add(1)
	require.NoError(t, b.Publish(testEvent))
	wg.Wait()
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

	var wg sync.WaitGroup
	count := 0
	_, err := r.Subscribe(s, e, deliverFuncCounter(&count, &wg), Drop, testOnError)
	require.NoError(t, err)

	wg.Add(1)
	require.NoError(t, b.Publish(testEvent))
	wg.Wait()

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

	var wg sync.WaitGroup
	var count atomic.Uint32
	_, err := r.Subscribe(s1, e,
		func(e Event, offset uint64) error {
			count.Add(1)
			wg.Done()
			return nil
		},
		Drop, testOnError)
	require.NoError(t, err)
	_, err = r.Subscribe(s2, e, func(e Event, offset uint64) error {
		count.Add(1)
		wg.Done()
		return nil
	},
		Drop, testOnError)
	require.NoError(t, err)

	wg.Add(2)
	require.NoError(t, b.Publish(testEvent))
	wg.Wait()

	assert.Equal(t, 2, int(count.Load()))
}

func TestPublishSubsciberFailsToOneSub(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(1024), BusOpts{PoolWorkers: 10})
	e := "test-event"
	p := "publisher"
	d := []byte("Hello, World")
	s1 := "caleb"
	s2 := "bray"

	testEvent := NewEvent(e, p, d)

	var wg sync.WaitGroup
	count := 0
	errCount := 0
	_, err := r.Subscribe(s1, e, deliverFuncCounter(&count, &wg), Drop, testOnError)
	_, err = r.Subscribe(s2, e, errDeliverCounter(&errCount, &wg), Drop, testOnError)
	require.NoError(t, err)

	wg.Add(2)
	require.NoError(t, b.Publish(testEvent))
	wg.Wait()

	assert.Equal(t, 1, count)
	assert.Equal(t, 1, errCount)
}

func TestSlowSubscriberDropPolicy(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(4096), BusOpts{PoolWorkers: 10})
	e := "test-event"

	block := make(chan struct{})
	var received atomic.Int32

	_, err := r.Subscribe("caleb", e, func(e Event, offset uint64) error {
		<-block
		received.Add(1)
		return nil
	}, Drop, testOnError)
	require.NoError(t, err)

	for i := range 100 {
		b.Publish(NewEvent(e, "publisher", fmt.Appendf(nil, "event-%d", i)))
	}

	close(block)

	require.Eventually(t, func() bool {
		return received.Load() >= 1
	}, time.Second, 10*time.Millisecond)

	assert.Less(t, int(received.Load()), 100)
}

func TestSlowSubscriberDisconnectPolicy(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(4096), BusOpts{PoolWorkers: 10})
	e := "test-event"

	block := make(chan struct{})

	sub, err := r.Subscribe("caleb", e, func(ev Event, offset uint64) error {
		<-block
		return nil
	}, Disconnect, testOnError)
	require.NoError(t, err)

	for i := range 20 {
		b.Publish(NewEvent(e, "publisher", fmt.Appendf(nil, "event-%d", i)))
	}

	// subscription should have been removed
	require.Eventually(t, func() bool {
		return r.GetSubscriptionById(sub.ID) == nil
	}, time.Second, 10*time.Millisecond)

	close(block)
}

func TestSlowSubscriberDoesntBlockFastOne(t *testing.T) {
	r := NewRegistry()
	b := NewEventBus(r, utils.NewTestLog(4096), BusOpts{PoolWorkers: 10})
	e := "test-event"

	block := make(chan struct{})
	var fastCount atomic.Int32
	var wg sync.WaitGroup

	_, err := r.Subscribe("slow", e, func(ev Event, offset uint64) error {
		<-block
		return nil
	}, Drop, testOnError)
	require.NoError(t, err)

	_, err = r.Subscribe("fast", e, func(ev Event, offset uint64) error {
		fastCount.Add(1)
		wg.Done()
		return nil
	}, Drop, testOnError)
	require.NoError(t, err)

	wg.Add(5)
	for i := range 5 {
		b.Publish(NewEvent(e, "publisher", fmt.Appendf(nil, "event-%d", i)))
	}

	wg.Wait()
	assert.Equal(t, int32(5), fastCount.Load())

	close(block)
}

func deliverFuncCounter(count *int, wg *sync.WaitGroup) DeliverFunc {
	return func(e Event, o uint64) error {
		*count++
		wg.Done()
		return nil
	}
}

func errDeliverCounter(count *int, wg *sync.WaitGroup) DeliverFunc {
	return func(e Event, o uint64) error {
		*count++
		wg.Done()
		return fmt.Errorf("simluating error")
	}
}

func NoOpOnDeliveryError(Delivery, error) {}
