package registry

import (
	"path"
	"sync"
	"testing"

	"github.com/calebbray/pubsub/pkg/pubsub"
	"github.com/calebbray/pubsub/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionPersistentToDisk(t *testing.T) {
	p := path.Join(t.TempDir(), "test-store.json")
	fs, err := NewFileStore(p)
	require.NoError(t, err)

	pr, err := NewPersistentRegistry(fs)
	require.NoError(t, err)

	_, err = pr.Subscribe("caleb", "test-event", nil, pubsub.Drop, noOpOnDeliveryError)
	require.NoError(t, err)
	require.NoError(t, fs.fd.Close())

	fs, err = NewFileStore(p)
	require.NoError(t, err)

	pr, err = NewPersistentRegistry(fs)
	require.NoError(t, err)

	subs, err := pr.Registry.GetSubscriptions("test-event")
	require.NoError(t, err)
	assert.Equal(t, 1, len(subs), "one test event subscription")

	subs, err = pr.Registry.GetSubscriberSubscriptions("caleb")
	require.NoError(t, err)
	assert.Equal(t, 1, len(subs), "one caleb subscription")
}

func TestUnsubscriptionOnDisk(t *testing.T) {
	p := path.Join(t.TempDir(), "test-store.json")
	fs, err := NewFileStore(p)
	require.NoError(t, err)

	pr, err := NewPersistentRegistry(fs)
	require.NoError(t, err)

	_, err = pr.Subscribe("caleb", "test-event", nil, pubsub.Drop, noOpOnDeliveryError)
	require.NoError(t, err)
	require.NoError(t, fs.fd.Close())

	fs, err = NewFileStore(p)
	require.NoError(t, err)

	pr, err = NewPersistentRegistry(fs)
	require.NoError(t, err)

	subs, err := pr.Registry.GetSubscriberSubscriptions("caleb")
	require.NoError(t, err)
	assert.Equal(t, 1, len(subs), "one caleb subscription")

	require.NoError(t, pr.Unsubscribe(subs[0].ID))
	require.NoError(t, fs.fd.Close())

	fs, err = NewFileStore(p)
	require.NoError(t, err)

	pr, err = NewPersistentRegistry(fs)
	require.NoError(t, err)

	subs, err = pr.Registry.GetSubscriberSubscriptions("caleb")
	require.NoError(t, err)
	assert.Equal(t, 0, len(subs), "zero caleb subscription")
}

func TestReattachDeliverFunc(t *testing.T) {
	p := path.Join(t.TempDir(), "test-store.json")
	fs, err := NewFileStore(p)
	require.NoError(t, err)

	pr, err := NewPersistentRegistry(fs)
	require.NoError(t, err)

	b := pubsub.NewEventBus(pr.Registry, utils.NewTestLog(1024), pubsub.BusOpts{})

	var wg sync.WaitGroup
	count := 0
	var lastOffset uint64
	var sub *pubsub.Subscription

	sub, err = pr.Subscribe("caleb", "test-event", func(e pubsub.Event, offset uint64) error {
		lastOffset = offset
		count++
		wg.Done()
		return nil
	}, pubsub.Drop, noOpOnDeliveryError)
	require.NoError(t, err)

	// publish one event, wait for delivery
	wg.Add(1)
	require.NoError(t, b.Publish(pubsub.NewEvent("test-event", "foo", nil)))
	wg.Wait()

	assert.Equal(t, 1, count)

	// persist ack explicitly after goroutine finishes
	require.NoError(t, pr.Ack(sub.ID, lastOffset))
	require.NoError(t, fs.fd.Close())

	// reopen — simulate restart
	fs, err = NewFileStore(p)
	require.NoError(t, err)

	pr, err = NewPersistentRegistry(fs)
	require.NoError(t, err)

	foundSub := pr.Registry.GetSubscriptionById(sub.ID)
	require.NotNil(t, foundSub)
	assert.Equal(t, lastOffset, foundSub.LastAckOffset, "ack offset persisted correctly")

	// reattach with new deliver func
	wg.Add(1)
	require.NoError(t, pr.Reattach("caleb", "test-event", deliverFuncCounter(&count, &wg), pubsub.Drop, noOpOnDeliveryError))

	b = pubsub.NewEventBus(pr.Registry, utils.NewTestLog(1024), pubsub.BusOpts{})
	require.NoError(t, b.Publish(pubsub.NewEvent("test-event", "foo", nil)))
	wg.Wait()

	assert.Equal(t, 2, count)
}

func deliverFuncCounter(count *int, wg *sync.WaitGroup) pubsub.DeliverFunc {
	return func(e pubsub.Event, o uint64) error {
		*count++
		wg.Done()
		return nil
	}
}

func noOpOnDeliveryError(pubsub.Delivery, error) {}
