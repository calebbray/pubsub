package registry

import (
	"path"
	"testing"

	"pipelines/pkg/pubsub"
	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionPersistentToDisk(t *testing.T) {
	p := path.Join(t.TempDir(), "test-store.json")
	fs, err := NewFileStore(p)
	require.NoError(t, err)

	pr, err := NewPersistentRegistry(fs)
	require.NoError(t, err)

	_, err = pr.Subscribe("caleb", "test-event", nil)
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

	_, err = pr.Subscribe("caleb", "test-event", nil)
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
	count := 0
	var sub *pubsub.Subscription
	sub, err = pr.Subscribe("caleb", "test-event", func(e pubsub.Event, offset uint64) error {
		count++
		require.NoError(t, pr.Ack(sub.ID, offset))
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, b.Publish(pubsub.NewEvent("test-event", "foo", nil)))
	require.NoError(t, b.Publish(pubsub.NewEvent("test-event", "foo", nil)))

	assert.Equal(t, 2, count)
	require.NoError(t, fs.fd.Close())

	fs, err = NewFileStore(p)
	require.NoError(t, err)

	pr, err = NewPersistentRegistry(fs)
	require.NoError(t, err)

	foundSub := pr.Registry.GetSubscriptionById(sub.ID)
	require.NotNil(t, foundSub)

	assert.True(t, foundSub.LastAckOffset > 0, "ack is persistent")

	require.NoError(t, pr.Reattach("caleb", "test-event", deliverFuncCounter(&count)))

	b = pubsub.NewEventBus(pr.Registry, utils.NewTestLog(1024), pubsub.BusOpts{})
	require.NoError(t, b.Publish(pubsub.NewEvent("test-event", "foo", nil)))
	assert.Equal(t, 3, count)
}

func deliverFuncCounter(count *int) pubsub.DeliverFunc {
	return func(e pubsub.Event, o uint64) error {
		*count++
		return nil
	}
}
