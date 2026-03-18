package pubsub

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeAndRetrieve(t *testing.T) {
	r := NewRegistry()
	e := "test-event"
	subscriber := "caleb"

	s, err := r.Subscribe(subscriber, e)
	require.NoError(t, err)
	t.Run("retrieve by event type", func(t *testing.T) {
		subs, err := r.GetSubscriptions(e)
		require.NoError(t, err)

		require.Equal(t, 1, len(subs))
		assert.Equal(t, s, subs[0])
	})

	t.Run("retrieve by subscriber", func(t *testing.T) {
		subs, err := r.GetSubscriberSubscriptions(subscriber)
		require.NoError(t, err)

		require.Equal(t, 1, len(subs))
		assert.Equal(t, s, subs[0])
	})
}

func TestGetSubscribersReturnsEmptySliceIfNoSubs(t *testing.T) {
	r := NewRegistry()
	subs, _ := r.GetSubscriptions("foo")
	assert.Equal(t, 0, len(subs))
}

func TestUnsubscribeLifecycle(t *testing.T) {
	r := NewRegistry()
	e1 := "test-event-1"
	e2 := "test-event-2"
	e3 := "test-event-3"
	subscriber := "caleb"

	_, err := r.Subscribe(subscriber, e1)
	require.NoError(t, err)

	s2, err := r.Subscribe(subscriber, e2)
	require.NoError(t, err)

	s3, err := r.Subscribe(subscriber, e3)
	require.NoError(t, err)

	t.Run("unsubscribe removes from event type", func(t *testing.T) {
		require.NoError(t, r.Unsubscribe(s2.ID))
		subs, _ := r.GetSubscriptions(e2)
		for _, s := range subs {
			assert.NotEqual(t, s.ID, s2.ID)
		}
	})

	t.Run("unsubscribe removes from subscriber type", func(t *testing.T) {
		require.NoError(t, r.Unsubscribe(s3.ID))
		subs, _ := r.GetSubscriptions(e3)
		for _, s := range subs {
			assert.NotEqual(t, s.ID, s3.ID)
		}
	})
}

func TestMultipleSubscribersToSameEventType(t *testing.T) {
	r := NewRegistry()
	e := "test-event-1"
	s1 := "caleb"
	s2 := "bray"

	_, err := r.Subscribe(s1, e)
	require.NoError(t, err)
	_, err = r.Subscribe(s2, e)
	require.NoError(t, err)

	subs, err := r.GetSubscriptions(e)
	require.NoError(t, err)

	assert.Equal(t, 2, len(subs))
}
