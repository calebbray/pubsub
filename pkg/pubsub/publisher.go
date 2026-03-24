package pubsub

import (
	"encoding/json"
	"errors"
	"time"

	eventlog "pipelines/pkg/event_log"

	"github.com/google/uuid"
)

type DeliverFunc func(e Event, offset uint64) error

type Event struct {
	ID          uuid.UUID `json:"id"`
	Type        string    `json:"type"`
	ProducerId  string    `json:"producerId"`
	Payload     []byte    `json:"payload"`
	PublishedAt time.Time `json:"publishedAt"`
}

func NewEvent(et, producerId string, payload []byte) Event {
	return Event{
		ID:          uuid.New(),
		Type:        et,
		ProducerId:  producerId,
		Payload:     payload,
		PublishedAt: time.Now(),
	}
}

type BusOpts struct {
	Dlq   *DeadLetterQueue
	Retry RetryPolicy
}

type Bus struct {
	BusOpts
	registry *Registry
	log      *eventlog.Log
}

func NewEventBus(r *Registry, l *eventlog.Log, opts BusOpts) *Bus {
	return &Bus{
		registry: r,
		log:      l,
		BusOpts:  opts,
	}
}

func (b *Bus) Publish(e Event) error {
	encoded, err := encodeEvent(e)
	if err != nil {
		return err
	}

	offset, err := b.log.Append(encoded)
	if err != nil {
		return err
	}

	subs, _ := b.registry.GetSubscriptions(e.Type)

	for _, sub := range subs {
		select {
		case sub.deliverBuf.buf <- delivery{event: e, offset: offset}:
			// delivered
		default:
			// buffer is full - apply policy
			switch sub.deliverBuf.policy {
			case Drop:
				// skip
			case Disconnect:
				b.registry.Unsubscribe(sub.ID)
			}

		}
	}
	return nil
}

func (b *Bus) Subscribe(subscriberId, eventType string, fn DeliverFunc, policy SlowSubscriberPolicy) (*Subscription, error) {
	var subId string
	onError := func(d delivery, err error) {
		delivered := false
		for range b.Retry.MaxRetries {
			time.Sleep(b.Retry.Delay)
			if err := fn(d.event, d.offset); err == nil {
				delivered = true
				break
			}
		}

		if !delivered && b.Dlq != nil {
			b.Dlq.Append(d.event, err.Error(), subId)
		}
	}
	sub, err := b.registry.Subscribe(subscriberId, eventType, fn, policy, onError)
	if err != nil {
		return nil, err
	}
	subId = sub.ID
	return sub, nil
}

var ErrSubscriptionNotFound = errors.New("subscription not found")

func (b *Bus) Replay(subscriptionId string) error {
	// get the subscriber
	s := b.registry.GetSubscriptionById(subscriptionId)
	if s == nil {
		// should this just return nil? maybe this just shouldn't care
		return ErrSubscriptionNotFound
	}

	// create an iterator for the log
	logs := eventlog.NewIterator(b.log, s.LastAckOffset)

	// publish unacknowledged events back to the subscriber
	for logs.Next() {
		e, err := decodeEvent(logs.Data())
		if err != nil {
			return err
		}
		s.deliverBuf.buf <- delivery{event: e, offset: logs.Offset()}
	}
	return nil
}

func encodeEvent(e Event) ([]byte, error) {
	return json.Marshal(e)
}

func decodeEvent(p []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(p, &e); err != nil {
		return e, err
	}
	return e, nil
}
