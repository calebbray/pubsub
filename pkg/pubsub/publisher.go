package pubsub

import (
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	eventlog "github.com/calebbray/pubsub/pkg/event_log"
	"github.com/calebbray/pubsub/pkg/metrics"

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
	Dlq         *DeadLetterQueue
	Retry       RetryPolicy
	PoolWorkers int
	Logger      *slog.Logger
	Metrics     metrics.MetricsProvider
}

type Bus struct {
	BusOpts
	registry SubscriptionRegistry
	log      eventlog.EventStore
	pool     *WorkerPool
}

func NewEventBus(r SubscriptionRegistry, l eventlog.EventStore, opts BusOpts) *Bus {
	workers := opts.PoolWorkers
	if workers == 0 {
		workers = 3
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("component", "event_bus")

	opts.Logger = logger

	if opts.Metrics == nil {
		opts.Metrics = metrics.NewRegistry()
	}

	return &Bus{
		registry: r,
		log:      l,
		BusOpts:  opts,
		pool:     NewWorkerPool(workers, logger),
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
		err := b.pool.Submit(NewJob(sub.ID, sub.inbox, Delivery{e, offset}, sub.policy, b.Unsubscribe, func() {
			b.Metrics.Counter("events.dropped").Inc()
		}))
		if err != nil && errors.Is(err, ErrPoolFull) {
			switch sub.policy {
			case Drop:
				b.Logger.Warn("subscriber inbox full, dropping event",
					"event_id", e.ID,
					"subscriber_id", sub.ID,
				)
				b.Metrics.Counter("events.dropped").Inc()
				continue
			case Disconnect:
				b.Logger.Warn("subscriber inbox full, disconnecting",
					"event_id", e.ID,
					"subscriber_id", sub.ID,
				)
				b.registry.Unsubscribe(sub.ID)
				b.Metrics.Counter("events.dropped").Inc()
			}
		} else {
			b.Metrics.Counter("events.delivered").Inc()
		}
	}
	b.Metrics.Counter("events.published").Inc()
	return nil
}

func (b *Bus) Subscribe(subscriberId, eventType string, fn DeliverFunc, policy SlowSubscriberPolicy) (*Subscription, error) {
	var subId string
	onError := func(d Delivery, err error) {
		delivered := false
		b.Logger.Warn("deliver failed",
			"event_id", d.event.ID,
			"subscriber_id", subId,
			"error", err,
		)
		for i := range b.Retry.MaxRetries {
			time.Sleep(b.Retry.Delay)
			if err := fn(d.event, d.offset); err == nil {
				delivered = true
				b.Metrics.Counter("deliveries.retried").Add(int64(i + 1))
				break
			}
			b.Logger.Warn("retry failed",
				"event_id", d.event.ID,
				"subscriber_id", subId,
				"retry_attempt", i,
				"error", err,
			)
		}

		if !delivered {
			b.Metrics.Counter("deliveries.retried").Add(int64(b.Retry.MaxRetries))
			if b.Dlq != nil {
				b.Logger.Error("retries exhausted, writing to DLQ",
					"event_id", d.event.ID,
					"subscriberId", subId,
				)
				b.Metrics.Counter("events.dlq").Inc()
				b.Dlq.Append(d.event, err.Error(), subId)
			}
		}
	}
	sub, err := b.registry.Subscribe(subscriberId, eventType, fn, policy, onError)
	if err != nil {
		return nil, err
	}
	b.Metrics.Gauge("subscribers.active").Inc()
	subId = sub.ID
	return sub, nil
}

func (b *Bus) Unsubscribe(id string) error {
	err := b.registry.Unsubscribe(id)
	if err == nil {
		b.Metrics.Gauge("subscribers.active").Dec()
	}
	return err
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
		b.pool.Submit(
			NewJob(s.ID, s.inbox, Delivery{e, logs.Offset()}, s.policy, b.Unsubscribe, func() {
				b.Metrics.Counter("events.dropped").Inc()
			}),
		)
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
