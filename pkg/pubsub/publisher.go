package pubsub

import (
	"encoding/json"
	"time"

	eventlog "pipelines/pkg/event_log"

	"github.com/google/uuid"
)

type DeliverFunc func(e Event) error

type Event struct {
	ID          uuid.UUID
	Type        string
	ProducerId  string
	Payload     []byte
	PublishedAt time.Time
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

type Bus struct {
	registry *Registry
	log      *eventlog.Log
}

func NewEventBus(r *Registry, l *eventlog.Log) *Bus {
	return &Bus{
		registry: r,
		log:      l,
	}
}

func (b *Bus) Publish(e Event) error {
	encoded, err := encodeEvent(e)
	if err != nil {
		return err
	}

	if _, err := b.log.Append(encoded); err != nil {
		return err
	}

	subs, _ := b.registry.GetSubscriptions(e.Type)

	for _, sub := range subs {
		if err := sub.DeliverFunc(e); err != nil {
			// do something if delivery fails?
		}
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
