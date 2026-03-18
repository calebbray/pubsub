package pubsub

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

type Subscription struct {
	ID           string
	SubscriberId string
	EventType    string
}

func NewSubscription(subscriberId, eventType string) *Subscription {
	return &Subscription{
		ID:           uuid.New().String(),
		SubscriberId: subscriberId,
		EventType:    eventType,
	}
}

type Registry struct {
	mu           sync.RWMutex
	bySubscriber map[string][]*Subscription
	byEventType  map[string][]*Subscription
}

func NewRegistry() *Registry {
	return &Registry{
		bySubscriber: make(map[string][]*Subscription),
		byEventType:  make(map[string][]*Subscription),
	}
}

func (r *Registry) Subscribe(subscriberID, eventType string) (*Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	s := NewSubscription(subscriberID, eventType)
	r.byEventType[eventType] = append(r.byEventType[eventType], s)
	r.bySubscriber[subscriberID] = append(r.bySubscriber[subscriberID], s)
	return s, nil
}

func (r *Registry) Unsubscribe(subscriptionId string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	deleteSubscription(r.byEventType, subscriptionId)
	deleteSubscription(r.bySubscriber, subscriptionId)
	return nil
}

func deleteSubscription(m map[string][]*Subscription, id string) {
	for k, subs := range m {
		for i, s := range subs {
			if s.ID == id {
				subs[i] = subs[len(subs)-1]
				subs[len(subs)-1] = nil
				m[k] = subs[:len(subs)-1]
				return
			}
		}
	}
}

func (r *Registry) GetSubscriptions(eventType string) ([]*Subscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	et, ok := r.byEventType[eventType]
	if !ok {
		return []*Subscription{}, nil
	}

	return et, nil
}

func (r *Registry) GetSubscriberSubscriptions(subscriberID string) ([]*Subscription, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	et, ok := r.bySubscriber[subscriberID]
	if !ok {
		return []*Subscription{}, nil
	}

	return et, nil
}
