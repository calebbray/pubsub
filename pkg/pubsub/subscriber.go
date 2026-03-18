package pubsub

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type Subscription struct {
	ID            string
	SubscriberId  string
	EventType     string
	DeliverFunc   DeliverFunc
	lastAckOffset uint64
}

func NewSubscription(subscriberId, eventType string, fn DeliverFunc) *Subscription {
	return &Subscription{
		ID:           uuid.New().String(),
		SubscriberId: subscriberId,
		EventType:    eventType,
		DeliverFunc:  fn,
	}
}

type Registry struct {
	mu             sync.RWMutex
	bySubscriber   map[string][]*Subscription
	byEventType    map[string][]*Subscription
	bySubscription map[string]*Subscription
}

func NewRegistry() *Registry {
	return &Registry{
		bySubscriber:   make(map[string][]*Subscription),
		byEventType:    make(map[string][]*Subscription),
		bySubscription: make(map[string]*Subscription),
	}
}

func (r *Registry) Subscribe(subscriberID, eventType string, fn DeliverFunc) (*Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if fn == nil {
		fn = DefaultDeliverFunc
	}

	s := NewSubscription(subscriberID, eventType, fn)
	r.byEventType[eventType] = append(r.byEventType[eventType], s)
	r.bySubscriber[subscriberID] = append(r.bySubscriber[subscriberID], s)
	r.bySubscription[s.ID] = s
	return s, nil
}

func (r *Registry) Unsubscribe(subscriptionId string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	deleteSubscription(r.byEventType, subscriptionId)
	deleteSubscription(r.bySubscriber, subscriptionId)
	delete(r.bySubscription, subscriptionId)
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

func (r *Registry) GetSubscriptionById(subscriptionId string) *Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bySubscription[subscriptionId]
}

func (r *Registry) Ack(subscriptionId string, offset uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub, ok := r.bySubscription[subscriptionId]
	if !ok {
		return ErrSubscriptionNotFound
	}

	sub.lastAckOffset = offset
	return nil
}

func DefaultDeliverFunc(e Event, offset uint64) error {
	fmt.Println(offset, e)
	return nil
}
