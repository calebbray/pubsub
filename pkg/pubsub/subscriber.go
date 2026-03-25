package pubsub

import (
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type Subscription struct {
	ID            string `json:"id"`
	SubscriberId  string `json:"subscriberId"`
	EventType     string `json:"eventType"`
	LastAckOffset uint64 `json:"lastAckOffset"`
	inbox         chan Delivery
	policy        SlowSubscriberPolicy
	onError       OnDeliveryError
	closeOnce     sync.Once
}

type OnDeliveryError func(Delivery, error)

func NewSubscription(subscriberId, eventType string,
	fn DeliverFunc, slowPolicy SlowSubscriberPolicy,
	onError OnDeliveryError,
) *Subscription {
	s := &Subscription{
		ID:           uuid.New().String(),
		SubscriberId: subscriberId,
		EventType:    eventType,
		policy:       slowPolicy,
		onError:      onError,
		inbox:        make(chan Delivery, 16),
	}

	go s.handleInbox(fn)

	return s
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

func (r *Registry) Subscribe(
	subscriberID, eventType string, fn DeliverFunc,
	policy SlowSubscriberPolicy, onError OnDeliveryError,
) (*Subscription, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if fn == nil {
		fn = DefaultDeliverFunc
	}

	s := NewSubscription(subscriberID, eventType, fn, policy, onError)
	r.byEventType[eventType] = append(r.byEventType[eventType], s)
	r.bySubscriber[subscriberID] = append(r.bySubscriber[subscriberID], s)
	r.bySubscription[s.ID] = s
	return s, nil
}

func (r *Registry) Unsubscribe(subscriptionId string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if sub, ok := r.bySubscription[subscriptionId]; ok {
		sub.closeOnce.Do(func() { close(sub.inbox) })
	}
	deleteSubscription(r.byEventType, subscriptionId)
	deleteSubscription(r.bySubscriber, subscriptionId)
	delete(r.bySubscription, subscriptionId)
	return nil
}

func (r *Registry) Restore(sub *Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if sub.inbox == nil {
		sub.inbox = make(chan Delivery, 16)
	}

	r.byEventType[sub.EventType] = append(r.byEventType[sub.EventType], sub)
	r.bySubscriber[sub.SubscriberId] = append(r.bySubscriber[sub.SubscriberId], sub)
	r.bySubscription[sub.ID] = sub
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

	sub.LastAckOffset = offset
	return nil
}

func (r *Registry) Reattach(
	subscriberId, eventType string, fn DeliverFunc,
	policy SlowSubscriberPolicy, onError func(Delivery, error),
) error {
	subs, ok := r.bySubscriber[subscriberId]
	if !ok {
		return fmt.Errorf("can not reattach")
	}

	for _, sub := range subs {
		if sub.EventType == eventType {
			sub.inbox = make(chan Delivery, 16)
			sub.policy = policy
			sub.onError = onError

			go sub.handleInbox(fn)
			return nil
		}
	}
	return fmt.Errorf("subscription for subscriber %s and event %s not found", subscriberId, eventType)
}

func (s *Subscription) handleInbox(fn DeliverFunc) {
	for d := range s.inbox {
		if err := fn(d.event, d.offset); err != nil && s.onError != nil {
			s.onError(d, err)
		}
	}
}

func DefaultDeliverFunc(e Event, offset uint64) error {
	fmt.Println(offset, e)
	return nil
}

type SlowSubscriberPolicy int

const (
	Drop SlowSubscriberPolicy = iota
	Disconnect
)

type Delivery struct {
	event  Event
	offset uint64
}
