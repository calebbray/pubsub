package registry

import (
	"encoding/json"
	"io"
	"os"

	"github.com/calebbray/pubsub/pkg/pubsub"
)

type Store interface {
	Save(sub *pubsub.Subscription) error
	Delete(subscriptionId string) error
	Load() ([]*pubsub.Subscription, error)
}

type PersistentRegistry struct {
	*pubsub.Registry
	Store
}

func NewPersistentRegistry(s Store) (*PersistentRegistry, error) {
	subs, err := s.Load()
	if err != nil {
		return nil, err
	}
	r := pubsub.NewRegistry()

	for _, sub := range subs {
		r.Restore(sub)
	}

	pr := &PersistentRegistry{
		Registry: r,
		Store:    s,
	}

	return pr, nil
}

func (r *PersistentRegistry) Subscribe(subscriberId, eventType string, fn pubsub.DeliverFunc, policy pubsub.SlowSubscriberPolicy, onError pubsub.OnDeliveryError) (*pubsub.Subscription, error) {
	s, err := r.Registry.Subscribe(subscriberId, eventType, fn, policy, onError)
	if err != nil {
		return nil, err
	}

	if err := r.Save(s); err != nil {
		return nil, err
	}

	return s, err
}

func (r *PersistentRegistry) Unsubscribe(subscriptionId string) error {
	if err := r.Registry.Unsubscribe(subscriptionId); err != nil {
		return err
	}

	return r.Delete(subscriptionId)
}

func (r *PersistentRegistry) Ack(subscriptionId string, offset uint64) error {
	if err := r.Registry.Ack(subscriptionId, offset); err != nil {
		return err
	}
	sub := r.Registry.GetSubscriptionById(subscriptionId)
	if sub == nil {
		return pubsub.ErrSubscriptionNotFound
	}

	return r.Save(sub)
}

type FileStore struct {
	fd *os.File
}

func NewFileStore(path string) (*FileStore, error) {
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	return &FileStore{fd}, nil
}

func (s *FileStore) Save(sub *pubsub.Subscription) error {
	subs, err := s.Load()
	if err != nil {
		return err
	}

	updated := false
	for i, existing := range subs {
		if existing.ID == sub.ID {
			subs[i] = sub
			updated = true
			break
		}
	}

	if !updated {
		subs = append(subs, sub)
	}

	return writeJson(s.fd, &subs)
}

func (s *FileStore) Delete(subscriptionId string) error {
	subs, err := s.Load()
	if err != nil {
		return err
	}

	for i, sub := range subs {
		if sub.ID == subscriptionId {
			subs[i] = subs[len(subs)-1]
			subs[len(subs)-1] = nil
			subs = subs[:len(subs)-1]
			break
		}
	}

	return writeJson(s.fd, &subs)
}

func (s *FileStore) Load() ([]*pubsub.Subscription, error) {
	data, err := os.ReadFile(s.fd.Name())
	if err != nil {
		return nil, err
	}

	var subs []*pubsub.Subscription
	if len(data) > 0 {
		if err := json.Unmarshal(data, &subs); err != nil {
			return nil, err
		}
	}

	return subs, nil
}

func writeJson(f *os.File, v any) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := f.Truncate(0); err != nil {
		return err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	if err := json.NewEncoder(f).Encode(v); err != nil {
		return err
	}

	return f.Sync()
}
