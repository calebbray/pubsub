package pubsub

import (
	"encoding/json"
	"time"

	eventlog "pipelines/pkg/event_log"
)

type RetryPolicy struct {
	MaxRetries int
	Delay      time.Duration
}

type DeadLetterQueue struct {
	*eventlog.Log
}

type DlqLog struct {
	Event        `json:"event"`
	Reason       string `json:"reason"`
	SubscriberId string `json:"subscriberId"`
}

type DLQEntry struct {
	Log    DlqLog
	Offset uint64
}

func (q *DeadLetterQueue) Append(e Event, reason string, subscriberId string) error {
	encoded, err := encodeDlqLog(DlqLog{
		Event:        e,
		Reason:       reason,
		SubscriberId: subscriberId,
	})
	if err != nil {
		return err
	}
	_, err = q.Log.Append(encoded)
	if err != nil {
		return err
	}
	return nil
}

func (q *DeadLetterQueue) Inspect() ([]*DLQEntry, error) {
	logs := eventlog.NewIterator(q.Log, 0)
	var entrys []*DLQEntry
	for logs.Next() {
		l, err := decodeDlqLog(logs.Data())
		if err != nil {
			return entrys, err
		}
		entrys = append(entrys, &DLQEntry{Log: l, Offset: logs.Offset()})
	}
	return entrys, nil
}

func (q *DeadLetterQueue) Replay(bus *Bus) error {
	entries, err := q.Inspect()
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if err := bus.Publish(entry.Log.Event); err != nil {
			return err
		}
	}

	return nil
}

func (q *DeadLetterQueue) Clear() error {
	if err := q.EventLogger.Truncate(0); err != nil {
		return err
	}

	q.Log.ResetSize()
	return nil
}

func encodeDlqLog(l DlqLog) ([]byte, error) {
	return json.Marshal(l)
}

func decodeDlqLog(p []byte) (DlqLog, error) {
	var l DlqLog
	if err := json.Unmarshal(p, &l); err != nil {
		return DlqLog{}, err
	}
	return l, nil
}
