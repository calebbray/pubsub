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

type dlqLog struct {
	Event        `json:"event"`
	Reason       string `json:"reason"`
	SubscriberId string `json:"subscriberId"`
}

func (q *DeadLetterQueue) Append(e Event, reason string, subscriberId string) error {
	encoded, err := encodeDlqLog(dlqLog{
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

func encodeDlqLog(l dlqLog) ([]byte, error) {
	return json.Marshal(l)
}

func decodeDlqLog(p []byte) (dlqLog, error) {
	var l dlqLog
	if err := json.Unmarshal(p, &l); err != nil {
		return dlqLog{}, err
	}
	return l, nil
}
