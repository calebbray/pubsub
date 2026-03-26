package pubsub

import (
	"errors"
	"log/slog"
)

type WorkerPool struct {
	workers int
	jobs    chan Job
	logger  *slog.Logger
}

type Job struct {
	inbox        chan Delivery
	delivery     Delivery
	policy       SlowSubscriberPolicy
	subId        string
	onDisconnect func(subId string) error
	onDrop       func()
}

func NewJob(
	subId string, inbox chan Delivery,
	d Delivery, policy SlowSubscriberPolicy,
	onDisconnect func(subId string) error,
	onDrop func(),
) Job {
	return Job{
		inbox:        inbox,
		delivery:     d,
		policy:       policy,
		subId:        subId,
		onDisconnect: onDisconnect,
		onDrop:       onDrop,
	}
}

func NewWorkerPool(workers int, logger *slog.Logger) *WorkerPool {
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("component", "worker_pool")
	p := &WorkerPool{
		workers: workers,
		jobs:    make(chan Job, workers),
		logger:  logger,
	}

	for range workers {
		go p.work()
	}

	return p
}

func (p *WorkerPool) work() {
	for job := range p.jobs {
		func() {
			defer func() { recover() }()
			select {
			case job.inbox <- job.delivery:
			// delivered to subscribers inbox
			default:
				switch job.policy {
				case Drop:
					p.logger.Warn("subscriber inbox full, dropping event",
						"event_id", job.delivery.event.ID,
						"subscriber_id", job.subId,
					)
					job.onDrop()
				case Disconnect:
					p.logger.Warn("subscriber inbox full, disconnecting",
						"event_id", job.delivery.event.ID,
						"subscriber_id", job.subId,
					)
					job.onDisconnect(job.subId)
				}
			}
		}()
	}
}

var ErrPoolFull = errors.New("pool full")

func (p *WorkerPool) Submit(job Job) error {
	select {
	case p.jobs <- job:
		return nil
	default:
		return ErrPoolFull
	}
}

func (p *WorkerPool) Stop() {
	close(p.jobs)
}
