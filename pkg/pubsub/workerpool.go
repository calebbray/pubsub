package pubsub

import "errors"

type WorkerPool struct {
	workers int
	jobs    chan Job
}

type Job struct {
	inbox        chan delivery
	delivery     delivery
	policy       SlowSubscriberPolicy
	subId        string
	onDisconnect func(subId string) error
}

func NewJob(
	subId string, inbox chan delivery,
	d delivery, policy SlowSubscriberPolicy,
	onDisconnect func(subId string) error,
) Job {
	return Job{
		inbox:        inbox,
		delivery:     d,
		policy:       policy,
		subId:        subId,
		onDisconnect: onDisconnect,
	}
}

func NewWorkerPool(workers int) *WorkerPool {
	p := &WorkerPool{
		workers: workers,
		jobs:    make(chan Job, workers),
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
				case Disconnect:
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
