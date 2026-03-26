package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type MetricsProvider interface {
	Counter(name string) Counter
	Gauge(name string) Gauger
}

type Counter interface {
	Inc()
	Add(n int64)
}

type Gauger interface {
	Set(n int64)
	Inc()
	Dec()
}

type Registry struct {
	mu       sync.RWMutex
	counters map[string]*CountMetric
	gauges   map[string]*GaugeMetric
}

func NewRegistry() *Registry {
	return &Registry{
		counters: make(map[string]*CountMetric),
		gauges:   make(map[string]*GaugeMetric),
	}
}

func (r *Registry) Snapshot() map[string]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]int64)
	for name, c := range r.counters {
		out[name] = c.Value()
	}
	for name, g := range r.gauges {
		out[name] = g.Value()
	}
	return out
}

func (r *Registry) Counter(name string) Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.gauges[name]; ok {
		panic(fmt.Sprintf("metric %q already registered as gauge", name))
	}
	if c, ok := r.counters[name]; ok {
		return c
	}

	c := &CountMetric{name: name}
	r.counters[name] = c
	return c
}

func (r *Registry) Gauge(name string) Gauger {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.counters[name]; ok {
		panic(fmt.Sprintf("metric %q already registered as counter", name))
	}
	if g, ok := r.gauges[name]; ok {
		return g
	}

	g := &GaugeMetric{name: name}
	r.gauges[name] = g
	return g
}

type CountMetric struct {
	name  string
	count atomic.Int64
}

func (c *CountMetric) Inc()         { c.count.Add(1) }
func (c *CountMetric) Add(n int64)  { c.count.Add(n) }
func (c *CountMetric) Value() int64 { return c.count.Load() }

type GaugeMetric struct {
	name  string
	value atomic.Int64
}

func (g *GaugeMetric) Set(n int64)  { g.value.Store(n) }
func (g *GaugeMetric) Inc()         { g.value.Add(1) }
func (g *GaugeMetric) Dec()         { g.value.Add(-1) }
func (g *GaugeMetric) Value() int64 { return g.value.Load() }
