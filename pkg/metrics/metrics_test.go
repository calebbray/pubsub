package metrics

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCounterIncrement(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("test.counter")

	c.Inc()
	c.Inc()
	c.Add(3)

	snapshot := r.Snapshot()
	assert.Equal(t, int64(5), snapshot["test.counter"])
}

func TestCounterConcurrentAccess(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("test.concurrent")

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			c.Inc()
		})
	}
	wg.Wait()

	snapshot := r.Snapshot()
	assert.Equal(t, int64(100), snapshot["test.concurrent"])
}

func TestGaugeOperations(t *testing.T) {
	r := NewRegistry()
	g := r.Gauge("test.gauge")

	g.Set(10)
	assert.Equal(t, int64(10), r.Snapshot()["test.gauge"])

	g.Inc()
	assert.Equal(t, int64(11), r.Snapshot()["test.gauge"])

	g.Dec()
	assert.Equal(t, int64(10), r.Snapshot()["test.gauge"])

	g.Set(0)
	assert.Equal(t, int64(0), r.Snapshot()["test.gauge"])
}

func TestSnapshotReturnsAllMetrics(t *testing.T) {
	r := NewRegistry()

	r.Counter("requests").Add(5)
	r.Counter("errors").Inc()
	r.Gauge("active").Set(3)

	snapshot := r.Snapshot()
	assert.Equal(t, int64(5), snapshot["requests"])
	assert.Equal(t, int64(1), snapshot["errors"])
	assert.Equal(t, int64(3), snapshot["active"])
}

func TestSameNameReturnsSameInstance(t *testing.T) {
	r := NewRegistry()

	c1 := r.Counter("hits")
	c2 := r.Counter("hits")

	c1.Inc()
	snapshot := r.Snapshot()
	assert.Equal(t, int64(1), snapshot["hits"])

	c2.Inc()
	snapshot = r.Snapshot()
	assert.Equal(t, int64(2), snapshot["hits"])
}

func TestNameConflictPanics(t *testing.T) {
	r := NewRegistry()
	r.Counter("metric")

	assert.Panics(t, func() {
		r.Gauge("metric")
	})
}
