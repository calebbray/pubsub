package rpc

import (
	"encoding/json"
	"testing"

	"pipelines/pkg/metrics"
	"pipelines/pkg/transport"
	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminHealthHandler(t *testing.T) {
	srv := newRPCServer(t)
	c := newRPCTestClient(t, srv.Addr())
	defer c.Close()

	msg, err := c.Send("/_admin/health", nil)
	require.NoError(t, err)

	var res HealthStatus
	err = json.Unmarshal(msg.Payload, &res)
	require.NoError(t, err)

	assert.True(t, res.OK)
}

func TestAdminStatusHandler(t *testing.T) {
	srv := newRPCServer(t)

	c := newRPCTestClient(t, srv.Addr())
	defer c.Close()

	msg, err := c.Send("/_admin/status", nil)
	require.NoError(t, err)

	var res statusResponse
	err = json.Unmarshal(msg.Payload, &res)
	require.NoError(t, err)

	assert.Equal(t, 4, res.MethodCount)
	assert.True(t, res.Uptime > 0)
}

func TestAdminMetricHandler(t *testing.T) {
	srv := newRPCServer(t)

	c := newRPCTestClient(t, srv.Addr())
	defer c.Close()

	msg, err := c.Send("/_admin/metrics", nil)
	require.NoError(t, err)

	var res metricResponse
	err = json.Unmarshal(msg.Payload, &res)
	require.NoError(t, err)

	assert.Equal(t, 1, res.Counts["/_admin/metrics"])
}

func TestAdminReadyHandler(t *testing.T) {
	srv := newRPCServer(t)

	c := newRPCTestClient(t, srv.Addr())
	defer c.Close()

	msg, err := c.Send("/_admin/ready", nil)
	require.NoError(t, err)

	var res HealthStatus
	err = json.Unmarshal(msg.Payload, &res)
	require.NoError(t, err)

	// We haven't configured any subscribers so this should be false
	assert.False(t, res.OK)
}

func TestReadyEndpointReturnsTrueWithActiveSubscribers(t *testing.T) {
	srv := newRPCServer(t)

	c := newRPCTestClient(t, srv.Addr())
	defer c.Close()

	// simulate an active subscriber
	srv.Metrics.Gauge("subscribers.active").Inc()

	msg, err := c.Send("/_admin/ready", nil)
	require.NoError(t, err)

	var status HealthStatus
	require.NoError(t, json.Unmarshal(msg.Payload, &status))
	assert.True(t, status.OK)
}

func TestCustomHealthCheckerCanBeInjected(t *testing.T) {
	rpcSrv := NewServer(ServerOpts{
		HealthChecker: alwaysUnhealthyChecker{},
	})

	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: rpcSrv,
	})

	c := newRPCTestClient(t, srv.Addr())
	defer c.Close()

	msg, err := c.Send("/_admin/health", nil)
	require.NoError(t, err)

	var status HealthStatus
	require.NoError(t, json.Unmarshal(msg.Payload, &status))
	assert.False(t, status.OK)
	assert.Equal(t, "custom unhealthy", status.Message)
}

type alwaysUnhealthyChecker struct{}

func (a alwaysUnhealthyChecker) Health() HealthStatus {
	return HealthStatus{OK: false, Message: "custom unhealthy"}
}

func (a alwaysUnhealthyChecker) Ready() HealthStatus {
	return HealthStatus{OK: false, Message: "custom not ready"}
}

func newRPCServer(t *testing.T) *Server {
	t.Helper()

	metrics := metrics.NewRegistry()
	srv := NewServer(ServerOpts{Metrics: metrics})
	tr := utils.NewTestServer(t, transport.ServerOpts{
		Handler: srv,
	})

	srv.addr = tr.Addr()

	return srv
}
