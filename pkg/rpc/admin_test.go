package rpc

import (
	"encoding/json"
	"testing"

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

	var res healthResponse
	err = json.Unmarshal(msg.Payload, &res)
	require.NoError(t, err)

	assert.Equal(t, "ok", res.Status)
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

	assert.Equal(t, 3, res.MethodCount)
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

func newRPCServer(t *testing.T) *Server {
	t.Helper()

	srv := NewServer(ServerOpts{})
	tr := utils.NewTestServer(t, transport.ServerOpts{
		Handler: srv,
	})

	srv.addr = tr.Addr()

	return srv
}
