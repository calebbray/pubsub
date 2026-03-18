package server

import (
	"sync"
	"testing"

	"pipelines/pkg/rpc"
	"pipelines/pkg/session"
	"pipelines/pkg/transport"
	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthClientsCanMakeCalls(t *testing.T) {
	srv := newRPCServer(t)
	client, err := newRPCClient(t, srv.Addr(), "foobarbaz")
	require.NoError(t, err)
	defer client.Close()

	_, err = client.Send("/_admin/health", nil)
	require.NoError(t, err)
}

func TestUnauthedClientsDontMakeItToRPC(t *testing.T) {
	srv := newRPCServer(t)
	_, err := newRPCClient(t, srv.Addr(), "bad token")
	require.Error(t, err)
}

func TestRateLimitedClientsDontMakeItToRPC(t *testing.T) {
	srv := newRPCServer(t)
	c1, err := newRPCClient(t, srv.Addr(), "foobarbaz")
	require.NoError(t, err)
	c2, err := newRPCClient(t, srv.Addr(), "foobarbaz")
	require.NoError(t, err)

	defer c1.Close()
	defer c2.Close()

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 8 {
			_, err := c1.Send("/_admin/health", nil)
			assert.NoError(t, err)
		}
	})

	var c2err error
	for range 100 {
		_, c2err = c2.Send("/_admin/health", nil)
		if c2err != nil {
			break
		}
	}

	assert.Error(t, c2err, "should be rate limited")

	wg.Wait()
}

func newRPCServer(t *testing.T) *transport.Server {
	t.Helper()
	return utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			ValidToken:        "foobarbaz",
			SupportedVersions: []uint8{1},
			Handler:           rpc.NewServer(),
			RateLimit: &session.RateLimitOpts{
				BurstSize: 10,
				Limit:     5,
			},
		},
	})
}

func newRPCClient(t *testing.T, addr string, token string) (*rpc.Client, error) {
	t.Helper()
	sess, err := session.Dial(
		addr,
		"calebbray",
		token,
		[]uint8{1},
	)
	if err != nil {
		return nil, err
	}

	return rpc.NewClient(sess), err
}
