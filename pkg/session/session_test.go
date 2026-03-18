package session_test

import (
	"fmt"
	"testing"
	"time"

	"pipelines/pkg/session"
	"pipelines/pkg/transport"
	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionsAreUnique(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.EchoConnHandler{},
	})

	c1, err := transport.Dial(srv.Addr())
	require.NoError(t, err)
	defer c1.Close()

	c2, err := transport.Dial(srv.Addr())
	require.NoError(t, err)
	defer c2.Close()

	s1 := session.New(c1, nil)
	s2 := session.New(c2, nil)

	assert.NotEqual(t, s1.Id(), s2.Id(), "Sessions should have different IDs")
}

func TestSessionCreatedTime(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.EchoConnHandler{},
	})

	conn, err := transport.Dial(srv.Addr())
	require.NoError(t, err)
	defer conn.Close()

	before := time.Now()
	s := session.New(conn, nil)
	after := time.Now()
	assert.True(t, s.CreatedAt().After(before))
	assert.True(t, s.CreatedAt().Before(after))
}

func TestHandshakeAndCapabilities(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1})
	require.NoError(t, err)
	defer s.Close()
	require.NotNil(t, s)
}

func TestInvalidHandshake(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:    transport.EchoFrameHandler{},
			ValidToken: "mysecrettoken",
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoke", []uint8{1})
	require.Error(t, err, "invalid token should fail")
	assert.Contains(t, err.Error(), "invalid token")
	assert.Nil(t, s)
}

func TestInvalidCapabilities(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{2, 3},
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1})
	require.Error(t, err, "non supported client version should fail")
	assert.Contains(t, err.Error(), "unsupported protocol version")
	assert.Nil(t, s)
}

func TestServerPicksUpLatestCapability(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{2, 3},
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{2, 3})
	require.NoError(t, err)
	defer s.Close()
	assert.Equal(t, 3, s.Version())
}

func TestSendingMessagesAfterSessionValidates(t *testing.T) {
	s, _ := newValidatedTestSession(t)
	defer s.Close()
	msg := []byte("hello from the session")
	require.NoError(t, s.Send(msg), "session successfully sends message")

	echo, err := s.Receive()
	require.NoError(t, err, "session successfully receives message")

	assert.Equal(t, msg, echo, "message successfully went round trip")
}

func TestFramesUnderRateLimit(t *testing.T) {
	s, _ := newValidatedTestSession(t)
	defer s.Close()

	// default session comes with 5 rps and burst 10
	// only going to be able to get 8 because we send two
	// messages as part of the handshake
	for i := range 8 {
		msg := fmt.Appendf(nil, "frame-%d", i)
		assert.NoError(t, s.Send(msg))
	}
}

func TestFramesOverRateLimit(t *testing.T) {
	s, _ := newValidatedTestSession(t)
	defer s.Close()

	var err error

	for i := range 100 {
		msg := fmt.Appendf(nil, "frame-%d", i)
		err = s.Send(msg)
		if err != nil {
			break
		}
		_, err = s.Receive()
		if err != nil {
			break
		}
	}

	assert.Error(t, err)
}

func TestMultipleSessionRateLimits(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
			RateLimit:         &session.RateLimitOpts{Limit: 5, BurstSize: 10},
		},
	})

	s1, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1})
	require.NoError(t, err)

	s2, err := session.Dial(
		srv.Addr(), "s2",
		"mysecrettoken", []uint8{1},
	)

	require.NoError(t, err)
	defer s1.Close()
	defer s2.Close()

	// session 1 has no errors
	go func() {
		for i := range 8 {
			msg := fmt.Appendf(nil, "frame-%d", i)
			assert.NoError(t, s1.Send(msg))
			_, err := s1.Receive()
			assert.NoError(t, err)
		}
	}()

	// session 2 hits rate limit
	for i := range 100 {
		msg := fmt.Appendf(nil, "frame-%d", i)
		err = s2.Send(msg)
		if err != nil {
			break
		}
		_, err = s2.Receive()
		if err != nil {
			break
		}
	}

	assert.Error(t, err)
}

func TestUnlimitedFrameRateLimit(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1})
	require.NoError(t, err)
	defer s.Close()

	for i := range 100 {
		msg := fmt.Appendf(nil, "frame-%d", i)
		require.NoError(t, s.Send(msg))
	}
}

func newValidatedTestSession(t *testing.T) (*session.Session, *transport.Server) {
	t.Helper()
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
			RateLimit:         &session.RateLimitOpts{Limit: 5, BurstSize: 10},
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1})
	require.NoError(t, err)
	return s, srv
}
