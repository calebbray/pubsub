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

	s1 := session.New(c1, session.SessionOpts{})
	s2 := session.New(c2, session.SessionOpts{})

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
	s := session.New(conn, session.SessionOpts{})
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

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
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

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoke", []uint8{1}, session.SessionOpts{}, "")
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

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
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

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{2, 3}, session.SessionOpts{}, "")
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

	s1, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)

	s2, err := session.Dial(
		srv.Addr(), "s2",
		"mysecrettoken", []uint8{1},
		session.SessionOpts{},
		"",
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

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)
	defer s.Close()

	for i := range 100 {
		msg := fmt.Appendf(nil, "frame-%d", i)
		require.NoError(t, s.Send(msg))
	}
}

func TestClientHeartbeatSendsPings(t *testing.T) {
	sess, _ := newHeartbeatTestSession(t)
	defer sess.Close()

	// sleep past the time out (100 ms)
	time.Sleep(101 * time.Millisecond)

	require.NoError(t, sess.Send([]byte("still alive")))
	_, err := sess.Receive()
	require.NoError(t, err)
}

func TestIdleConnectionClosesAfterTimeout(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
			RateLimit:         &session.RateLimitOpts{Limit: 5, BurstSize: 10},
			Heartbeat: session.HeartbeatConfig{
				Timeout: 100 * time.Millisecond,
			},
		},
	})

	sess, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)
	defer sess.Close()

	// sleep past the time out (100 ms)
	time.Sleep(101 * time.Millisecond)

	_, err = sess.Receive()
	require.Error(t, err)
}

func TestClientHeartbeatRoundTrip(t *testing.T) {
	sess, _ := newValidatedTestSession(t)
	defer sess.Close()

	require.NoError(t, sess.Send([]byte{byte(session.KindPing)}))
	d, err := sess.Receive()

	require.NoError(t, err)
	require.Equal(t, 1, len(d))
	assert.Equal(t, d[0], byte(session.KindPong))
}

func TestNoHeartbeat(t *testing.T) {
	sess, _ := newValidatedTestSession(t)
	defer sess.Close()

	time.Sleep(100 * time.Millisecond)
	require.NoError(t, sess.Send([]byte("hello")))
	_, err := sess.Receive()
	require.NoError(t, err)
}

func TestServerGeneratesNewSessionToken(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
			Store:             session.NewInMemoryStore(),
		},
	})

	sess, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)

	assert.NotEqual(t, "", sess.SessionToken())
}

func newHeartbeatTestSession(t *testing.T) (*session.Session, *transport.Server) {
	t.Helper()
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
			RateLimit:         &session.RateLimitOpts{Limit: 5, BurstSize: 10},
			Heartbeat: session.HeartbeatConfig{
				Interval: 50 * time.Millisecond,
				Timeout:  100 * time.Millisecond,
			},
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{
		Heartbeat: session.HeartbeatConfig{
			Interval: 50 * time.Millisecond,
			Timeout:  100 * time.Millisecond,
		},
	}, "")
	require.NoError(t, err)
	return s, srv
}

func TestNewConnectionGeneratesSessionToken(t *testing.T) {
	srv, store := newSessionStoreServer(t)
	sess, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)
	defer sess.Close()

	assert.NotEmpty(t, sess.SessionToken())
	// verify it's in the store
	_, err = store.Load(sess.SessionToken())
	require.NoError(t, err)
}

func TestReconnectWithValidTokenResumesSession(t *testing.T) {
	srv, _ := newSessionStoreServer(t)

	// first connection
	sess, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)
	token := sess.SessionToken()
	version := sess.Version()
	sess.Close()

	// reconnect with token
	sess2, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, token)
	require.NoError(t, err)
	defer sess2.Close()

	assert.Equal(t, token, sess2.SessionToken())
	assert.Equal(t, version, sess2.Version())
}

func TestReconnectWithInvalidTokenDoesFullHandshake(t *testing.T) {
	srv, _ := newSessionStoreServer(t)

	sess, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "bogus-token")
	require.NoError(t, err)
	defer sess.Close()

	// should get a new token, not the bogus one
	assert.NotEqual(t, session.SessionToken("bogus-token"), sess.SessionToken())
	assert.NotEmpty(t, sess.SessionToken())
}

func TestExpiredSessionTokenDoesFullHandshake(t *testing.T) {
	srv, _ := newSessionStorServerWithTTL(t, 50*time.Millisecond)

	sess, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)
	token := sess.SessionToken()
	sess.Close()

	time.Sleep(100 * time.Millisecond) // let token expire

	sess2, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, token)
	require.NoError(t, err)
	defer sess2.Close()

	assert.NotEqual(t, token, sess2.SessionToken())
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

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)
	return s, srv
}

func newSessionStoreServer(t *testing.T) (*transport.Server, *session.InMemoryStore) {
	t.Helper()
	store := session.NewInMemoryStore()
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
			Store:             store,
		},
	})
	return srv, store
}

func newSessionStorServerWithTTL(t *testing.T, ttl time.Duration) (*transport.Server, *session.InMemoryStore) {
	t.Helper()
	store := session.NewInMemoryStore()
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:           transport.EchoFrameHandler{},
			ValidToken:        "mysecrettoken",
			SupportedVersions: []uint8{1},
			Store:             store,
			SessionTTL:        ttl,
		},
	})
	return srv, store
}
