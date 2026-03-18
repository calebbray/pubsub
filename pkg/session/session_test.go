package session_test

import (
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

	s1 := session.New(c1)
	s2 := session.New(c2)

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
	s := session.New(conn)
	after := time.Now()
	assert.True(t, s.CreatedAt().After(before))
	assert.True(t, s.CreatedAt().Before(after))
}

func TestHandshake(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:    transport.EchoFrameHandler{},
			ValidToken: "mysecrettoken",
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken")
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

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoke")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid token")
	assert.Nil(t, s)
}

func TestValidSessionSendAndReceive(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: session.SessionHandler{
			Handler:    transport.EchoFrameHandler{},
			ValidToken: "mysecrettoken",
		},
	})

	s, err := session.Dial(srv.Addr(), "caleb", "mysecrettoken")
	require.NoError(t, err)
	defer s.Close()
	msg := []byte("hello from the session")
	require.NoError(t, s.Send(msg), "session successfully sends message")

	echo, err := s.Receive()
	require.NoError(t, err, "session successfully receives message")

	assert.Equal(t, msg, echo, "message successfully went round trip")
}
