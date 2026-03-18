package transport_test

import (
	"testing"

	"pipelines/pkg/transport"
	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialClosedServerFails(t *testing.T) {
	s := utils.NewTestServer(t, transport.ServerOpts{
		Logger:  utils.LogWriter{},
		Handler: utils.EchoConnHandler{},
	})
	s.Close()

	_, err := transport.Dial(s.Addr())
	require.Error(t, err)
}

// Handler receives the right bytes and echos
func TestHandlingFrames(t *testing.T) {
	s := utils.NewTestServer(t, transport.ServerOpts{
		Logger:  utils.LogWriter{},
		Handler: utils.EchoConnHandler{},
	})

	conn, err := transport.Dial(s.Addr())
	require.NoError(t, err)
	defer conn.Close()

	msg := "foobar"
	require.NoError(t, transport.WriteFrame(conn, []byte(msg)))

	response, err := transport.ReadFrame(conn)
	require.NoError(t, err)

	assert.Equal(t, string(msg), string(response))
}

func TestHandlingMultipleFrames(t *testing.T) {
	s := utils.NewTestServer(t, transport.ServerOpts{
		Logger:  utils.LogWriter{},
		Handler: utils.EchoConnHandler{},
	})

	conn, err := transport.Dial(s.Addr())
	require.NoError(t, err)
	defer conn.Close()

	msg1 := "foo"
	msg2 := "bar"
	msg3 := "baz"
	require.NoError(t, transport.WriteFrame(conn, []byte(msg1)))
	require.NoError(t, transport.WriteFrame(conn, []byte(msg2)))
	require.NoError(t, transport.WriteFrame(conn, []byte(msg3)))

	response, err := transport.ReadFrame(conn)
	require.NoError(t, err)
	assert.Equal(t, string(msg1), string(response))

	response, err = transport.ReadFrame(conn)
	require.NoError(t, err)
	assert.Equal(t, string(msg2), string(response))

	response, err = transport.ReadFrame(conn)
	require.NoError(t, err)
	assert.Equal(t, string(msg3), string(response))
}

func TestHandlingMultipleConnections(t *testing.T) {
	s := utils.NewTestServer(t, transport.ServerOpts{
		Logger:  utils.LogWriter{},
		Handler: utils.EchoConnHandler{},
	})

	conn1, err := transport.Dial(s.Addr())
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := transport.Dial(s.Addr())
	require.NoError(t, err)
	defer conn2.Close()

	conn1Msg := []byte("hello from conn 1")
	conn2Msg := []byte("hello from conn 2")

	require.NoError(t, transport.WriteFrame(conn1, conn1Msg))
	require.NoError(t, transport.WriteFrame(conn2, conn2Msg))

	response, err := transport.ReadFrame(conn1)
	require.NoError(t, err)
	assert.Equal(t, conn1Msg, response)

	response, err = transport.ReadFrame(conn2)
	require.NoError(t, err)
	assert.Equal(t, conn2Msg, response)
}

// TODO: Something in this test is funky
// func TestHandlerErrorClosesConnection(t *testing.T) {
// 	s := newTestServer(t, ServerOpts{
// 		Logger:  LogWriter{},
// 		Handler: errHandler{},
// 	})
// 	conn, err := Dial(s.Addr())
// 	require.NoError(t, err)
// 	WriteFrame(conn, []byte("this should close connection"))
// 	_, err = ReadFrame(conn)
// 	// require.Error(t, err)
// 	require.Error(t, WriteFrame(conn, []byte("connection should be closed")))
// }
