package transport

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialClosedServerFails(t *testing.T) {
	s := newTestServer(t, ServerOpts{
		Logger:  LogWriter{},
		Handler: EchoHandler{},
	})
	s.Close()

	_, err := Dial(s.Addr())
	require.Error(t, err)
}

// Handler receives the right bytes and echos
func TestHandlingFrames(t *testing.T) {
	s := newTestServer(t, ServerOpts{
		Logger:  LogWriter{},
		Handler: EchoHandler{},
	})

	conn, err := Dial(s.Addr())
	require.NoError(t, err)
	defer conn.Close()

	msg := "foobar"
	require.NoError(t, WriteFrame(conn, []byte(msg)))

	response, err := ReadFrame(conn)
	require.NoError(t, err)

	assert.Equal(t, string(msg), string(response))
}

func TestHandlingMultipleFrames(t *testing.T) {
	s := newTestServer(t, ServerOpts{
		Logger:  LogWriter{},
		Handler: EchoHandler{},
	})

	conn, err := Dial(s.Addr())
	require.NoError(t, err)
	defer conn.Close()

	msg1 := "foo"
	msg2 := "bar"
	msg3 := "baz"
	require.NoError(t, WriteFrame(conn, []byte(msg1)))
	require.NoError(t, WriteFrame(conn, []byte(msg2)))
	require.NoError(t, WriteFrame(conn, []byte(msg3)))

	response, err := ReadFrame(conn)
	require.NoError(t, err)
	assert.Equal(t, string(msg1), string(response))

	response, err = ReadFrame(conn)
	require.NoError(t, err)
	assert.Equal(t, string(msg2), string(response))

	response, err = ReadFrame(conn)
	require.NoError(t, err)
	assert.Equal(t, string(msg3), string(response))
}

func TestHandlingMultipleConnections(t *testing.T) {
	s := newTestServer(t, ServerOpts{
		Logger:  LogWriter{},
		Handler: EchoHandler{},
	})

	conn1, err := Dial(s.Addr())
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := Dial(s.Addr())
	require.NoError(t, err)
	defer conn2.Close()

	conn1Msg := []byte("hello from conn 1")
	conn2Msg := []byte("hello from conn 2")

	require.NoError(t, WriteFrame(conn1, conn1Msg))
	require.NoError(t, WriteFrame(conn2, conn2Msg))

	response, err := ReadFrame(conn1)
	require.NoError(t, err)
	assert.Equal(t, conn1Msg, response)

	response, err = ReadFrame(conn2)
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

func newTestServer(t *testing.T, opts ServerOpts) *Server {
	t.Helper()
	s := NewServer(":0", opts)
	go s.Run()
	<-s.Ready()
	t.Cleanup(func() { s.Close() })
	return s
}

type errHandler struct{}

var TestErr = errors.New("TestError")

func (errHandler) HandleFrame(w io.Writer, frame []byte) error {
	return TestErr
}

type LogWriter struct{}

func (w LogWriter) Write(p []byte) (int, error) {
	fmt.Printf("%s", p)
	return len(p), nil
}
