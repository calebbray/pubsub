package transport

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerDialAndReceive(t *testing.T) {
	s := newTestServer(t)

	conn, err := Dial(s.Addr())
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, WriteFrame(conn, []byte("Hello World!")))
}

func TestFramesReceivedInOrder(t *testing.T) {
	s := newTestServer(t)

	conn, err := Dial(s.Addr())
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, WriteFrame(conn, []byte("first frame")))
	require.NoError(t, WriteFrame(conn, []byte("second frame")))
}

func TestMultipleConnections(t *testing.T) {
	s := newTestServer(t)

	conn1, err := Dial(s.Addr())
	require.NoError(t, err)
	defer conn1.Close()

	conn2, err := Dial(s.Addr())
	require.NoError(t, err)
	defer conn2.Close()

	require.NoError(t, WriteFrame(conn1, []byte("first conn")))
	require.NoError(t, WriteFrame(conn2, []byte("second conn")))
}

func TestDialClosedServerFails(t *testing.T) {
	s := newTestServer(t)
	s.Close()

	_, err := Dial(s.Addr())
	require.Error(t, err)
}

func newTestServer(t *testing.T) *Server {
	t.Helper()
	s := NewServer(":0", ServerOpts{Logger: LogWriter{}})
	go s.Run()
	<-s.Ready()
	t.Cleanup(func() { s.Close() })
	return s
}

type LogWriter struct{}

func (w LogWriter) Write(p []byte) (int, error) {
	fmt.Printf("%s", p)
	return len(p), nil
}
