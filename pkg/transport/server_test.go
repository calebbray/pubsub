package transport_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"pipelines/pkg/transport"
	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDialClosedServerFails(t *testing.T) {
	s := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.EchoConnHandler{},
	})
	s.Close()

	_, err := transport.Dial(s.Addr())
	require.Error(t, err)
}

// Handler receives the right bytes and echos
func TestHandlingFrames(t *testing.T) {
	s := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.EchoConnHandler{},
		Logger: utils.NewJSONLogger(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}),
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

func TestSlowClientMissesDeadline(t *testing.T) {
	s := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.EchoConnHandler{},
		DeadlineConfig: transport.DeadlineConfig{
			ReadTimeout:  50 * time.Millisecond,
			WriteTimeout: 50 * time.Millisecond,
		},
	})

	conn, err := transport.Dial(s.Addr())
	require.NoError(t, err)
	defer conn.Close()

	time.Sleep(100 * time.Millisecond)

	_, err = transport.ReadFrame(conn)
	require.Error(t, err)
}

func TestSendJustInTime(t *testing.T) {
	s := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.EchoConnHandler{},
		DeadlineConfig: transport.DeadlineConfig{
			ReadTimeout:  500 * time.Millisecond,
			WriteTimeout: 500 * time.Millisecond,
		},
	})

	conn, err := transport.Dial(s.Addr())
	require.NoError(t, err)
	defer conn.Close()

	err = transport.WriteFrame(conn, []byte("in time"))
	require.NoError(t, err)

	frame, err := transport.ReadFrame(conn)
	require.NoError(t, err)
	assert.Equal(t, []byte("in time"), frame)
}

func TestIsTimeout(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.EchoConnHandler{},
	})

	conn, err := transport.Dial(srv.Addr())
	require.NoError(t, err)
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	time.Sleep(10 * time.Millisecond)

	_, err = transport.ReadFrame(conn)
	require.True(t, transport.IsTimeout(err))
}

func TestGracefulShutdown(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.EchoConnHandler{},
	})

	conn, err := transport.Dial(srv.Addr())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Nil(t, srv.Shutdown(ctx))

	_, err = transport.ReadFrame(conn)
	require.Error(t, err)
}

func TestShutdownTimesOut(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.BlockingConnHandler{},
	})
	_, err := transport.Dial(srv.Addr())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = srv.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDialFailsAfterShutdown(t *testing.T) {
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: utils.BlockingConnHandler{},
	})
	_, err := transport.Dial(srv.Addr())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	srv.Shutdown(ctx)
	_, err = transport.Dial(srv.Addr())
	require.Error(t, err)
}
