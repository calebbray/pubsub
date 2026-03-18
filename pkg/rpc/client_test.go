package rpc

import (
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	"pipelines/pkg/transport"
	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestResponseRPC(t *testing.T) {
	s := newRPCTestServer(t)
	c := newRPCTestClient(t, s.Addr())
	defer c.Close()

	method := "/foo"
	payload := []byte("Hello, World!")

	msg, err := c.Send(method, payload)
	require.NoError(t, err)

	assert.Equal(t, 1, int(msg.ReqId))
	assert.Equal(t, KindResponse, msg.Kind)
	assert.Equal(t, method, msg.Method)
	assert.Equal(t, payload, msg.Payload)
}

func TestRPCConcurrentClientRequests(t *testing.T) {
	s := newRPCTestServer(t)
	c := newRPCTestClient(t, s.Addr())
	defer c.Close()

	method := "/foo"
	payload := []byte("Hello, World!")

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)

		go func(x int) {
			defer wg.Done()
			method := fmt.Sprintf("%s/%d", method, x)
			msg, err := c.Send(method, payload)
			require.NoError(t, err)
			assert.Equal(t, method, msg.Method)
		}(i)
	}

	wg.Wait()
}

func TestRPCClosePendingRequestErrors(t *testing.T) {
	block := make(chan struct{})
	received := make(chan struct{})
	srv := utils.NewTestServer(t, transport.ServerOpts{
		Handler: EchoRPCConnHandler{
			Handler: BlockingHandler{block: block, received: received},
		},
	})

	c := newRPCTestClient(t, srv.Addr())

	errCh := make(chan error, 1)
	go func() {
		_, err := c.Send("/foo", []byte("hello"))
		errCh <- err
	}()

	<-received
	c.Close()

	err := <-errCh
	require.Error(t, err)
	close(block)
}

func TestRPCClientClose(t *testing.T) {
	s := newRPCTestServer(t)
	c := newRPCTestClient(t, s.Addr())

	method := "/foo"
	payload := []byte("Hello, World!")

	_, err := c.Send(method, payload)
	require.NoError(t, err)

	c.Close()

	_, err = c.Send(method, payload)
	require.Error(t, err)
}

func newRPCTestClient(t *testing.T, serverAddr string) *Client {
	t.Helper()
	conn, err := transport.Dial(serverAddr)
	require.NoError(t, err)

	return NewClient(conn)
}

func newRPCTestServer(t *testing.T) *transport.Server {
	t.Helper()
	return utils.NewTestServer(t, transport.ServerOpts{
		Handler: EchoRPCConnHandler{
			Handler: EchoRPCFrameHandler{},
		},
	})
}

type EchoRPCConnHandler struct {
	Handler transport.FrameHandler
}

func (h EchoRPCConnHandler) HandleConn(conn net.Conn) {
	defer conn.Close()

	for {
		frame, err := transport.ReadFrame(conn)
		if err != nil {
			return
		}

		if err := h.Handler.HandleFrame(conn, frame); err != nil {
			return
		}
	}
}

type EchoRPCFrameHandler struct{}

func (h EchoRPCFrameHandler) HandleFrame(w io.Writer, frame []byte) error {
	msg, err := Decode(frame)
	if err != nil {
		return err
	}

	msg.Kind = KindResponse
	return transport.WriteFrame(w, Encode(msg))
}

type BlockingHandler struct {
	block    chan struct{}
	received chan struct{}
}

func (h BlockingHandler) HandleFrame(w io.Writer, frame []byte) error {
	close(h.received)
	<-h.block
	return nil
}
