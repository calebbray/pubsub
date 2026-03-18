package rpc

import (
	"errors"
	"testing"

	"pipelines/pkg/transport"
	"pipelines/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func handlerLogger(t *testing.T, h HandlerFunc) HandlerFunc {
	return func(payload []byte) ([]byte, error) {
		t.Log(string(payload))
		return h(payload)
	}
}

func testHandler(payload []byte) ([]byte, error) {
	return []byte{4, 2, 0, 6, 9}, nil
}

func testHandler2(payload []byte) ([]byte, error) {
	return []byte("other handler"), nil
}

var errHandlerErr = errors.New("handler error")

func errorHandler(payload []byte) ([]byte, error) {
	return nil, errHandlerErr
}

func TestRegisterHandlerAndCallIt(t *testing.T) {
	rpcServer := NewServer()
	rpcServer.Register("/foo", handlerLogger(t, testHandler))
	rpcServer.Register("/bar", handlerLogger(t, testHandler2))

	transportServer := utils.NewTestServer(t, transport.ServerOpts{
		Handler: rpcServer,
	})

	c := newRPCTestClient(t, transportServer.Addr())
	defer c.Close()

	msg, err := c.Send("/foo", []byte("Sending a foo rpc request"))
	require.NoError(t, err)
	assert.Equal(t, []byte{4, 2, 0, 6, 9}, msg.Payload)

	msg, err = c.Send("/bar", []byte("Sending a foo rpc request"))
	require.NoError(t, err)
	assert.Equal(t, []byte("other handler"), msg.Payload)
}

func TestUnregisteredHandlerReturnsError(t *testing.T) {
	rpcServer := NewServer()

	transportServer := utils.NewTestServer(t, transport.ServerOpts{
		Handler: rpcServer,
	})

	c := newRPCTestClient(t, transportServer.Addr())
	defer c.Close()

	msg, err := c.Send("/foo", []byte("Sending a foo rpc request"))
	require.NoError(t, err)

	assert.Equal(t, []byte("no route associated with given method"), msg.Payload)
	assert.Equal(t, KindError, msg.Kind)
}

func TestHandlerReturnsError(t *testing.T) {
	rpcServer := NewServer()
	rpcServer.Register("/foo", errorHandler)

	transportServer := utils.NewTestServer(t, transport.ServerOpts{
		Handler: rpcServer,
	})

	c := newRPCTestClient(t, transportServer.Addr())
	defer c.Close()

	msg, err := c.Send("/foo", []byte("Sending a foo rpc request"))
	require.NoError(t, err)

	assert.Equal(t, []byte(errHandlerErr.Error()), msg.Payload)
	assert.Equal(t, KindError, msg.Kind)
}
