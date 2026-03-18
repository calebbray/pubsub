package utils

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"pipelines/pkg/transport"
)

func NewTestServer(t *testing.T, opts transport.ServerOpts) *transport.Server {
	t.Helper()
	s := transport.NewServer(":0", opts)
	go s.Run()
	<-s.Ready()
	t.Cleanup(func() { s.Close() })
	return s
}

type ErrHandler struct{}

var TestErr = errors.New("TestError")

func (ErrHandler) HandleFrame(w io.Writer, frame []byte) error {
	return TestErr
}

type LogWriter struct{}

func (w LogWriter) Write(p []byte) (int, error) {
	fmt.Printf("%s", p)
	return len(p), nil
}

type EchoConnHandler struct{}

func (EchoConnHandler) HandleConn(conn net.Conn) {
	defer conn.Close()

	for {
		frame, err := transport.ReadFrame(conn)
		if err != nil {
			return
		}
		transport.WriteFrame(conn, frame)
	}
}
