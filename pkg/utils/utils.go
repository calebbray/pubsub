package utils

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"

	eventlog "github.com/calebbray/pubsub/pkg/event_log"
	"github.com/calebbray/pubsub/pkg/transport"
)

func NewJSONLogger(w io.Writer, opts *slog.HandlerOptions) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, opts))
}

func NoOpLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func NewTestServer(t *testing.T, opts transport.ServerOpts) *transport.Server {
	t.Helper()
	s := transport.NewServer(":0", opts)
	go s.Run(func(addr string) {
		s.Logger.Info("server running...", "port", addr)
	})
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

type BlockingConnHandler struct{}

func (h BlockingConnHandler) HandleConn(conn net.Conn) {
	defer conn.Close()
	<-make(chan struct{})
}

func NewTestLog(size int64) *eventlog.Log {
	return &eventlog.Log{
		EventLogger: &TestLog{
			data: make([]byte, size),
		},
	}
}

type TestLog struct {
	data []byte
}

func (l *TestLog) ReadAt(p []byte, offset int64) (n int, err error) {
	if offset >= int64(len(l.data)) {
		return 0, io.EOF
	}

	if offset < 0 {
		return 0, fmt.Errorf("negative offset")
	}

	available := int64(len(l.data)) - offset
	if int64(len(p)) < available {
		n = len(p)
	} else {
		n = int(available)
		err = io.EOF
	}

	copy(p[:n], l.data[offset:offset+int64(n)])
	return n, err
}

func (l *TestLog) WriteAt(p []byte, offset int64) (n int, err error) {
	if offset < 0 || offset > int64(len(l.data)) {
		return 0, fmt.Errorf("invalid offset %d", offset)
	}

	remainingSpace := int64(len(l.data)) - offset
	if int64(len(p)) > remainingSpace {
		p = p[:remainingSpace]
		err = io.EOF
	}

	copy(l.data[offset:], p)
	n = len(p)
	return n, err
}

func (l *TestLog) Close() error {
	return nil
}

func (l *TestLog) Sync() error {
	return nil
}

func (l *TestLog) Truncate(size int64) error {
	newData := make([]byte, size)
	copy(newData, l.data[:size])
	l.data = newData
	return nil
}
