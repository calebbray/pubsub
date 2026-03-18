package transport

import (
	"io"
	"net"
)

type ConnHandler interface {
	HandleConn(conn net.Conn)
}

type FrameHandler interface {
	HandleFrame(w io.Writer, frame []byte) error
}

type EchoFrameHandler struct{}

func (h EchoFrameHandler) HandleFrame(w io.Writer, frame []byte) error {
	return WriteFrame(w, frame)
}
