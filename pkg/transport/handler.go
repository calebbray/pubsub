package transport

import (
	"io"
)

type Handler interface {
	HandleFrame(w io.Writer, frame []byte) error
}

type EchoHandler struct{}

func (h EchoHandler) HandleFrame(w io.Writer, frame []byte) error {
	return WriteFrame(w, frame)
}
