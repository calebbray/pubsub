package rpc

import (
	"fmt"
	"net"

	"pipelines/pkg/transport"
)

type HandlerFunc func(payload []byte) ([]byte, error)

type Server struct {
	handlers map[string]HandlerFunc
}

func NewServer() *Server {
	return &Server{
		handlers: make(map[string]HandlerFunc),
	}
}

func (s *Server) HandleConn(conn net.Conn) {
	defer conn.Close()
	for {
		frame, err := transport.ReadFrame(conn)
		if err != nil {
			return
		}

		msg, err := Decode(frame)
		if err != nil {
			msg.Kind = KindError
			msg.Payload = []byte("internal server error")
			transport.WriteFrame(conn, Encode(msg))
			// do something with the error?
			continue
		}

		fn, ok := s.handlers[msg.Method]
		if !ok {
			msg.Kind = KindError
			msg.Payload = []byte("no route associated with given method")
			transport.WriteFrame(conn, Encode(msg))
			continue
		}

		response, err := fn(msg.Payload)
		if err != nil {
			msg.Kind = KindError
			msg.Payload = fmt.Appendf(nil, "%s", err)
			transport.WriteFrame(conn, Encode(msg))
			continue
		}

		msg.Kind = KindResponse
		msg.Payload = response

		if err := transport.WriteFrame(conn, Encode(msg)); err != nil {
			continue
		}
	}
}

func (s *Server) Register(method string, fn HandlerFunc) {
	s.handlers[method] = fn
}
