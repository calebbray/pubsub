package session

import (
	"errors"
	"io"
	"net"
	"time"

	"pipelines/pkg/transport"

	"github.com/google/uuid"
)

type Session struct {
	net.Conn
	sessionId uuid.UUID
	createdAt time.Time
}

func New(conn net.Conn) *Session {
	return &Session{
		Conn:      conn,
		sessionId: uuid.New(),
		createdAt: time.Now(),
	}
}

func (s *Session) Send(msg []byte) error {
	return transport.WriteFrame(s.Conn, msg)
}

func (s *Session) Receive() ([]byte, error) {
	return transport.ReadFrame(s.Conn)
}

func (s *Session) Id() string {
	return s.sessionId.String()
}

func (s *Session) CreatedAt() time.Time {
	return s.createdAt
}

type SessionHandler struct {
	ValidToken string
	Handler    transport.FrameHandler
}

func (h SessionHandler) HandleConn(conn net.Conn) {
	defer conn.Close()

	s := New(conn)
	if err := s.ServerHandshake(h.ValidToken); err != nil {
		return
	}

	for {
		frame, err := s.Receive()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			var opErr *net.OpError
			if errors.As(err, &opErr) {
				return
			}

			return
		}

		if err := h.Handler.HandleFrame(conn, frame); err != nil {
			return
		}
	}
}

func Dial(addr, clientId, token string) (*Session, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	s := New(conn)
	if err := s.ClientHandshake(clientId, token); err != nil {
		conn.Close()
		return nil, err
	}
	return s, nil
}
