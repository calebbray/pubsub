package session

import (
	"errors"
	"io"
	"net"
	"time"

	"pipelines/pkg/transport"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

type Session struct {
	net.Conn
	rateLimiter *rate.Limiter
	sessionId   uuid.UUID
	createdAt   time.Time
	version     uint8
}

type RateLimitOpts struct {
	Limit     int
	BurstSize int
}

func New(conn net.Conn, rlOpts *RateLimitOpts) *Session {
	s := &Session{
		Conn:      conn,
		sessionId: uuid.New(),
		createdAt: time.Now(),
	}

	if rlOpts != nil {
		rl := rate.NewLimiter(rate.Limit(rlOpts.Limit), rlOpts.BurstSize)
		s.rateLimiter = rl
	}

	return s
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

func (s *Session) Version() int {
	return int(s.version)
}

func (s *Session) Allow() bool {
	if s.rateLimiter != nil {
		return s.rateLimiter.Allow()
	}
	return true
}

type SessionHandler struct {
	ValidToken        string
	SupportedVersions []uint8
	RateLimit         *RateLimitOpts
	Handler           transport.FrameHandler
}

func (h SessionHandler) HandleConn(conn net.Conn) {
	defer conn.Close()

	s := New(conn, h.RateLimit)
	if err := s.ServerHello(h.ValidToken, h.SupportedVersions); err != nil {
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

		if !s.Allow() {
			return
		}
		if err := h.Handler.HandleFrame(conn, frame); err != nil {
			return
		}
	}
}

func Dial(addr, clientId, token string, versions []uint8) (*Session, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	s := New(conn, nil)

	if err := s.ClientHello(clientId, token, versions); err != nil {
		return nil, err
	}

	return s, nil
}
