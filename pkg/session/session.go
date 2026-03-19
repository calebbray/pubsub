package session

import (
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"pipelines/pkg/transport"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

type Session struct {
	net.Conn
	SessionOpts

	rateLimiter *rate.Limiter
	sessionId   uuid.UUID
	createdAt   time.Time
	version     uint8

	closeOnce sync.Once
	quit      chan struct{}
}

type SessionOpts struct {
	Heartbeat HeartbeatConfig

	RateLimitOpts *RateLimitOpts
}

type RateLimitOpts struct {
	Limit     int
	BurstSize int
}

func New(conn net.Conn, opts SessionOpts) *Session {
	s := &Session{
		Conn:        conn,
		sessionId:   uuid.New(),
		createdAt:   time.Now(),
		SessionOpts: opts,
		quit:        make(chan struct{}),
	}

	if opts.RateLimitOpts != nil {
		rl := rate.NewLimiter(rate.Limit(opts.RateLimitOpts.Limit), opts.RateLimitOpts.BurstSize)
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

func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		close(s.quit)
		s.Conn.Close()
	})
	return nil
}

type SessionHandler struct {
	ValidToken        string
	SupportedVersions []uint8
	RateLimit         *RateLimitOpts
	Handler           transport.FrameHandler
	Heartbeat         HeartbeatConfig
}

func (h SessionHandler) HandleConn(conn net.Conn) {
	defer conn.Close()

	s := New(conn, SessionOpts{RateLimitOpts: h.RateLimit})
	if err := s.ServerHello(h.ValidToken, h.SupportedVersions); err != nil {
		return
	}

	var idleTimer *time.Timer
	if h.Heartbeat.Timeout > 0 {
		idleTimer = time.AfterFunc(h.Heartbeat.Timeout, func() {
			conn.Close()
		})
		defer idleTimer.Stop()
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

		if idleTimer != nil {
			idleTimer.Reset(h.Heartbeat.Timeout)
		}

		if len(frame) > 0 && Kind(frame[0]) == KindPing {
			s.Send([]byte{byte(KindPong)})
			continue
		}

		if !s.Allow() {
			return
		}
		if err := h.Handler.HandleFrame(conn, frame); err != nil {
			return
		}
	}
}

func Dial(addr, clientId, token string, versions []uint8, opts SessionOpts) (*Session, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	s := New(conn, opts)

	if err := s.ClientHello(clientId, token, versions); err != nil {
		return nil, err
	}

	if s.Heartbeat.Interval > 0 {
		go s.ClientHeartbeat()
	}

	return s, nil
}

func (s *Session) ClientHeartbeat() {
	ticker := time.NewTicker(s.Heartbeat.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.Send([]byte{byte(KindPing)}); err != nil {
				return
			}

			msg, err := s.Receive()
			if err != nil {
				return
			}

			if len(msg) == 0 || Kind(msg[0]) != KindPong {
				return
			}
		case <-s.quit:
			return
		}
	}
}
