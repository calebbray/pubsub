package session

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/calebbray/pubsub/pkg/transport"

	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

type Session struct {
	net.Conn
	SessionOpts

	sessionToken SessionToken
	rateLimiter  *rate.Limiter
	sessionId    uuid.UUID
	createdAt    time.Time
	version      uint8
	clientId     string

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

func (s *Session) SessionToken() SessionToken {
	return s.sessionToken
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
	Store             SessionStore
	SessionTTL        time.Duration
	Logger            *slog.Logger
}

func (h SessionHandler) HandleConn(conn net.Conn) {
	defer conn.Close()
	if h.Logger == nil {
		h.Logger = slog.Default()
	} else {
		h.Logger = h.Logger.With("component", "session")
	}

	s := New(conn, SessionOpts{RateLimitOpts: h.RateLimit})

	if err := h.handleServerHello(s); err != nil {
		return
	}

	h.Logger.Info("successful session handshake", "clientId", s.clientId, "version", s.version)

	var idleTimer *time.Timer
	if h.Heartbeat.Timeout > 0 {
		idleTimer = time.AfterFunc(h.Heartbeat.Timeout, func() {
			h.Logger.Warn("idle timeout", "session", s.Id())
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

func (h SessionHandler) handleServerHello(s *Session) error {
	req, err := s.validateServerHandshake(h.ValidToken)
	if err != nil {
		h.Logger.Warn("invalid session handshake", "session", s.Id(), "reason", err)
		return err
	}

	s.clientId = req.ClientID

	res := &HandshakeResponse{}

	var state *SessionState
	if h.Store != nil {
		state, _ = h.lookupSession(req.ResumeToken)
	}

	if state != nil {
		s.version = uint8(state.Version)
		res.Token = req.ResumeToken
		res.Version = uint8(state.Version)
	} else {
		res.Token = SessionToken(uuid.New().String())
	}

	res.Success = true
	res.Resumed = state != nil

	b, err := json.Marshal(res)
	if err != nil {
		return err
	}

	if err := s.Send(b); err != nil {
		return err
	}

	if !res.Resumed {
		if err := s.ServerNegotiate(h.SupportedVersions); err != nil {
			if errors.Is(err, ErrUnsupportedVersion) {
				h.Logger.Warn("invalid version protocol", "session", s.Id(), "reason", err)
			}
			return err
		}
	}

	if h.Store != nil {
		h.Store.Save(res.Token, SessionState{
			ClientId: req.ClientID,
			Version:  s.Version(),
			LastSeen: time.Now(),
		})
	}

	return nil
}

func (h SessionHandler) lookupSession(t SessionToken) (*SessionState, error) {
	state, err := h.Store.Load(t)
	if errors.Is(err, ErrSessionNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	if state.Expired(h.SessionTTL) {
		h.Store.Delete(t)
		return nil, nil
	}

	state.LastSeen = time.Now()
	h.Store.Save(t, *state)
	return state, nil
}

func Dial(addr, clientId, token string, versions []uint8, opts SessionOpts, resumeToken SessionToken) (*Session, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	s := New(conn, opts)
	s.clientId = clientId

	t, err := s.ClientHello(clientId, token, versions, resumeToken)
	if err != nil {
		return nil, err
	}
	s.sessionToken = t

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

type SessionToken string

type SessionState struct {
	ClientId string
	Version  int
	LastSeen time.Time
}

func (s *SessionState) Expired(ttl time.Duration) bool {
	if ttl == 0 {
		return false
	}
	return time.Since(s.LastSeen) > ttl
}

type SessionStore interface {
	Save(token SessionToken, state SessionState) error
	Load(token SessionToken) (*SessionState, error)
	Delete(token SessionToken) error
}

type InMemoryStore struct {
	mu       sync.Mutex
	sessions map[SessionToken]*SessionState
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		sessions: make(map[SessionToken]*SessionState),
	}
}

func (s *InMemoryStore) Save(token SessionToken, state SessionState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[token] = &state
	return nil
}

var ErrSessionNotFound = errors.New("session not found")

func (s *InMemoryStore) Load(token SessionToken) (*SessionState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.sessions[token]
	if !ok {
		return nil, ErrSessionNotFound
	}
	return state, nil
}

func (s *InMemoryStore) Delete(token SessionToken) error {
	s.mu.Lock()
	delete(s.sessions, token)
	s.mu.Unlock()
	return nil
}
