package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

type Server struct {
	ServerOpts

	addr string
	ln   net.Listener
	wg   sync.WaitGroup

	tracker *connTracker

	ready chan struct{}
}

type ServerOpts struct {
	DeadlineConfig
	Handler    ConnHandler
	Logger     *slog.Logger
	ValidToken string
}

type DeadlineConfig struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func NewServer(addr string, opts ServerOpts) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("component", "transport")
	opts.Logger = logger
	s := &Server{
		addr:       addr,
		ready:      make(chan struct{}),
		tracker:    newConnTracker(),
		ServerOpts: opts,
	}

	return s
}

func (s *Server) Run(cb func(addr string)) error {
	if err := s.Listen(cb); err != nil {
		return fmt.Errorf("error while listening at (%s): %w", s.addr, err)
	}

	return nil
}

func (s *Server) Listen(cb func(addr string)) error {
	var err error
	s.ln, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("could not open listener (%s): %w", s.addr, err)
	}

	close(s.ready)
	if cb != nil {
		cb(s.ln.Addr().String())
	}

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.Logger.Error("error accepting connection", "error", err)
			continue
		}

		s.wg.Add(1)
		go s.handleConn(conn)

	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.Logger.Info("shutting down")
	s.ln.Close()
	s.tracker.closeAll()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.Logger.Info("server shutdown gracefully")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return s.Shutdown(ctx)
}

func (s *Server) Ready() <-chan struct{} {
	return s.ready
}

func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

func Dial(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}

func (s *Server) handleConn(conn net.Conn) {
	defer s.wg.Done()
	s.Logger.Debug("accepted new connection", "from", conn.RemoteAddr().String())

	if s.ReadTimeout > 0 || s.WriteTimeout > 0 {
		conn = &connWithDeadline{
			Conn:         conn,
			readTimeout:  s.ReadTimeout,
			writeTimeout: s.WriteTimeout,
		}
	}

	s.tracker.add(conn)

	defer func() {
		s.tracker.remove(conn)
		s.Logger.Debug("connection closed", "from", conn.RemoteAddr().String())
		conn.Close()
	}()

	s.Handler.HandleConn(conn)
}

func IsTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

type connWithDeadline struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c connWithDeadline) Read(p []byte) (int, error) {
	if c.readTimeout > 0 {
		c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}
	return c.Conn.Read(p)
}

func (c connWithDeadline) Write(p []byte) (int, error) {
	if c.writeTimeout > 0 {
		c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	return c.Conn.Write(p)
}

type connTracker struct {
	mu          sync.Mutex
	connections map[net.Conn]struct{}
}

func newConnTracker() *connTracker {
	return &connTracker{
		connections: make(map[net.Conn]struct{}),
	}
}

func (t *connTracker) add(conn net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.connections[conn] = struct{}{}
}

func (t *connTracker) remove(conn net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.connections, conn)
}

// prevents race conditions with handleConn deleting from servers conn map.
// lock, copy, unlock, close pattern.
func (t *connTracker) closeAll() {
	t.mu.Lock()
	conns := make([]net.Conn, 0, len(t.connections))
	for conn := range t.connections {
		conns = append(conns, conn)
	}
	t.mu.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}
