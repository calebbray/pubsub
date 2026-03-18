package transport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	// "pipelines/pkg/session"
)

type Server struct {
	ServerOpts

	addr string
	ln   net.Listener
	wg   sync.WaitGroup

	ready chan struct{}
}

type ServerOpts struct {
	Handler    ConnHandler
	Logger     io.Writer
	ValidToken string
}

func NewServer(addr string, opts ServerOpts) *Server {
	s := &Server{addr: addr, ServerOpts: opts, ready: make(chan struct{})}

	return s
}

func (s *Server) Run() error {
	if err := s.Listen(); err != nil {
		return fmt.Errorf("error while listening at (%s): %w", s.addr, err)
	}

	return nil
}

func (s *Server) Listen() error {
	var err error
	s.ln, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("could not open listener (%s): %w", s.addr, err)
	}

	close(s.ready)

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			// eventually this could maybe be some kind of slog.Error
			return fmt.Errorf("error accepting connection: %w", err)
		}

		s.wg.Add(1)
		go s.handleConn(conn)

	}
}

func (s *Server) Close() error {
	if s.ln != nil {
		s.ln.Close()
	}
	s.wg.Wait()
	return nil
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
	s.Handler.HandleConn(conn)
}
