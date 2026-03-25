package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"pipelines/pkg/transport"
)

type HandlerFunc func(payload []byte) ([]byte, error)

type healthResponse struct {
	Status string `json:"status"`
}

func (s *Server) handleHealth(_ []byte) ([]byte, error) {
	return json.Marshal(healthResponse{Status: "ok"})
}

type statusResponse struct {
	Uptime      time.Duration `json:"uptime"`
	MethodCount int           `json:"methodCount"`
}

func (s *Server) handleStatus(_ []byte) ([]byte, error) {
	return json.Marshal(statusResponse{
		Uptime:      time.Since(s.upSince),
		MethodCount: s.methodCount,
	})
}

type metricResponse struct {
	Counts map[string]int `json:"counts"`
}

func (s *Server) handleMetrics(_ []byte) ([]byte, error) {
	return json.Marshal(metricResponse{Counts: s.metrics})
}

func (s *Server) metricLogMiddleware(route string, h HandlerFunc) (string, HandlerFunc) {
	return route, func(payload []byte) ([]byte, error) {
		s.metrics[route]++
		return h(payload)
	}
}

type Server struct {
	ServerOpts
	handlers    map[string]HandlerFunc
	metrics     map[string]int
	addr        string
	methodCount int

	upSince time.Time
}

type ServerOpts struct {
	Logger *slog.Logger
}

func NewServer(opts ServerOpts) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("component", "rpc")
	opts.Logger = logger

	s := &Server{
		handlers:   make(map[string]HandlerFunc),
		metrics:    make(map[string]int),
		upSince:    time.Now(),
		ServerOpts: opts,
	}

	s.Register(s.metricLogMiddleware("/_admin/health", s.handleHealth))
	s.Register(s.metricLogMiddleware("/_admin/status", s.handleStatus))
	s.Register(s.metricLogMiddleware("/_admin/metrics", s.handleMetrics))

	return s
}

func (s *Server) HandleFrame(w io.Writer, frame []byte) error {
	msg, err := Decode(frame)
	if err != nil {
		s.Logger.Error("failed to decode frame", "error", err)
		msg.Kind = KindError
		msg.Payload = []byte("internal server error")
		return transport.WriteFrame(w, Encode(msg))
	}

	fn, ok := s.handlers[msg.Method]
	if !ok {
		s.Logger.Warn("unregistered method called", "method", msg.Method)
		msg.Kind = KindError
		msg.Payload = []byte("no route associated with given method")
		return transport.WriteFrame(w, Encode(msg))
	}

	response, err := fn(msg.Payload)
	if err != nil {
		s.Logger.Warn("handler returned error", "method", msg.Method, "error", err)
		msg.Kind = KindError
		msg.Payload = fmt.Appendf(nil, "%s", err)
		return transport.WriteFrame(w, Encode(msg))
	}

	msg.Kind = KindResponse
	msg.Payload = response
	return transport.WriteFrame(w, Encode(msg))
}

func (s *Server) HandleConn(conn net.Conn) {
	s.Logger.Debug("rpc connection opened", "remote_addr", conn.RemoteAddr())
	defer func() {
		s.Logger.Debug("rpc connection closed", "remote_addr", conn.RemoteAddr())
		conn.Close()
	}()
	for {
		frame, err := transport.ReadFrame(conn)
		if err != nil {
			return
		}

		if err := s.HandleFrame(conn, frame); err != nil {
			return
		}
	}
}

func (s *Server) Register(method string, fn HandlerFunc) {
	s.handlers[method] = fn
	s.metrics[method] = 0
	s.methodCount++
}

func (s *Server) Addr() string {
	return s.addr
}
