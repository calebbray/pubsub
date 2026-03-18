package rpc

import (
	"encoding/json"
	"fmt"
	"io"
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
	handlers    map[string]HandlerFunc
	metrics     map[string]int
	addr        string
	methodCount int

	upSince time.Time
}

func NewServer() *Server {
	s := &Server{
		handlers: make(map[string]HandlerFunc),
		metrics:  make(map[string]int),
		upSince:  time.Now(),
	}

	s.Register(s.metricLogMiddleware("/_admin/health", s.handleHealth))
	s.Register(s.metricLogMiddleware("/_admin/status", s.handleStatus))
	s.Register(s.metricLogMiddleware("/_admin/metrics", s.handleMetrics))

	return s
}

func (s *Server) HandleFrame(w io.Writer, frame []byte) error {
	msg, err := Decode(frame)
	if err != nil {
		msg.Kind = KindError
		msg.Payload = []byte("internal server error")
		return transport.WriteFrame(w, Encode(msg))
	}

	fn, ok := s.handlers[msg.Method]
	if !ok {
		msg.Kind = KindError
		msg.Payload = []byte("no route associated with given method")
		return transport.WriteFrame(w, Encode(msg))
	}

	response, err := fn(msg.Payload)
	if err != nil {
		msg.Kind = KindError
		msg.Payload = fmt.Appendf(nil, "%s", err)
		return transport.WriteFrame(w, Encode(msg))
	}

	msg.Kind = KindResponse
	msg.Payload = response
	return transport.WriteFrame(w, Encode(msg))
}

func (s *Server) HandleConn(conn net.Conn) {
	defer conn.Close()
	for {
		frame, err := transport.ReadFrame(conn)
		if err != nil {
			return
		}

		s.HandleFrame(conn, frame)
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
