package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/calebbray/pubsub/pkg/metrics"
	"github.com/calebbray/pubsub/pkg/transport"
)

type HandlerFunc func(payload []byte) ([]byte, error)

type HealthChecker interface {
	Health() HealthStatus
	Ready() HealthStatus
}

type HealthStatus struct {
	OK      bool              `json:"ok"`
	Message string            `json:"message"`
	Details map[string]string `json:"details"`
}

type HealthCheck struct {
	metrics metrics.MetricsProvider
}

func (hc HealthCheck) Health() HealthStatus {
	return HealthStatus{
		OK:      true,
		Message: "healthy",
	}
}

func (hc HealthCheck) Ready() HealthStatus {
	ok := hc.metrics.Gauge("subscribers.active").Value() > 0
	return HealthStatus{
		OK:      ok,
		Message: "service ready",
	}
}

func DefaultHealthChecker(m metrics.MetricsProvider) HealthChecker {
	return HealthCheck{m}
}

func (s *Server) handleHealth(_ []byte) ([]byte, error) {
	return json.Marshal(s.HealthChecker.Health())
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

func (s *Server) handleReady(_ []byte) ([]byte, error) {
	return json.Marshal(s.HealthChecker.Ready())
}

func (s *Server) MetricLogMiddleware(route string, h HandlerFunc) (string, HandlerFunc) {
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
	HealthChecker HealthChecker
	Logger        *slog.Logger
	Metrics       metrics.MetricsProvider
}

func NewServer(opts ServerOpts) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	logger = logger.With("component", "rpc")
	opts.Logger = logger

	if opts.Metrics == nil {
		opts.Metrics = metrics.NewRegistry()
	}

	if opts.HealthChecker == nil {
		opts.HealthChecker = DefaultHealthChecker(opts.Metrics)
	}

	s := &Server{
		handlers:   make(map[string]HandlerFunc),
		metrics:    make(map[string]int),
		upSince:    time.Now(),
		ServerOpts: opts,
	}

	s.Register(s.MetricLogMiddleware("/_admin/health", s.handleHealth))
	s.Register(s.MetricLogMiddleware("/_admin/status", s.handleStatus))
	s.Register(s.MetricLogMiddleware("/_admin/metrics", s.handleMetrics))
	s.Register(s.MetricLogMiddleware("/_admin/ready", s.handleReady))

	return s
}

// session.HandleConn will call this
func (s *Server) HandleFrame(w io.Writer, frame []byte) error {
	msg, err := Decode(frame)
	if err != nil {
		s.Logger.Error("failed to decode frame", "error", err)
		msg.Kind = KindError
		msg.Payload = []byte("internal server error")
		return transport.WriteFrame(w, Encode(msg))
	}

	s.Logger.Debug("rpc request received",
		"method", msg.Method,
		"req_id", msg.ReqId,
		"payload", string(msg.Payload),
	)

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

	s.Logger.Debug("rpc response dispatched",
		"method", msg.Method,
		"req_id", msg.ReqId,
		"kind", msg.Kind,
		"payload", string(msg.Payload),
	)
	return transport.WriteFrame(w, Encode(msg))
}

// this is really just a util to implement connHandler so we can test
// the rpc server in isolation without needing to have an authorized session
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
