package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	eventlog "github.com/calebbray/pubsub/pkg/event_log"
	"github.com/calebbray/pubsub/pkg/metrics"
	"github.com/calebbray/pubsub/pkg/pubsub"
	"github.com/calebbray/pubsub/pkg/registry"
	"github.com/calebbray/pubsub/pkg/rpc"
	"github.com/calebbray/pubsub/pkg/session"
	"github.com/calebbray/pubsub/pkg/transport"
	"github.com/calebbray/pubsub/pkg/utils"
)

type Server struct {
	config    Config
	transport *transport.Server
	rpc       *rpc.Server
	bus       *pubsub.Bus
	registry  pubsub.SubscriptionRegistry
	metrics   metrics.MetricsProvider
	logger    *slog.Logger
}

func NewServer(cfg Config) *Server {
	logger := slog.Default()
	if cfg.LogFormat == "json" {
		logger = utils.NewJSONLogger(os.Stderr, &slog.HandlerOptions{
			Level: deriveSlogLevel(cfg.LogLevel),
		})
	}

	if cfg.MaxLogSize == 0 {
		cfg.MaxLogSize = 32 * 1024 * 1024 // 32MB
	}

	met := metrics.NewRegistry()

	logs, err := eventlog.NewSegmentedLog(cfg.LogDir, cfg.MaxLogSize)
	if err != nil {
		panic(err)
	}

	fs, err := registry.NewFileStore(cfg.RegistryPath)
	if err != nil {
		panic(err)
	}

	reg, err := registry.NewPersistentRegistry(fs)
	if err != nil {
		panic(err)
	}

	bus := pubsub.NewEventBus(
		reg,
		logs,
		pubsub.BusOpts{
			Logger:  logger,
			Metrics: met,
		},
	)

	rpcServer := rpc.NewServer(rpc.ServerOpts{
		Logger:        logger,
		Metrics:       met,
		HealthChecker: rpc.DefaultHealthChecker(met),
	})

	tr := transport.NewServer(cfg.Addr, transport.ServerOpts{
		Logger:  logger,
		Metrics: met,
		DeadlineConfig: transport.DeadlineConfig{
			ReadTimeout:  cfg.TransportConfig.ReadTimeout,
			WriteTimeout: cfg.TransportConfig.WriteTimeout,
		},
		Handler: session.SessionHandler{
			ValidToken:        cfg.AuthToken,
			SupportedVersions: cfg.SupportedVersions,
			Logger:            logger,
			Store:             session.NewInMemoryStore(),
			SessionTTL:        cfg.SessionConfig.TTL,
			Handler:           rpcServer,
			RateLimit: &session.RateLimitOpts{
				Limit:     cfg.SessionConfig.RateLimit.Limit,
				BurstSize: cfg.SessionConfig.RateLimit.BurstSize,
			},
			Heartbeat: session.HeartbeatConfig{
				Timeout: cfg.SessionConfig.HeartbeatTimeout,
			},
		},
	})

	return &Server{
		config:    cfg,
		transport: tr,
		logger:    logger,
		rpc:       rpcServer,
		metrics:   met,
		bus:       bus,
		registry:  reg,
	}
}

func (s *Server) Register(method string, fn rpc.HandlerFunc) {
	s.rpc.Register(s.rpc.MetricLogMiddleware(method, fn))
}

func (s *Server) Start(cb func(addr string)) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.transport.Run(cb)
	}()
	<-s.transport.Ready()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.transport.Shutdown(ctx)
}

func (s *Server) Addr() string {
	return s.transport.Addr()
}

func deriveSlogLevel(level string) slog.Leveler {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func ensurePaths(cfg Config) error {
	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		return fmt.Errorf("failed to create log dir: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(cfg.RegistryPath), 0o755); err != nil {
		return fmt.Errorf("failed to create registry dir: %w", err)
	}

	return nil
}
