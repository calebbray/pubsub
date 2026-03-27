package pubsub_test

import (
	"path"
	"testing"
	"time"

	"github.com/calebbray/pubsub"
	"github.com/calebbray/pubsub/pkg/rpc"
	"github.com/calebbray/pubsub/pkg/session"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerStartsAndAcceptsConnections(t *testing.T) {
	cfg := pubsub.DefaultConfig()
	cfg.Addr = ":0" // just get whatever port is free
	cfg.AuthToken = "mysecrettoken"
	cfg.LogDir = t.TempDir()
	cfg.RegistryPath = path.Join(t.TempDir(), "registry.json")

	s := pubsub.NewServer(cfg)
	require.NoError(t, s.Start(nil))
	defer s.Stop()

	_, err := session.Dial(s.Addr(), "caleb", "mysecrettoken", []uint8{1}, session.SessionOpts{}, "")
	require.NoError(t, err)
}

func TestRegisterAHandlerAndCallRPC(t *testing.T) {
	cfg := pubsub.DefaultConfig()
	cfg.Addr = ":0" // just get whatever port is free
	cfg.AuthToken = "mysecrettoken"
	cfg.LogLevel = "debug"
	cfg.LogFormat = "json"
	cfg.LogDir = t.TempDir()
	cfg.RegistryPath = path.Join(t.TempDir(), "registry.json")

	s := pubsub.NewServer(cfg)

	s.Register("/test/method", func(payload []byte) ([]byte, error) {
		return []byte("success"), nil
	})
	require.NoError(t, s.Start(nil))
	defer s.Stop()

	client, err := pubsub.NewClient(pubsub.ClientConfig{
		Addr:              s.Addr(),
		ClientId:          "caleb",
		AuthToken:         "mysecrettoken",
		SupportedVersions: []uint8{1},
		HeartbeatInterval: 30 * time.Second,
	})
	require.NoError(t, err)

	msg, err := client.Send("/test/method", []byte("ground control to major tom"))
	require.NoError(t, err)
	assert.Equal(t, rpc.KindResponse, msg.Kind)
	assert.Equal(t, "/test/method", msg.Method)
	assert.Equal(t, []byte("success"), msg.Payload)
}

func TestServerStartsAndStopsCleanly(t *testing.T) {
	cfg := pubsub.DefaultConfig()
	cfg.Addr = ":0" // just get whatever port is free
	cfg.LogDir = t.TempDir()
	cfg.RegistryPath = path.Join(t.TempDir(), "registry.json")
	s := pubsub.NewServer(cfg)
	require.NoError(t, s.Start(nil))
	require.NoError(t, s.Stop())
}
