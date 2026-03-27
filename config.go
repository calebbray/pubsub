package pubsub

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type ConfigFile struct {
	Server Config `yaml:"server"`
}

type Config struct {
	Addr              string          `yaml:"addr"`
	AuthToken         string          `yaml:"auth_token"`
	SupportedVersions []uint8         `yaml:"supported_versions"`
	TransportConfig   TransportConfig `yaml:"transport"`
	SessionConfig     SessionConfig   `yaml:"session"`
	LogLevel          string          `yaml:"log_level"`
	LogFormat         string          `yaml:"log_format"`
	RegistryPath      string          `yaml:"registry_path"`
	LogDir            string          `yaml:"log_dir"`
	MaxLogSize        uint64          `yaml:"max_log_size"`
}

func DefaultConfig() Config {
	return Config{
		Addr:              ":8080",
		SupportedVersions: []uint8{1},
		LogLevel:          "info",
		LogFormat:         "text",
		LogDir:            "data/logs",
		RegistryPath:      "data/registry.json",
		MaxLogSize:        32 * 1024 * 1024, // 32MB
		TransportConfig: TransportConfig{
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		SessionConfig: SessionConfig{
			TTL:              24 * time.Hour,
			HeartbeatTimeout: 90 * time.Second,
			RateLimit: RateLimitConfig{
				Limit:     100,
				BurstSize: 200,
			},
		},
	}
}

type RateLimitConfig struct {
	Limit     int `yaml:"limit"`
	BurstSize int `yaml:"burst_size"`
}

type TransportConfig struct {
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

type SessionConfig struct {
	RateLimit        RateLimitConfig `yaml:"rate_limit"`
	HeartbeatTimeout time.Duration   `yaml:"heartbeat_timeout"`
	TTL              time.Duration   `yaml:"ttl"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	expanded := os.ExpandEnv(string(data))

	var cfg ConfigFile
	if err = yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return Config{}, err
	}

	return cfg.Server, nil
}
