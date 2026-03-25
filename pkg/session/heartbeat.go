package session

import (
	"time"
)

type Kind uint8

const (
	KindPing Kind = 0xFE
	KindPong Kind = 0xFF
)

type HeartbeatConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}
