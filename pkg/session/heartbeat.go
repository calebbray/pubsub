package session

import (
	"time"
)

type Kind uint8

const (
	KindPing Kind = iota
	KindPong
)

type HeartbeatConfig struct {
	Interval time.Duration
	Timeout  time.Duration
}
