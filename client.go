package pubsub

import (
	"time"

	"github.com/calebbray/pubsub/pkg/rpc"
	"github.com/calebbray/pubsub/pkg/session"
)

const (
	KindRequest  = rpc.KindRequest
	KindResponse = rpc.KindResponse
	KindError    = rpc.KindError
)

type ClientConfig struct {
	Addr              string
	ClientId          string
	AuthToken         string
	SupportedVersions []uint8
	HeartbeatInterval time.Duration
	ResumeToken       SessionToken
}

type Client struct {
	rpc     *rpc.Client
	session *session.Session
}

func NewClient(cfg ClientConfig) (*Client, error) {
	sess, err := session.Dial(
		cfg.Addr,
		cfg.ClientId,
		cfg.AuthToken,
		cfg.SupportedVersions,
		session.SessionOpts{
			Heartbeat: session.HeartbeatConfig{
				Interval: cfg.HeartbeatInterval,
			},
		},
		session.SessionToken(cfg.ResumeToken),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		rpc:     rpc.NewClient(sess),
		session: sess,
	}, nil
}

type Message rpc.Message

func (c *Client) Send(method string, payload []byte) (Message, error) {
	res, err := c.rpc.Send(method, payload)
	return Message(res), err
}

func (c *Client) Close() error {
	return c.session.Close()
}

type SessionToken session.SessionToken

func (c *Client) SessionToken() SessionToken {
	return SessionToken(c.session.SessionToken())
}
