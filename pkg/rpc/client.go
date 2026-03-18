package rpc

import (
	"errors"
	"net"
	"sync"

	"pipelines/pkg/transport"
)

type Client struct {
	conn    net.Conn
	close   sync.Once
	mu      sync.Mutex
	pending map[uint64]chan result
	quitCh  chan struct{}
	nextReq uint64
}

type result struct {
	msg Message
	err error
}

func NewClient(conn net.Conn) *Client {
	c := &Client{
		conn:    conn,
		quitCh:  make(chan struct{}),
		pending: make(map[uint64]chan result),
		nextReq: 1,
	}

	go c.readLoop()
	return c
}

func (c *Client) Send(method string, payload []byte) (Message, error) {
	c.mu.Lock()
	req := Message{ReqId: c.nextReq, Kind: KindRequest, Method: method, Payload: payload}
	c.nextReq++

	ch := make(chan result, 1)
	c.pending[req.ReqId] = ch
	c.mu.Unlock()

	encoded := Encode(req)

	if err := transport.WriteFrame(c.conn, encoded); err != nil {
		return Message{}, err
	}

	var response result
	select {
	case response = <-ch:
		if response.err != nil {
			return Message{}, response.err
		}
	case <-c.quitCh:
		return Message{}, ErrClientClosed
	}

	return response.msg, nil
}

func (c *Client) Close() error {
	c.close.Do(func() {
		close(c.quitCh)
		c.conn.Close()
	})
	return nil
}

func (c *Client) readLoop() {
	defer c.Close()
	for {
		frame, err := transport.ReadFrame(c.conn)
		if err != nil {
			c.mu.Lock()
			for id, ch := range c.pending {
				ch <- result{err: err}
				delete(c.pending, id)
			}
			c.mu.Unlock()
			return
		}

		msg, err := Decode(frame)

		c.mu.Lock()
		ch, ok := c.pending[msg.ReqId]
		delete(c.pending, msg.ReqId)
		c.mu.Unlock()

		if ok {
			ch <- result{msg: msg}
		}

	}
}

var ErrClientClosed = errors.New("client closed")
