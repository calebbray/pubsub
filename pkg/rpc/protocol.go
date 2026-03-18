package rpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Kind uint8

const (
	KindRequest Kind = iota
	KindResponse
	KindError
)

type Message struct {
	ReqId   uint64
	Kind    Kind
	Method  string
	Payload []byte
}

func (m Message) String() string {
	return fmt.Sprintf("ReqID: %d\nKind: %d\nMethod: %s\nPayload: %s\n", m.ReqId, m.Kind, m.Method, m.Payload)
}

// Encoded Message should look like this:
// [u64 reqId][u8 Kind][u16 method len][method...][u32 payloadlen][payload...]
func Encode(msg Message) []byte {
	bufSize := 8 + 1 + 2 + len(msg.Method) + 4 + len(msg.Payload)
	buf := make([]byte, bufSize)

	binary.BigEndian.PutUint64(buf[0:8], msg.ReqId)
	buf[8] = byte(msg.Kind)
	binary.BigEndian.PutUint16(buf[9:11], uint16(len(msg.Method)))

	offset := 11
	copy(buf[offset:offset+len(msg.Method)], []byte(msg.Method))

	offset += len(msg.Method)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(msg.Payload)))

	offset += 4
	copy(buf[offset:offset+len(msg.Payload)], msg.Payload)

	return buf
}

func Decode(data []byte) (Message, error) {
	var msg Message
	r := bytes.NewReader(data)

	var reqId uint64
	if err := binary.Read(r, binary.BigEndian, &reqId); err != nil {
		return msg, err
	}

	var k Kind
	if err := binary.Read(r, binary.BigEndian, &k); err != nil {
		return msg, err
	}

	var methodSize uint16
	if err := binary.Read(r, binary.BigEndian, &methodSize); err != nil {
		return msg, err
	}

	method := make([]byte, methodSize)
	if _, err := io.ReadFull(r, method); err != nil {
		return msg, err
	}

	var payloadSize uint32
	if err := binary.Read(r, binary.BigEndian, &payloadSize); err != nil {
		return msg, err
	}

	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(r, payload); err != nil {
		return msg, err
	}

	msg.ReqId = reqId
	msg.Kind = k
	msg.Method = string(method)
	msg.Payload = payload

	return msg, nil
}
