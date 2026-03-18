package session

import (
	"encoding/json"
	"errors"
	"fmt"
)

type HandshakeRequest struct {
	ClientID string `json:"clientId"`
	Token    string `json:"token"`
}

type HandshakeResponse struct {
	Success bool   `json:"success"`
	Reason  string `json:"reason"`
}

var ErrInvalidToken = errors.New("invalid token")

func (s *Session) ServerHandshake(validToken string) error {
	b, err := s.Receive()
	if err != nil {
		return err
	}

	var received HandshakeRequest
	if err = json.Unmarshal(b, &received); err != nil {
		return err
	}

	// validates token
	var response HandshakeResponse
	if received.Token != validToken {
		response.Success = false
		response.Reason = "invalid token"
		b, _ = json.Marshal(&response)
		s.Send(b)
		return ErrInvalidToken
	}

	response.Success = true

	b, err = json.Marshal(&response)
	if err != nil {
		return err
	}

	return s.Send(b)
}

func (s *Session) ClientHandshake(clientId, token string) error {
	// Writes handshake request
	req := HandshakeRequest{ClientID: clientId, Token: token}
	payload, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	if err := s.Send(payload); err != nil {
		return err
	}

	// reads handshake response
	b, err := s.Receive()
	if err != nil {
		return err
	}

	var res HandshakeResponse
	if err = json.Unmarshal(b, &res); err != nil {
		return err
	}

	if !res.Success {
		return fmt.Errorf("invalid handshake: %s", res.Reason)
	}

	return nil
}
