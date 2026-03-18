package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

type HandshakeRequest struct {
	ClientID string `json:"clientId"`
	Token    string `json:"token"`
}

type HandshakeResponse struct {
	Success bool   `json:"success"`
	Reason  string `json:"reason"`
}

type CapabilitiesRequest struct {
	Versions []uint8 `json:"version"`
}

type CapabilitiesResponse struct {
	Version uint8  `json:"version"`
	Reason  string `json:"reason"`
}

const UnsupportedVersion uint8 = 255

var (
	ErrInvalidToken       = errors.New("invalid token")
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
)

func (s *Session) ServerHello(validToken string, supportedVersions []uint8) error {
	if err := s.ServerHandshake(validToken); err != nil {
		return err
	}

	return s.ServerNegotiate(supportedVersions)
}

func (s *Session) ClientHello(clientId, token string, supportedVersions []uint8) error {
	if err := s.ClientHandshake(clientId, token); err != nil {
		return err
	}

	return s.ClientNegotiate(supportedVersions)
}

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

func (s *Session) ServerNegotiate(supported []uint8) error {
	b, err := s.Receive()
	if err != nil {
		return err
	}

	var received CapabilitiesRequest
	if err = json.Unmarshal(b, &received); err != nil {
		return err
	}

	var response CapabilitiesResponse
	matches := match(received.Versions, supported)

	if len(matches) == 0 {
		response.Version = UnsupportedVersion
		response.Reason = ErrUnsupportedVersion.Error()
		err = ErrUnsupportedVersion
	} else {
		response.Version = slices.Max(matches)
	}

	b, jerr := json.Marshal(&response)
	if jerr != nil {
		return jerr
	}

	if serr := s.Send(b); serr != nil {
		return serr
	}

	return err
}

func match[T comparable](a, b []T) []T {
	var matches []T
	for _, x := range a {
		for _, y := range b {
			if x == y {
				matches = append(matches, x)
			}
		}
	}
	return matches
}

func (s *Session) ClientNegotiate(supported []uint8) error {
	req := CapabilitiesRequest{Versions: supported}
	b, err := json.Marshal(&req)
	if err != nil {
		return err
	}

	if err = s.Send(b); err != nil {
		return err
	}

	b, err = s.Receive()
	if err != nil {
		return err
	}

	var res CapabilitiesResponse
	if err = json.Unmarshal(b, &res); err != nil {
		return err
	}

	if res.Version == UnsupportedVersion {
		return fmt.Errorf("unsuccessful negotiation: %s", res.Reason)
	}

	s.version = res.Version

	return nil
}
