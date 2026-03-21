package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

type HandshakeRequest struct {
	ClientID    string       `json:"clientId"`
	Token       string       `json:"token"`
	ResumeToken SessionToken `json:"resumeToken"`
}

type HandshakeResponse struct {
	Success bool         `json:"success"`
	Reason  string       `json:"reason"`
	Resumed bool         `json:"resumed"`
	Version uint8        `json:"version"`
	Token   SessionToken `json:"token"`
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

func (s *Session) ClientHello(clientId, token string, supportedVersions []uint8, resumeToken SessionToken) (SessionToken, error) {
	info, err := s.ClientHandshake(clientId, token, resumeToken)
	if err != nil {
		return "", err
	}

	if info.resumed {
		s.version = info.version
		return info.token, nil
	}

	if err := s.ClientNegotiate(supportedVersions); err != nil {
		return "", err
	}

	return info.token, nil
}

func (s *Session) validateServerHandshake(validToken string) (HandshakeRequest, error) {
	b, err := s.Receive()
	if err != nil {
		return HandshakeRequest{}, err
	}

	var req HandshakeRequest
	if err = json.Unmarshal(b, &req); err != nil {
		// fail to unmarshal means malformed request. Not valid
		return HandshakeRequest{}, err
	}

	if req.Token != validToken {
		res := HandshakeResponse{Success: false, Reason: "invalid token"}
		b, _ := json.Marshal(res)
		s.Send(b)
		return HandshakeRequest{}, ErrInvalidToken
	}

	return req, nil
}

type clientSessionInfo struct {
	token   SessionToken
	resumed bool
	version uint8
}

func (s *Session) ClientHandshake(clientId, token string, resumeToken SessionToken) (clientSessionInfo, error) {
	// Writes handshake request
	req := HandshakeRequest{ClientID: clientId, Token: token, ResumeToken: resumeToken}
	payload, err := json.Marshal(&req)
	if err != nil {
		return clientSessionInfo{}, err
	}

	if err := s.Send(payload); err != nil {
		return clientSessionInfo{}, err
	}

	// reads handshake response
	b, err := s.Receive()
	if err != nil {
		return clientSessionInfo{}, err
	}

	var res HandshakeResponse
	if err = json.Unmarshal(b, &res); err != nil {
		return clientSessionInfo{}, err
	}

	if !res.Success {
		return clientSessionInfo{}, fmt.Errorf("invalid handshake: %s", res.Reason)
	}

	return clientSessionInfo{
		token:   res.Token,
		resumed: res.Resumed,
		version: res.Version,
	}, nil
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

	var res CapabilitiesResponse
	matches := match(received.Versions, supported)

	if len(matches) == 0 {
		res.Version = UnsupportedVersion
		res.Reason = ErrUnsupportedVersion.Error()
		err = ErrUnsupportedVersion
	} else {
		s.version = res.Version
		res.Version = slices.Max(matches)
	}

	b, jerr := json.Marshal(&res)
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
