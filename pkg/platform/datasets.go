package platform

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"pipelines/pkg/pubsub"
)

type Version struct {
	major, minor, patch int
}

func NewVersion(strVersion string) (Version, error) {
	var mj, mn, pt int

	parts := strings.Split(strVersion, ".")

	if len(parts) != 3 {
		return Version{}, fmt.Errorf("invalid version format; expected x.x.x, got %s", strVersion)
	}

	fmt.Sscanf(strVersion, "%d.%d.%d", &mj, &mn, &pt)
	return Version{mj, mn, pt}, nil
}

func (v Version) Greater(version Version) bool {
	if v.major != version.major {
		return v.major > version.major
	}

	if v.minor != version.minor {
		return v.minor > version.minor
	}

	return v.patch > version.patch
}

func (v Version) Less(version Version) bool {
	if v.major != version.major {
		return v.major < version.major
	}

	if v.minor != version.minor {
		return v.minor < version.minor
	}

	return v.patch < version.patch
}

func (v Version) Equal(v2 Version) bool {
	return v.major == v2.major &&
		v.minor == v2.minor &&
		v.patch == v2.patch
}

func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.major, v.minor, v.patch)
}

type DatasetState struct {
	Name              string
	LatestVersion     Version
	LatestArtifactId  string
	LastPublishedAt   time.Time
	Status            string
	LastFailureReason string
}

type StateProjection struct {
	state map[string]*DatasetState
}

func NewStateProjection() *StateProjection {
	return &StateProjection{
		state: make(map[string]*DatasetState),
	}
}

var ErrDatasetNotFound = errors.New("dataset not found")

func (s *StateProjection) Project(event pubsub.Event, artifact Artifact) error {
	v, err := NewVersion(artifact.Version)
	if err != nil {
		return fmt.Errorf("artifact provided invalid version: %w", err)
	}

	ds, ok := s.state[artifact.DatasetName]
	if !ok {

		ds = &DatasetState{Name: artifact.DatasetName, LatestVersion: v}
		s.state[artifact.DatasetName] = ds
	}

	if v.Less(ds.LatestVersion) {
		return nil
	}

	ds.LatestVersion = v
	ds.LastPublishedAt = event.PublishedAt
	ds.LatestArtifactId = artifact.Id
	ds.Status = "ok"
	ds.LastFailureReason = ""
	return nil
}

func (s *StateProjection) RecordFailure(datasetName, reason string) error {
	ds, err := s.GetState(datasetName)
	if err != nil {
		return err
	}

	ds.LastFailureReason = reason
	ds.Status = "failed"
	return nil
}

func (s *StateProjection) GetState(datasetName string) (*DatasetState, error) {
	ds, ok := s.state[datasetName]
	if !ok {
		return nil, ErrDatasetNotFound
	}
	return ds, nil
}
