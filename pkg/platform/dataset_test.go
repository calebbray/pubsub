package platform

import (
	"testing"

	"pipelines/pkg/pubsub"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	datasetName = "test-dataset"
	eventType   = "dataset.published"
	publisher   = "core-lmi"
	version1    = "1.0.0"
	version2    = "2.0.0"
)

func TestProjectNewArtifact(t *testing.T) {
	version, err := NewVersion(version1)
	require.NoError(t, err)
	s := NewStateProjection()

	e := pubsub.NewEvent("dataset.published", publisher, []byte("test dataset updated"))
	a := NewArtifact(datasetName, publisher, version1, "1", "www.foo.com")
	require.NoError(t, s.Project(e, a))

	ds, err := s.GetState(datasetName)
	require.NoError(t, err)

	assert.Equal(t, datasetName, ds.Name)
	assert.Equal(t, version, ds.LatestVersion)
	assert.Equal(t, a.Id, ds.LatestArtifactId)
	assert.Equal(t, "ok", ds.Status)
}

func TestProjectArtifactWithMultipleVersions(t *testing.T) {
	v1, err := NewVersion(version1)
	require.NoError(t, err)
	v2, err := NewVersion(version2)
	require.NoError(t, err)
	s := NewStateProjection()

	e1 := pubsub.NewEvent("dataset.published", publisher, []byte("test dataset updated"))
	a1 := NewArtifact(datasetName, publisher, version1, "1", "www.foo.com")
	require.NoError(t, s.Project(e1, a1))

	ds, err := s.GetState(datasetName)
	require.NoError(t, err)
	assert.Equal(t, v1, ds.LatestVersion, "current version is '1.0.0' after first publish")
	assert.Equal(t, a1.Id, ds.LatestArtifactId)

	e2 := pubsub.NewEvent("dataset.published", publisher, []byte("test dataset updated"))
	a2 := NewArtifact(datasetName, publisher, version2, "1", "www.foo.com")
	require.NoError(t, s.Project(e2, a2))

	assert.Equal(t, datasetName, ds.Name)
	assert.Equal(t, v2, ds.LatestVersion, "current version is '2.0.0' after second publish")
	assert.Equal(t, a2.Id, ds.LatestArtifactId)
	assert.Equal(t, "ok", ds.Status)
}

func TestRecordedFailure(t *testing.T) {
	s := NewStateProjection()
	e := pubsub.NewEvent("dataset.published", publisher, []byte("test dataset updated"))
	a := NewArtifact(datasetName, publisher, version1, "1", "www.foo.com")
	require.NoError(t, s.Project(e, a))

	ds, err := s.GetState(datasetName)
	require.NoError(t, err)

	reason := "YOU LOSE. YOU GET NOTHING"
	s.RecordFailure(datasetName, reason)
	assert.Equal(t, "failed", ds.Status)
	assert.Equal(t, reason, ds.LastFailureReason)
}

func TestPublishAfterFailure(t *testing.T) {
	s := NewStateProjection()
	e := pubsub.NewEvent("dataset.published", publisher, []byte("test dataset updated"))
	a := NewArtifact(datasetName, publisher, version1, "1", "www.foo.com")
	require.NoError(t, s.Project(e, a))

	ds, err := s.GetState(datasetName)
	require.NoError(t, err)

	reason := "YOU LOSE. YOU GET NOTHING"
	s.RecordFailure(datasetName, reason)
	assert.Equal(t, "failed", ds.Status)
	assert.Equal(t, reason, ds.LastFailureReason)

	e2 := pubsub.NewEvent("dataset.published", publisher, []byte("test dataset updated"))
	a2 := NewArtifact(datasetName, publisher, version2, "1", "www.foo.com")
	require.NoError(t, s.Project(e2, a2))

	assert.Equal(t, "ok", ds.Status)
	assert.Equal(t, "", ds.LastFailureReason)
}

func TestGetStateForUnknownDataset(t *testing.T) {
	s := NewStateProjection()
	_, err := s.GetState(datasetName)
	require.Error(t, err)
}
