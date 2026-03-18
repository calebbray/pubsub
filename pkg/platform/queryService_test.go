package platform

import (
	"path"
	"testing"

	"pipelines/pkg/pubsub"
	"pipelines/pkg/registry"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLatestArtifactReturnsCorrectArtifactAfterMultiplePublishes(t *testing.T) {
	v, _ := NewVersion(version2)
	qs := bootstrapQueryService(t)
	a, err := qs.LatestArtifact(datasetName)
	require.NoError(t, err, "artifact found for "+datasetName)

	assert.Equal(t, datasetName, a.DatasetName)
	assert.Equal(t, publisher, a.ProducerId)
	assert.Equal(t, v.String(), a.Version)
}

func TestDatasetStatusReturnsCurrentState(t *testing.T) {
	v, _ := NewVersion(version2)
	qs := bootstrapQueryService(t)
	a, err := qs.LatestArtifact(datasetName)
	require.NoError(t, err, "artifact found for "+datasetName)

	require.Equal(t, datasetName, a.DatasetName)

	state, err := qs.DatasetStatus(datasetName)
	require.NoError(t, err)
	assert.Equal(t, datasetName, state.Name)
	assert.Equal(t, "ok", state.Status)
	assert.Equal(t, v, state.LatestVersion)
	assert.Equal(t, a.Id, state.LatestArtifactId)
}

func TestSubscribersReturnsAllSubscribersByEventType(t *testing.T) {
	qs := bootstrapQueryService(t)

	subs, err := qs.Subscribers(eventType)
	require.NoError(t, err)

	assert.Equal(t, 2, len(subs))
	for _, sub := range subs {
		assert.Equal(t, eventType, sub.EventType)
	}
}

func TestLatestArtifactForUnknownDatasetReturnsError(t *testing.T) {
	qs := bootstrapQueryService(t)
	_, err := qs.DatasetStatus("fake dataset")
	require.Error(t, err)
}

func bootstrapQueryService(t *testing.T) *QueryService {
	catalogPath := path.Join(t.TempDir(), "catalog.json")
	registryPath := path.Join(t.TempDir(), "fileStoreRegistry.json")

	catalog, err := NewCatalog(catalogPath)
	require.NoError(t, err)

	fs, err := registry.NewFileStore(registryPath)
	require.NoError(t, err)

	reg, err := registry.NewPersistentRegistry(fs)
	require.NoError(t, err)

	_, err = reg.Subscribe("caleb", eventType, noOpDeliverFunc)
	require.NoError(t, err)
	_, err = reg.Subscribe("bray", eventType, noOpDeliverFunc)
	require.NoError(t, err)

	state := NewStateProjection()

	e1 := pubsub.NewEvent(eventType, publisher, nil)
	a1 := NewArtifact(datasetName, publisher, version1, "1", "")
	require.NoError(t, catalog.SaveArtifact(a1))
	require.NoError(t, state.Project(e1, a1))

	e2 := pubsub.NewEvent(eventType, publisher, nil)
	a2 := NewArtifact(datasetName, publisher, version2, "1", "")
	require.NoError(t, catalog.SaveArtifact(a2))
	require.NoError(t, state.Project(e2, a2))

	qs := NewQueryService(state, catalog, reg)
	return qs
}

func noOpDeliverFunc(e pubsub.Event, offset uint64) error {
	return nil
}
