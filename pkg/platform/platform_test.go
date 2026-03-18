package platform

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveAndRetriveArtifact(t *testing.T) {
	path := path.Join(t.TempDir(), "artifacts.json")
	c, err := NewCatalog(path)
	require.NoError(t, err)

	a := getTestArtifact("1")

	c.SaveArtifact(a)

	c.fd.Close()
	c, err = NewCatalog(path)
	require.NoError(t, err)

	persisted, err := c.GetArtifact(a.Id)
	require.NoError(t, err)

	assert.Equal(t, &a, persisted)
}

func TestListArtifactsByDatasetName(t *testing.T) {
	path := path.Join(t.TempDir(), "artifacts.json")
	c, err := NewCatalog(path)
	require.NoError(t, err)

	a := getTestArtifact("1")
	b := getTestArtifact("2")

	c.SaveArtifact(a)
	c.SaveArtifact(b)

	artifacts, err := c.ListArtifact(a.DatasetName)
	require.NoError(t, err)

	assert.Equal(t, 2, len(artifacts))

	for _, art := range artifacts {
		assert.Equal(t, "test-dataset", art.DatasetName)
	}
}

func TestGetArtifactWithUnknownID(t *testing.T) {
	path := path.Join(t.TempDir(), "artifacts.json")
	c, err := NewCatalog(path)
	require.NoError(t, err)

	_, err = c.GetArtifact("foo")

	require.Error(t, err)
}

func getTestArtifact(version string) Artifact {
	return NewArtifact("test-dataset", "caleb", version, "1", "foo.com")
}
