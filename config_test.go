package pubsub_test

import (
	"testing"

	"github.com/calebbray/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigFile(t *testing.T) {
	t.Setenv("AUTH_TOKEN", "mysecrettoken")
	cfg, err := pubsub.LoadConfig("example-config.yml")
	require.NoError(t, err)
	assert.Equal(t, "mysecrettoken", cfg.AuthToken)
}
