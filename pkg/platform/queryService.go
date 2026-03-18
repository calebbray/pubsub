package platform

import (
	"fmt"

	"pipelines/pkg/pubsub"
	"pipelines/pkg/registry"
)

type QueryService struct {
	stateProjection *StateProjection
	catalog         *Catalog
	registry        *registry.PersistentRegistry
}

func NewQueryService(s *StateProjection, c *Catalog, r *registry.PersistentRegistry) *QueryService {
	qs := &QueryService{
		stateProjection: s,
		catalog:         c,
		registry:        r,
	}

	return qs
}

func (q *QueryService) LatestArtifact(datasetName string) (*Artifact, error) {
	state, err := q.stateProjection.GetState(datasetName)
	if err != nil {
		return nil, fmt.Errorf("could not get artifact for %s:\n%w", datasetName, err)
	}

	return q.catalog.GetArtifact(state.LatestArtifactId)
}

func (q *QueryService) DatasetStatus(datasetName string) (*DatasetState, error) {
	return q.stateProjection.GetState(datasetName)
}

func (q *QueryService) Subscribers(eventType string) ([]*pubsub.Subscription, error) {
	return q.registry.GetSubscriptions(eventType)
}
