package mock

import (
	"context"

	"github.com/ONSdigital/dp-graph/models"
	"github.com/ONSdigital/dp-graph/observation"
)

func (m *Mock) StreamCSVRows(ctx context.Context, instanceID, filterID string, filters *observation.DimensionFilters, limit *int) (observation.StreamRowReader, error) {
	return nil, m.checkForErrors()
}

func (m *Mock) InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionIDs map[string]string) error {
	return m.checkForErrors()
}
