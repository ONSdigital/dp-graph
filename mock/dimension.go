package mock

import (
	"context"

	"github.com/ONSdigital/dp-graph/v2/models"
)

func (m *Mock) InsertDimension(ctx context.Context, cache map[string]string, instanceID string, d *models.Dimension) (*models.Dimension, error) {
	return nil, m.checkForErrors()
}
