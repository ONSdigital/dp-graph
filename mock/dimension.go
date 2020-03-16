package mock

import (
	"context"

	"github.com/ONSdigital/dp-graph/models"
)

func (m *Mock) InsertDimension(ctx context.Context, cache map[string]string, i *models.Instance, d *models.Dimension) (*models.Dimension, error) {
	return nil, m.checkForErrors()
}
