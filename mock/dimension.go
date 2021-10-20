package mock

import (
	"context"
	"sync"

	"github.com/ONSdigital/dp-graph/v3/models"
)

func (m *Mock) InsertDimension(ctx context.Context, cache map[string]string, cacheMutex *sync.Mutex, instanceID string, d *models.Dimension) (*models.Dimension, error) {
	return nil, m.checkForErrors()
}
