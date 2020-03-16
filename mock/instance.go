package mock

import (
	"context"

	"github.com/ONSdigital/dp-graph/models"
)

func (m *Mock) CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error) {
	return 0, m.checkForErrors()
}

func (m *Mock) AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
	return m.checkForErrors()
}

func (m *Mock) SetInstanceIsPublished(ctx context.Context, instanceID string) error {
	return m.checkForErrors()
}

func (m *Mock) CreateInstanceConstraint(ctx context.Context, i *models.Instance) error {
	return m.checkForErrors()
}

func (m *Mock) CreateInstance(ctx context.Context, i *models.Instance) error {
	return m.checkForErrors()
}

func (m *Mock) AddDimensions(ctx context.Context, i *models.Instance) error {
	return m.checkForErrors()
}

func (m *Mock) CreateCodeRelationship(ctx context.Context, i *models.Instance, codeListID, code string) error {
	return m.checkForErrors()
}

func (m *Mock) InstanceExists(ctx context.Context, i *models.Instance) (bool, error) {
	return true, m.checkForErrors()
}
