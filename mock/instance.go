package mock

import (
	"context"
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

func (m *Mock) CreateInstanceConstraint(ctx context.Context, instanceID string) error {
	return m.checkForErrors()
}

func (m *Mock) CreateInstance(ctx context.Context, instanceID string, csvHeaders []string) error {
	return m.checkForErrors()
}

func (m *Mock) AddDimensions(ctx context.Context, instanceID string, dimensions []interface{}) error {
	return m.checkForErrors()
}

func (m *Mock) CreateCodeRelationship(ctx context.Context, instanceID, codeListID, code string) error {
	return m.checkForErrors()
}

func (m *Mock) InstanceExists(ctx context.Context, instanceID string) (bool, error) {
	return true, m.checkForErrors()
}
