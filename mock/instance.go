package mock

import (
	"context"
)

func (m *Mock) AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error {
	return m.checkForErrors()
}

func (m *Mock) SetInstanceIsPublished(ctx context.Context, instanceID string) error {
	return m.checkForErrors()
}
