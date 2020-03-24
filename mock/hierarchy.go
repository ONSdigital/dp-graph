package mock

import (
	"context"

	"github.com/ONSdigital/dp-graph/models"
)

func (m *Mock) CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) CloneNodes(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) CountNodes(ctx context.Context, instanceID, dimensionName string) (count int64, err error) {
	return 0, m.checkForErrors()
}

func (m *Mock) CloneRelationships(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) SetNumberOfChildren(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) SetHasData(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) MarkNodesToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) RemoveNodesNotMarkedToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) RemoveRemainMarker(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) GetHierarchyCodelist(ctx context.Context, instanceID, dimension string) (string, error) {
	return "codelistID", m.checkForErrors()
}

func (m *Mock) GetHierarchyRoot(ctx context.Context, instanceID, dimension string) (*models.HierarchyResponse, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.HierarchyResponse{
		Label:        "h-lay-bull",
		ID:           "h-eye-dee",
		NoOfChildren: 1,
		HasData:      true,
		Children: []*models.HierarchyElement{
			{
				Label:        "h-child1",
				NoOfChildren: 2,
			},
		},
	}, nil
}

func (m *Mock) GetHierarchyElement(ctx context.Context, instanceID, dimension, code string) (*models.HierarchyResponse, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.HierarchyResponse{
		Label:        "lay-bull",
		ID:           code,
		NoOfChildren: 1,
		HasData:      true,
		Breadcrumbs: []*models.HierarchyElement{
			{
				Label:        "child1",
				NoOfChildren: 1,
			},
		},
	}, nil
}
