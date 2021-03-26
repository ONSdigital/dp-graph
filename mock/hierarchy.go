package mock

import (
	"context"

	"github.com/ONSdigital/dp-graph/v2/models"
)

func (m *Mock) HierarchyExists(ctx context.Context, instanceID, dimension string) (hierarchyExists bool, err error) {
	return true, m.checkForErrors()
}

func (m *Mock) CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) GetCodesWithData(ctx context.Context, attempt int, instanceID, dimensionName string) (codes []string, err error) {
	return []string{}, m.checkForErrors()
}

func (m *Mock) GetGenericHierarchyNodeIDs(ctx context.Context, attempt int, codeListID string, codes []string) (nodeIDs map[string]string, err error) {
	return map[string]string{}, m.checkForErrors()
}

func (m *Mock) GetGenericHierarchyAncestriesIDs(ctx context.Context, attempt int, codeListID string, codes []string) (nodeIDs map[string]string, err error) {
	return map[string]string{}, m.checkForErrors()
}

func (m *Mock) CloneNodes(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) CloneNodesFromIDs(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string, ids map[string]struct{}, hasData bool) (err error) {
	return m.checkForErrors()
}

func (m *Mock) CountNodes(ctx context.Context, instanceID, dimensionName string) (count int64, err error) {
	return 0, m.checkForErrors()
}

func (m *Mock) GetHierarchyNodeIDs(ctx context.Context, attempt int, instanceID, dimensionName string) (ids map[string]struct{}, err error) {
	return map[string]struct{}{}, m.checkForErrors()
}

func (m *Mock) CloneRelationships(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) CloneRelationshipsFromIDs(ctx context.Context, attempt int, instanceID, dimensionName string, ids map[string]struct{}) error {
	return m.checkForErrors()
}

func (m *Mock) SetNumberOfChildren(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return m.checkForErrors()
}

func (m *Mock) SetNumberOfChildrenFromIDs(ctx context.Context, attempt int, ids map[string]struct{}) (err error) {
	return m.checkForErrors()
}

func (m *Mock) RemoveCloneEdges(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	return m.checkForErrors()
}

func (m *Mock) RemoveCloneEdgesFromSourceIDs(ctx context.Context, attempt int, ids map[string]struct{}) (err error) {
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
