package mock

import (
	"context"

	"github.com/ONSdigital/dp-graph/v2/models"
)

func (m *Mock) GetCodeLists(ctx context.Context, filterBy string) (*models.CodeListResults, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.CodeListResults{
		Items: []models.CodeList{
			{

				ID: "code-list-1",
			},
			{
				ID: "code-list-2",
			},
			{
				ID: "code-list-3",
			},
		},
	}, nil
}

func (m *Mock) GetCodeList(ctx context.Context, codeListID string) (*models.CodeList, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.CodeList{
		ID: codeListID,
	}, nil
}

func (m *Mock) GetEditions(ctx context.Context, codeListID string) (*models.Editions, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.Editions{
		Items: []models.Edition{
			{
				ID:    "edition-1",
				Label: "edition-label-1",
			},
			{
				ID:    "edition-2",
				Label: "edition-label-2",
			},
			{
				ID:    "edition-3",
				Label: "edition-label-3",
			},
		},
	}, nil
}

func (m *Mock) GetEdition(ctx context.Context, codeListID, edition string) (*models.Edition, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.Edition{
		ID:    edition,
		Label: "test-label",
	}, nil
}

func (m *Mock) CountCodes(ctx context.Context, codeListID string, edition string) (int64, error) {
	return 1, nil
}

func (m *Mock) GetCodes(ctx context.Context, codeListID, edition string) (*models.CodeResults, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.CodeResults{
		Items: []models.Code{},
	}, nil
}

func (m *Mock) GetCode(ctx context.Context, codeListID, edition string, code string) (*models.Code, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.Code{}, nil
}

func (m *Mock) GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*models.Datasets, error) {
	if err := m.checkForErrors(); err != nil {
		return nil, err
	}

	return &models.Datasets{
		Items: []models.Dataset{
			{
				ID:             code,
				DimensionLabel: "label 1",
				Editions: []models.DatasetEdition{
					{
						ID:            "edition-1",
						CodeListID:    codeListID,
						LatestVersion: 1,
					},
					{
						ID:            "edition-2",
						CodeListID:    codeListID,
						LatestVersion: 1,
					},
					{
						ID:            "edition-3",
						CodeListID:    codeListID,
						LatestVersion: 1,
					},
				},
			},
		},
	}, nil
}
