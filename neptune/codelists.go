package neptune

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-code-list-api/models"
	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/neptune/query"
)

/*
GetCodeLists provides a list of either all Code Lists, or a list of only those
having a boolean property with the name <filterBy> which is set to true. E.g.
"geography": "true". The caller is expected to
fully qualify the embedded Links field afterwards. It returns an error if:
- The Gremlin query failed to execute.
- A CodeList is encountered that does not have *listID* property.
*/
func (n *NeptuneDB) GetCodeLists(ctx context.Context, filterBy string) (*models.CodeListResults, error) {
	// Use differing Gremlin queries - depending on if a filterBy string is specified.
	var qry string
	if filterBy == "" {
		qry = fmt.Sprintf(query.GetCodeLists)
	} else {
		qry = fmt.Sprintf(query.GetCodeListsFiltered, filterBy)
	}
	codeListVertices, err := n.getVertices(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	results := &models.CodeListResults{}
	for _, codeListVertex := range codeListVertices {
		codeListID, err := codeListVertex.GetProperty("listID")
		if err != nil {
			return nil, errors.Wrapf(err, `Error reading "listID" property on Code List vertex`)
		}
		link := &models.CodeListLink{Self: &models.Link{ID: codeListID}}
		codeListMdl := models.CodeList{codeListID, link}
		results.Items = append(results.Items, codeListMdl)
	}
	howMany := len(codeListVertices)
	results.Count = howMany
	results.Limit = -1
	results.Offset = 0
	results.TotalCount = howMany
	return results, nil
}

// GetCodeList provides a CodeList for a given ID (e.g. "ashe-earnings"),
// having checked it exists
// in the database. Nb. The caller is expected to fully qualify the embedded
// Links field afterwards. It returns an error if:
// - The Gremlin query failed to execute.
// - The requested CodeList does not exist. (error is `ErrNotFound`)
// - Duplicate CodeLists exist with the given ID (error is `ErrMultipleFound`)
func (n *NeptuneDB) GetCodeList(ctx context.Context, codeListID string) (
	*models.CodeList, error) {
	existsQry := fmt.Sprintf(query.CodeListExists, codeListID)
	count, err := n.getNumber(existsQry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", existsQry)
	}
	if count == 0 {
		return nil, driver.ErrNotFound
	}
	if count > 1 {
		return nil, driver.ErrMultipleFound
	}

	return &models.CodeList{
		Links: &models.CodeListLink{
			Self: &models.Link{
				ID: codeListID,
			},
		},
	}, nil
}

func (n *NeptuneDB) GetEditions(ctx context.Context, codeListID string) (*models.Editions, error) {
	return &models.Editions{
		Count:      3,
		Offset:     0,
		Limit:      3,
		TotalCount: 3,
		Items: []models.Edition{
			{
				Links: &models.EditionLinks{
					Self: &models.Link{
						ID: "edition-1",
					},
				},
			},
			{
				Links: &models.EditionLinks{
					Self: &models.Link{
						ID: "edition-2",
					},
				},
			},
			{
				Links: &models.EditionLinks{
					Self: &models.Link{
						ID: "edition-3",
					},
				},
			},
		},
	}, nil
}

func (n *NeptuneDB) GetEdition(ctx context.Context, codeListID, edition string) (*models.Edition, error) {
	return &models.Edition{
		Links: &models.EditionLinks{
			Self: &models.Link{
				ID: edition,
			},
		},
	}, nil
}

func (n *NeptuneDB) GetCodes(ctx context.Context, codeListID, edition string) (*models.CodeResults, error) {
	return &models.CodeResults{
		Count:      3,
		Offset:     0,
		Limit:      3,
		TotalCount: 3,
		Items: []models.Code{
			{
				Links: &models.CodeLinks{
					Self: &models.Link{
						ID: "code-1",
					},
				},
			},
			{
				Links: &models.CodeLinks{
					Self: &models.Link{
						ID: "code-2",
					},
				},
			},
			{
				Links: &models.CodeLinks{
					Self: &models.Link{
						ID: "code-3",
					},
				},
			},
		},
	}, nil
}

func (n *NeptuneDB) GetCode(ctx context.Context, codeListID, edition string, code string) (*models.Code, error) {
	return &models.Code{
		Links: &models.CodeLinks{
			Self: &models.Link{
				ID: code,
			},
		},
	}, nil
}

func (n *NeptuneDB) GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*models.Datasets, error) {
	return &models.Datasets{
		Items: []models.Dataset{
			{
				DimensionLabel: "label 1",
				Links: &models.DatasetLinks{
					Self: &models.Link{
						ID: code,
					},
				},
				Editions: []models.DatasetEdition{
					{
						Links: &models.DatasetEditionLinks{
							Self: &models.Link{
								ID: "edition-1",
							},
							LatestVersion: &models.Link{
								ID: "1",
							},
							DatasetDimension: &models.Link{
								ID: codeListID,
							},
						},
					},
					{
						Links: &models.DatasetEditionLinks{
							Self: &models.Link{
								ID: "edition-2",
							},
							LatestVersion: &models.Link{
								ID: "1",
							},
							DatasetDimension: &models.Link{
								ID: codeListID,
							},
						},
					},
					{
						Links: &models.DatasetEditionLinks{
							Self: &models.Link{
								ID: "edition-3",
							},
							LatestVersion: &models.Link{
								ID: "1",
							},
							DatasetDimension: &models.Link{
								ID: codeListID,
							},
						},
					},
				},
			},
		},
	}, nil
}
