package neptune

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-code-list-api/models"
	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/neptune/query"
)

/*
GetCodeLists provides a list of either all Code Lists, or a list of only those
having a boolean property with the name <filterBy> which is set to true. E.g.
"geography": true. The caller is expected to
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
	results := &models.CodeListResults{
		Count:      len(codeListVertices),
		Limit:      len(codeListVertices),
		TotalCount: len(codeListVertices),
	}
	for _, codeListVertex := range codeListVertices {
		codeListID, err := codeListVertex.GetProperty("listID")
		if err != nil {
			return nil, errors.Wrapf(err, `Error reading "listID" property on Code List vertex`)
		}
		link := &models.CodeListLink{Self: &models.Link{ID: codeListID}}
		codeListMdl := models.CodeList{codeListID, link}
		results.Items = append(results.Items, codeListMdl)
	}
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

/*
GetEditions provides a models.Editions structure populated based on the
the values in the Code List vertices in the database, that have the provided
codeListId.
It returns an error if:
- The Gremlin query failed to execute. (wrapped error)
- No CodeLists are found of the requested codeListID (error is ErrNotFound')
- A CodeList is found that does not have the "edition" property (error is 'ErrNoSuchProperty')
*/
func (n *NeptuneDB) GetEditions(ctx context.Context, codeListID string) (*models.Editions, error) {
	qry := fmt.Sprintf(query.GetCodeList, codeListID)
	codeLists, err := n.getVertices(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	if len(codeLists) == 0 {
		return nil, driver.ErrNotFound
	}
	editions := &models.Editions{
		Count:      len(codeLists),
		Offset:     0,
		Limit:      len(codeLists),
		TotalCount: len(codeLists),
		Items:      []models.Edition{},
	}
	for _, codeList := range codeLists {
		editionString, err := codeList.GetProperty("edition")
		if err != nil {
			return nil, errors.Wrapf(err, `Error reading "edition" property on Code List vertex`)
		}
		edition := models.Edition{
			Links: &models.EditionLinks{
				Self: &models.Link{
					ID: editionString,
				},
			},
		}
		editions.Items = append(editions.Items, edition)
	}
	return editions, nil
}

/*
GetEdition provides an Edition structure for the code list in the database that
has both the given codeListID (e.g. "ashed-earnings"), and the given edition string
(e.g. "one-off").
Nb. The caller is expected to fully qualify the embedded Links field
afterwards.
It returns an error if:
- The Gremlin query failed to execute. (wrapped error)
- No CodeLists exist with the requested codeListID (error is `ErrNotFound`)
- A CodeList is found that does not have the "edition" property (error is 'ErrNoSuchProperty')
- More than one CodeList exists with the requested ID AND edition (error is `ErrMultipleFound`)
*/
func (n *NeptuneDB) GetEdition(ctx context.Context, codeListID, edition string) (*models.Edition, error) {
	qry := fmt.Sprintf(query.CodeListEditionExists, codeListID, edition)
	nFound, err := n.getNumber(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	if nFound == 0 {
		return nil, driver.ErrNotFound
	}
	if nFound > 1 {
		return nil, driver.ErrMultipleFound
	}
	// What we return (having performed the checks above), is actually hard-coded, as a function of the
	// method parameters.
	return &models.Edition{Links: &models.EditionLinks{Self: &models.Link{ID: edition}}}, nil
}

/*
GetCodes provides a list of Code(s) packaged into a models.CodeResults structure that has been populated by
a database query that finds the Code List nodes of the required codeListID (e.g. "ashe-earnings"), and the
required edition (e.g.  "one-off"), and then harvests the Code nodes that are known to be "usedBy" that
Code List.  It raises a wrapped error if the database raises a non-transient error, (e.g.  malformed
query).  It raises driver.ErrNotFound if the graph traversal above produces an empty list of codes -
including the case of a short-circuit early termination of the query, because no such qualifying code
list exists. It returns a wrapped error if a Code is found that does not have a "value" property.
*/
func (n *NeptuneDB) GetCodes(ctx context.Context, codeListID, edition string) (*models.CodeResults, error) {
	qry := fmt.Sprintf(query.GetCodes, codeListID, edition)
	codeResponses, err := n.getVertices(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	if len(codeResponses) == 0 {
		return nil, driver.ErrNotFound
	}
	codeResults := &models.CodeResults{
		Count:      len(codeResponses),
		Offset:     0,
		Limit:      len(codeResponses),
		TotalCount: len(codeResponses),
		Items:      []models.Code{},
	}

	for _, codeResponse := range codeResponses {
		codeValue, err := codeResponse.GetProperty("value")
		if err != nil {
			return nil, errors.Wrapf(err, `Error reading "value" property on Code vertex`)
		}
		codeItem := models.Code{
			Links: &models.CodeLinks{
				Self: &models.Link{
					ID: codeValue,
				},
			},
		}
		codeResults.Items = append(codeResults.Items, codeItem)
	}
	return codeResults, nil
}

/*
GetCode provides a Code struct to represent the requested code list, edition and code string.
E.g. ashe-earnings|one-off|hourly-pay-gross.
It doesn't need to access the database to form the response, but does so to validate the
query. Specifically it can return errors as follows:
- The Gremlin query failed to execute.
- The query parameter values do not successfully navigate to a Code node. (error is `ErrNotFound`)
- Duplicate Code(s) exist that satisfy the search criteria (error is `ErrMultipleFound`)
*/
func (n *NeptuneDB) GetCode(ctx context.Context, codeListID, edition string, code string) (*models.Code, error) {
	qry := fmt.Sprintf(query.CodeExists, codeListID, edition, code)
	nFound, err := n.getNumber(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	if nFound == 0 {
		return nil, driver.ErrNotFound
	}
	if nFound > 1 {
		return nil, driver.ErrMultipleFound
	}
	return &models.Code{
		Links: &models.CodeLinks{
			Self: &models.Link{
				ID: code,
			},
		},
	}, nil
}

func (n *NeptuneDB) GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*models.Datasets, error) {

	// Emit the query and parse the responses.
	qry := fmt.Sprintf(query.GetCodeDatasets, codeListID, edition, code)
	responses, err := n.getStringList(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin GetCodeDatasets failed: %q", qry)
	}

	// Harvest the responses into string-triples (dimensionName, edition, version)
	var responseTriples = [][]string{}
	const stride = 3 // I.e. dimesionName, edition, version
	nInstances := len(responses) / stride
	for i := 0; i < nInstances; i++ {
		offset := i * stride
		dimensionName := responses[offset+0]
		dataSetEdition := responses[offset+1]
		versionStr := responses[offset+2]
		responseTriples = append(responseTriples, []string{dimensionName, dataSetEdition, versionStr})
	}

	// Reduce to keep only latest versions using nested maps.
	type editionToLatestVersion map[string]int
	type dim2Edition map[string]editionToLatestVersion
	d2e := dim2Edition{}
	for _, respTriple := range responseTriples {
		dimensionName := respTriple[0]
		dataSetEdition := respTriple[1]
		versionStr := respTriple[2]

		versionInt, err := strconv.Atoi(versionStr)
		if err != nil {
			return nil, errors.Wrapf(err, "Cannot cast version (%q) to int", versionStr)
		}
		if _, ok := d2e[dimensionName]; !ok {
			d2e[dimensionName] = editionToLatestVersion{}
		}
		latestKnownV, ok := d2e[dimensionName][dataSetEdition]
		if !ok || latestKnownV < versionInt {
			d2e[dimensionName][dataSetEdition] = versionInt
		}
	}

	// Build the request response from the reduced data
	for dimensionName, e2v := range d2e {
		for dataSetEdition, version := range e2v {
			fmt.Printf("XXXXX %s, %s, %d\n", dimensionName, dataSetEdition, version)
		}
	}

	// todo
	// 1) reconcile the permuations of dimensionName and edition.
	// 2) for each permutation, find (among duplicates) the most recent version.
	// 3) populate a data structure the same shape as that below accordingly.

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
