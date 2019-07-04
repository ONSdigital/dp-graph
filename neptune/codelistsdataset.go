/*
This module, when combined with codelist.go, provides code that
satisfies the graph.driver.CodeList interface using Gremlin queries into
a Neptune database.

It is dedicated to code to satisfy the GetCodeDatasets() method -
which is sufficiently complex to merit a module (and tests) of its own.
*/
package neptune

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-code-list-api/models"
	"github.com/ONSdigital/dp-graph/neptune/query"
)

/*
GetCodeDatasets searches the database for datasets that are associated with
the given code list, code, and code list edition. Specifically those that
satisfy all of:
    1) code lists that match the requested code list ID.
    2) code lists of the requested edition.
    3) codes that match the requested code value.
    4) datasets that are related to qualifying codes by *inDataset* edges.
    5) datasets that have the *isPublished* state true.

Each such result from the database (potentially) has the properties:
    - dimensionName (what the dataset calls this dimension)
    - datasetEdition
    - version

The results however include all permuations of dimensionName and 
datasetEdition - BUT ONLY CITES the most recent dataset *version* of those 
found for that permuation.

*/
func (n *NeptuneDB) GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*models.Datasets, error) {

	// Emit the query and parse the responses.
	qry := fmt.Sprintf(query.GetCodeDatasets, codeListID, edition, code)
	responses, err := n.getStringList(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin GetCodeDatasets failed: %q", qry)
	}

	// Isolate the individual records from the flattened response.
	// [['dim', 'edition', 'version'], ['dim', 'edition', ...]]
	responseTriples, err := createTriples(responses)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot create triples.")
	}

	// Build datastructure to capture only latest dataset versions.
	dimensionNameToEditions, err := buildDim2Edition(responseTriples)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot isolate latest versions.")
	}

	// Package up the model-ised response.
	response := buildResponse(dimensionNameToEditions, code, codeListID)
	return response, nil
}

/*
createTriples splits a list of strings into clumps of 3
*/
func createTriples(responses []string) ([][]string, error) {
	var responseTriples = [][]string{}
	const stride = 3 // I.e. dimesionName, edition, version
	if len(responses)%stride != 0 {
		return nil, errors.New("List length is not divisible by 3")
	}
	nInstances := len(responses) / stride
	for i := 0; i < nInstances; i++ {
		offset := i * stride
		dimensionName := responses[offset+0]
		dataSetEdition := responses[offset+1]
		versionStr := responses[offset+2]
		responseTriples = append(responseTriples, []string{dimensionName, dataSetEdition, versionStr})
	}
	return responseTriples, nil
}

// These (nested) maps track the latest version cited by any combination
// of dimensionName and dataset edition.

type editionToLatestVersion map[string]int
type dim2Edition map[string]editionToLatestVersion

/*
buildDim2Edition consumes a list of triples such as
["dimName1", "datasetEdition1", "version4"], and builds a dim2Edition
structure based on the latest versions available for each combination of
dimension name and dataset edition.
*/
func buildDim2Edition(responseTriples [][]string) (dim2Edition, error) {
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
	return d2e, nil
}

/*
buildResponse is capable of consuming a dim2Edition data structure, along
with a few other query parameters, and from these, building the data
structure model hierchy required by the GetCodeDatasets API method.
*/
func buildResponse(d2e dim2Edition, code string, codeListID string) *models.Datasets {
	datasets := &models.Datasets{
		Items:      []models.Dataset{},
		Count:      len(d2e),
		Limit:      len(d2e),
		TotalCount: len(d2e),
	}
	for dimensionName, e2v := range d2e {
		datasetLinks := &models.DatasetLinks{Self: &models.Link{ID: code}}
		dataset := models.Dataset{
			Links:          datasetLinks,
			DimensionLabel: dimensionName,
			Editions:       []models.DatasetEdition{},
		}
		for dataSetEdition, version := range e2v {
			versionStr := fmt.Sprintf("%d", version)
			edition := models.DatasetEdition{}
			edition.Links = &models.DatasetEditionLinks{
				Self:             &models.Link{ID: dataSetEdition},
				LatestVersion:    &models.Link{ID: versionStr},
				DatasetDimension: &models.Link{ID: codeListID},
			}
			dataset.Editions = append(dataset.Editions, edition)
		}
		datasets.Items = append(datasets.Items, dataset)
	}
	return datasets
}
