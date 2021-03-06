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

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
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

The results however include all permutations of dimensionName and
datasetEdition - BUT ONLY CITES the most recent dataset *version* of those
found for that permutation.

*/
func (n *NeptuneDB) GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*models.Datasets, error) {

	// Emit the query and parse the responses.
	qry := fmt.Sprintf(query.GetCodeDatasets, codeListID, edition, code)
	responses, err := n.getStringList(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin GetCodeDatasets failed: %q", qry)
	}

	// Isolate the individual records from the flattened response.
	// [['dim', 'edition', 'version', 'datasetID'], ['dim', 'edition', ...]]
	responseRecords, err := createCodeDatasetRecords(responses)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot create records.")
	}

	// Build data structure to capture only latest dataset versions.
	latestVersionMaps, err := buildLatestVersionMaps(responseRecords)
	if err != nil {
		return nil, errors.Wrap(err, "Cannot isolate latest versions.")
	}

	// Package up the model-ised response.
	response := buildResponse(latestVersionMaps, code, codeListID)
	return response, nil
}

/*
createCodeDatasetRecords splits a list of strings into clumps of 4
*/
func createCodeDatasetRecords(responses []string) ([][]string, error) {
	const valuesPerRecord = 4
	return createRecords(responses, valuesPerRecord)
}

// These (nested) maps track the latest version cited by any combination
// of dimensionName, dataset edition, and datasetID.
// They are all keyed on strings and the nested assembly can be accessed
// like this:
// latestVersion = foo[datasetID][dimension][edition]

type editionToLatestVersion map[string]int
type dim2Edition map[string]editionToLatestVersion
type datasetID2Dim map[string]dim2Edition

/*
buildLatestVersionMaps consumes a list of records such as
["dimName1", "datasetEdition1", "version4", "datasetID3"], and builds a datasetID2Dim
structure based on the latest versions available for each combination of
dimension name, dataset edition, and datasetID.
*/
func buildLatestVersionMaps(responseRecords [][]string) (datasetID2Dim, error) {
	did2Dim := datasetID2Dim{}

	for _, record := range responseRecords {
		dimensionName := record[0]
		datasetEdition := record[1]
		versionStr := record[2]
		datasetID := record[3]

		versionInt, err := strconv.Atoi(versionStr)
		if err != nil {
			return nil, errors.Wrapf(err, "Cannot cast version (%q) to int", versionStr)
		}
		if _, ok := did2Dim[datasetID]; !ok {
			did2Dim[datasetID] = dim2Edition{}
		}
		if _, ok := did2Dim[datasetID][dimensionName]; !ok {
			did2Dim[datasetID][dimensionName] = editionToLatestVersion{}
		}
		latestKnownV, ok := did2Dim[datasetID][dimensionName][datasetEdition]
		if !ok || latestKnownV < versionInt {
			did2Dim[datasetID][dimensionName][datasetEdition] = versionInt
		}
	}
	return did2Dim, nil
}

/*
buildResponse is capable of consuming a datasetID2Dim data structure, along
with a few other query parameters, and from these, building the data
structure model hierchy required by the GetCodeDatasets API method.
*/
func buildResponse(did2Dim datasetID2Dim, code string, codeListID string) *models.Datasets {
	datasets := &models.Datasets{
		Items: []models.Dataset{},
	}
	for datasetID, dim2E := range did2Dim {
		for dimensionName, e2v := range dim2E {
			dataset := models.Dataset{
				ID:             datasetID,
				DimensionLabel: dimensionName,
				Editions:       []models.DatasetEdition{},
			}

			for datasetEdition, version := range e2v {
				edition := models.DatasetEdition{
					ID:            datasetEdition,
					CodeListID:    codeListID,
					LatestVersion: version,
				}

				dataset.Editions = append(dataset.Editions, edition)
			}
			datasets.Items = append(datasets.Items, dataset)
		}
	}
	return datasets
}
