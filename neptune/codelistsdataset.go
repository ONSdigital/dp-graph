/*
This module, when combined with codelist.go, provides code that
satisfies the graph.driver.CodeList interface using Gremlin queries into
a Neptune database.

This module is dedicated to code to satisfy the GetCodeDatasets() method -
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

func (n *NeptuneDB) GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*models.Datasets, error) {

	// Emit the query and parse the responses.
	qry := fmt.Sprintf(query.GetCodeDatasets, codeListID, edition, code)
	responses, err := n.getStringList(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin GetCodeDatasets failed: %q", qry)
	}

	// Isolate the individual records
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

	// Build the request response from the reduced data
	for dimensionName, e2v := range dimensionNameToEditions {
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

// These (nested) maps offer to track the latest version cited by any combination
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
