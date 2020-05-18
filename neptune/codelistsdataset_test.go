package neptune

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	. "github.com/smartystreets/goconvey/convey"
)

/*
TestCreateTriples validates a helper utility function used by the API method.
*/
func TestCreateTriples(t *testing.T) {
	Convey("Given an input list of 8 strings", t, func() {
		input := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
		Convey("When createCodeDatasetRecords() is called", func() {
			triples, err := createCodeDatasetRecords(input)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the 4-member clumps should be properly constructed", func() {
				So(triples[0], ShouldResemble, []string{"a", "b", "c", "d"})
				So(triples[1], ShouldResemble, []string{"e", "f", "g", "h"})
			})
		})
	})
	Convey("Given an empty input list", t, func() {
		input := []string{}
		Convey("When createCodeDatasetRecords() is called", func() {
			triples, err := createCodeDatasetRecords(input)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then an empty list should be returned", func() {
				So(triples, ShouldHaveLength, 0)
			})
		})
	})
	Convey("Given a list with length that is not divisible by 4", t, func() {
		input := []string{"a"}
		Convey("When createCodeDatasetRecords() is called", func() {
			_, err := createCodeDatasetRecords(input)
			Convey("Then an appropriate error should be returned", func() {
				expectedErr := "list length is not divisible by 4"
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
}

/*
TestCreateTestTriples validates a helper utility function used by the API method.
*/
func TestMakeTestRecords(t *testing.T) {
	Convey("When createTestTriples() is alled", t, func() {
		records := makeTestRecords()
		Convey("Then the returned [][]string structure should be composed correctly", func() {
			So(records, ShouldHaveLength, 16)
			// Take a couple of samples.
			So(records[3], ShouldResemble, []string{"dim0", "edition0", "1", "dataset1"})
			So(records[13], ShouldResemble, []string{"dim1", "edition1", "0", "dataset1"})
			So(records[14], ShouldResemble, []string{"dim1", "edition1", "1", "dataset0"})
		})
	})
}

/*
TestBuildLatestVersionMaps validates a helper utility function used by the API method.
*/
func TestBuildLatestVersionMaps(t *testing.T) {
	// Recall the map schema...
	// latestVersion = foo[datasetID][dimension][edition]

	Convey("Given a 2 * 2 * 2 * 2 combinatoria	l input", t, func() {
		inputRecords := makeTestRecords()
		Convey("When buildLatestVersionMaps() is called", func() {
			did2Dim, err := buildLatestVersionMaps(inputRecords)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the returned data structure should be properly constructed", func() {
				So(did2Dim, ShouldHaveLength, 2)
				So(did2Dim["dataset0"], ShouldHaveLength, 2)
				So(did2Dim["dataset0"]["dim0"], ShouldHaveLength, 2)
				// Take a sample
				latestVersion := did2Dim["dataset0"]["dim0"]["edition0"]
				So(latestVersion, ShouldEqual, 1)
			})
		})
	})
}

/*
TestBuildResponse validates a helper utility function used by the API method.
*/
func TestBuildResponse(t *testing.T) {
	Convey("Given records derived from a 2 * 2 * 2 * 2 combinatorial input", t, func() {
		inputRecords := makeTestRecords()
		Convey("When you call buildLatestVersionMaps with them", func() {
			did2Dim, err := buildLatestVersionMaps(inputRecords)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
				Convey("Then when buildResponse() is called using these datastructures", func() {
					codeValue := "testCodeValue"
					codeListID := "testCodeListID"
					response := buildResponse(did2Dim, codeValue, codeListID)
					Convey("Then the response should be well formed", func() {
						So(response.Items, ShouldHaveLength, 4)
						dataset := response.Items[1]
						// The order in which the responses come back is not
						// deterministic.
						// In repeat test runs if flips seemingly randomly.
						// so we use the ShouldBeIn assertion.
						So(dataset.DimensionLabel, ShouldBeIn, []string{"dim0", "dim1"})
						So(dataset.ID, ShouldBeIn, []string{"dataset0", "dataset1"})
						editions := dataset.Editions
						So(editions, ShouldHaveLength, 2)
						datasetEdition := editions[1]
						So(datasetEdition.ID, ShouldBeIn, []string{"edition0", "edition1"})
						So(datasetEdition.LatestVersion, ShouldEqual, 1)
					})
				})
			})
		})
	})
}

/*
makeTestRecords returns 8 lists of strings, in this pattern:
[["dim0", "edition0", "0", "dataset0"], ["dim0", "edition0", "1", "dataset1"], ...]
With all the permutations of the numeric suffix in {0|1}.
*/
func makeTestRecords() [][]string {
	records := [][]string{}
	for i := 0; i < 2; i++ {
		dimName := fmt.Sprintf("dim%d", i)
		for j := 0; j < 2; j++ {
			edition := fmt.Sprintf("edition%d", j)
			for k := 0; k < 2; k++ {
				version := fmt.Sprintf("%d", k)
				for m := 0; m < 2; m++ {
					datasetID := fmt.Sprintf("dataset%d", m)
					records = append(records, []string{dimName, edition, version, datasetID})
				}
			}
		}
	}
	return records
}

/*
TestGetCodeDatasetsAtAPILevel operates the GetCodeDatasets method at high
level with a mocked database - to validate the code in the high level method
alone.
*/
func TestGetCodeDatasetsAtAPILevel(t *testing.T) {
	Convey("Given a database that raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnMalformedStringListRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeDatasets is called", func() {
			_, err := db.GetCodeDatasets(context.Background(), "unusedCodeListID", "unusedEdition", "unusedCode")
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "MALFORMED REQUEST")
				So(err.Error(), ShouldContainSubstring, "g.V()")
			})
		})
	})
	Convey("Given a database that returns a list of strings indivisible by 4", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnFiveStrings,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeDatasets is called", func() {
			_, err := db.GetCodeDatasets(context.Background(), "unusedCodeListID", "unusedEdition", "unusedCode")
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "Cannot create records")
			})
		})
	})
	Convey("Given a database that returns non-integer version strings", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnStringRecordWithNonIntegerFourthElement,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeDatasets is called", func() {
			_, err := db.GetCodeDatasets(context.Background(), "unusedCodeListID", "unusedEdition", "unusedCode")
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring,
					`Cannot isolate latest versions.: Cannot cast version ("fibble") to int: strconv.Ato`)
			})
		})
	})
	Convey("Given a database that returns well-formed mocked records", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnProperlyFormedDatasetRecord,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeDatasets is called", func() {
			response, err := db.GetCodeDatasets(context.Background(), "unusedCodeListID", "unusedEdition", "unusedCode")
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver GetStringList function should be called once", func() {
				calls := poolMock.GetStringListCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := stripWhitespace(`

						g.V().hasLabel('_code_list').has('listID', 'unusedCodeListID').
						has('edition','unusedEdition').
						inE('usedBy').as('r').values('label').as('rl').select('r').
						match(
							__.as('r').outV().has('value',"unusedCode").as('c'),
							__.as('c').out('inDataset').as('d').
								select('d').values('edition').as('de').
								select('d').values('version').as('dv'),
								select('d').values('dataset_id').as('did').
							__.as('d').has('is_published',true)).
						union(select('rl', 'de', 'dv', 'did')).unfold().select(values)
                    `)
					actualQry := calls[0].Query
					So(stripWhitespace(actualQry), ShouldEqual, expectedQry)
				})
			})
			Convey("Then the returned results should reflect the hard coded mocked database responses", func() {
				So(response, ShouldNotBeNil)
				dataset := response.Items[0]
				So(dataset.DimensionLabel, ShouldEqual, "exampleDimName")
				So(dataset.ID, ShouldEqual, "exampleDatasetID")
				editions := dataset.Editions
				So(editions, ShouldHaveLength, 1)
				datasetEdition := editions[0]
				So(datasetEdition.ID, ShouldEqual, "exampleDatasetEdition")
				So(datasetEdition.LatestVersion, ShouldEqual, 3)
			})
		})
	})
}

func stripWhitespace(in string) string {
	return strings.Join(strings.Fields(in), "")
}
