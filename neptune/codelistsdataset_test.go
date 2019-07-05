package neptune

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-graph/neptune/internal"
	. "github.com/smartystreets/goconvey/convey"
)

/*
TestCreateTriples validates a helper utility function used by the API method.
*/
func TestCreateTriples(t *testing.T) {
	Convey("Given an input list of 6 strings", t, func() {
		input := []string{"a", "b", "c", "d", "e", "f"}
		Convey("When getTriples() is called", func() {
			triples, err := createTriples(input)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the 3-member clumps should be properly constructed", func() {
				So(triples[0], ShouldResemble, []string{"a", "b", "c"})
				So(triples[1], ShouldResemble, []string{"d", "e", "f"})
			})
		})
	})
	Convey("Given an empty input list", t, func() {
		input := []string{}
		Convey("When getTriples() is called", func() {
			triples, err := createTriples(input)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then an empty list should be returned", func() {
				So(triples, ShouldHaveLength, 0)
			})
		})
	})
	Convey("Given a list with length that is not divisible by 3", t, func() {
		input := []string{"a"}
		Convey("When getTriples() is called", func() {
			_, err := createTriples(input)
			Convey("Then an appropriate error should be returned", func() {
				expectedErr := "List length is not divisible by 3"
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
}

/*
TestCreateTestTriples validates a helper utility function used by the API method.
*/
func TestCreateTestTriples(t *testing.T) {
	Convey("When createTestTriples() is alled", t, func() {
		triples := makeTestTriples()
		Convey("Then the returned [][]string structure should be composed correctly", func() {
			So(triples, ShouldHaveLength, 8)
			// Take a couple of samples.
			So(triples[3], ShouldResemble, []string{"dim0", "edition1", "1"})
			So(triples[6], ShouldResemble, []string{"dim1", "edition1", "0"})
		})
	})
}

/*
TestBuildDim2Edition validates a helper utility function used by the API method.
*/
func TestBuildDim2Edition(t *testing.T) {
	Convey("Given a 2 * 2 * 2 combinatorial input", t, func() {
		inputTriples := makeTestTriples()
		Convey("When buildDim2Edition() is called", func() {
			d2e, err := buildDim2Edition(inputTriples)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the returned data structure should be properly constructed", func() {
				So(d2e, ShouldHaveLength, 2)
				So(d2e["dim0"], ShouldHaveLength, 2)
				So(d2e["dim1"], ShouldHaveLength, 2)
				// Take some samples.
				latestVersion := d2e["dim0"]["edition0"]
				So(latestVersion, ShouldEqual, 1)
			})
		})
	})
}

/*
TestBuildResponse validates a helper utility function used by the API method.
*/
func TestBuildResponse(t *testing.T) {
	Convey("Given triples derived from a 2 * 2 * 2 combinatorial input", t, func() {
		inputTriples := makeTestTriples()
		Convey("When you call buildDim2Edition with them", func() {
			d2e, err := buildDim2Edition(inputTriples)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
				Convey("Then when buildResponse() is called using these datastructures", func() {
					codeValue := "testCodeValue"
					codeListID := "testCodeListID"
					response := buildResponse(d2e, codeValue, codeListID)
					Convey("Then the response should be well formed", func() {
						So(response.Count, ShouldEqual, 2)
						So(response.Items, ShouldHaveLength, 2)
						dataset := response.Items[1]
						// Whether dim0 or dim1 is returned first is not deterministic.
						// In repeat test runs if flips seemingly randomly.
						// so we use the ShouldBeIn assertion.
						So(dataset.DimensionLabel, ShouldBeIn, []string{"dim0", "dim1"})
						So(dataset.Links.Self.ID, ShouldEqual, "testCodeValue")
						editions := dataset.Editions
						So(editions, ShouldHaveLength, 2)
						datasetEdition := editions[1]
						So(datasetEdition.Links.Self.ID, ShouldBeIn, []string{"edition0", "edition1"})
						So(datasetEdition.Links.LatestVersion.ID, ShouldEqual, "1")
					})
				})
			})
		})
	})
}

/*
makeTestTriples returns 8 lists of strings, in this pattern:
[["dim0", "edition0", "0"], ["dim0", "edition0", "1"], ...]
With all the permutations of the numeric suffix in {0|1}.
*/
func makeTestTriples() [][]string {
	triples := [][]string{}
	for i := 0; i < 2; i++ {
		dimName := fmt.Sprintf("dim%d", i)
		for j := 0; j < 2; j++ {
			edition := fmt.Sprintf("edition%d", j)
			for k := 0; k < 2; k++ {
				version := fmt.Sprintf("%d", k)
				triples = append(triples, []string{dimName, edition, version})
			}
		}
	}
	return triples
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
				So(err.Error(), ShouldContainSubstring, "MALFORMED REQUEST")
				So(err.Error(), ShouldContainSubstring, "g.V()")
			})
		})
	})
	Convey("Given a database that returns a list of strings indivisible by 3", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnFiveStrings,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeDatasets is called", func() {
			_, err := db.GetCodeDatasets(context.Background(), "unusedCodeListID", "unusedEdition", "unusedCode")
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldContainSubstring, "Cannot create triples")
			})
		})
	})
	Convey("Given a database that returns non-integer version strings", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnStringTripleWithNonIntegerThirdElement,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeDatasets is called", func() {
			_, err := db.GetCodeDatasets(context.Background(), "unusedCodeListID", "unusedEdition", "unusedCode")
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldContainSubstring,
					`Cannot isolate latest versions.: Cannot cast version ("fibble") to int: strconv.Ato`)
			})
		})
	})
	Convey("Given a database that returns well-formed mocked triples", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnProperlyFormedDatasetTriple,
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
                                __.as('r').outV().has('value','unusedCode').as('c'),
                                __.as('c').out('inDataset').as('d').
                                    select('d').values('edition').as('de').
                                    select('d').values('version').as('dv'),
                                __.as('d').has('is_published',true)).
                            union(select('rl', 'de', 'dv')).unfold().select(values)

                            `)
					actualQry := calls[0].Query
					So(stripWhitespace(actualQry), ShouldEqual, expectedQry)
				})
			})
			Convey("Then the returned results should reflect the hard coded mocked database responses", func() {
				So(response, ShouldNotBeNil)
				dataset := response.Items[0]
				So(dataset.DimensionLabel, ShouldEqual, "exampleDimName")
				So(dataset.Links.Self.ID, ShouldEqual, "unusedCode")
				editions := dataset.Editions
				So(editions, ShouldHaveLength, 1)
				datasetEdition := editions[0]
				So(datasetEdition.Links.Self.ID, ShouldEqual, "exampleDatasetEdition")
				So(datasetEdition.Links.LatestVersion.ID, ShouldEqual, "3")
			})
		})
	})
}

func stripWhitespace(in string) string {
	return strings.Join(strings.Fields(in), "")
}
