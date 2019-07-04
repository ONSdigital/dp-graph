package neptune

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

/*
TestCreateTriples validates a low level test input generator utility.
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
TestCreateTestTriples makes sure that a particular intended test input is
what it is expected to be.
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
TestBuildDim2Edition validates an individual function used in the implementation.
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
TestBuildResponse validates an individual function used in the implementation.
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