package neptune

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

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

func TestBuildDim2Edition(t *testing.T) {
	Convey("Given a 2 * 2 * 2 combinatorial input", t, func() {
		inputTriples := makeTestTriples()
		Convey("When buildDim2Edition() is called", func() {
			d2e, err := buildDim2Edition(inputTriples)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the returned data structure should be properly constructed", func() {
				latestVersion := d2e["dim0"]["edition0"]
				So(latestVersion, ShouldEqual, 1)
			})
		})
	})
}

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

// add other combinations
// add empty input
// add error handling
