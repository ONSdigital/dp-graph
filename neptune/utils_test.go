package neptune

import (
	"sync"
	"testing"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateMap(t *testing.T) {

	Convey("Given two string array with duplicated values", t, func() {
		a := []string{"0", "1", "2", "2"}
		b := []string{"0", "3", "3"}

		Convey("Then createMap returns a map of empty structs where the keys are the union of all array items", func() {
			m := createInterfaceMapFromArrays(a, b)
			So(m, ShouldResemble, map[string]interface{}{"0": struct{}{}, "1": struct{}{}, "2": struct{}{}, "3": struct{}{}})
		})
	})
}

func TestCreateArray(t *testing.T) {

	Convey("Given an empty struct map", t, func() {
		m := map[string]interface{}{"0": nil, "1": nil, "2": nil}

		Convey("Then createArray returns an array of strings containing the keys, in any order", func() {
			a := createArray(m)
			So(a, ShouldHaveLength, 3)
			So(a, ShouldContain, "0")
			So(a, ShouldContain, "1")
			So(a, ShouldContain, "2")
		})
	})
}

func TestUnique(t *testing.T) {

	Convey("Given a string array with duplicated values", t, func() {
		a := []string{"0", "1", "2", "0"}

		Convey("Then unique returns an array of unique values from the original array", func() {
			b := unique(a)
			So(b, ShouldHaveLength, 3)
			So(b, ShouldContain, "0")
			So(b, ShouldContain, "1")
			So(b, ShouldContain, "2")
		})
	})
}

func validateAllItems(expectedItems map[string]interface{}, processedChunks []map[string]interface{}) {
	for _, chunk := range processedChunks {
		for k, v := range chunk {
			expectedVal, found := expectedItems[k]
			So(found, ShouldBeTrue)
			So(expectedVal, ShouldEqual, v)
		}
	}
}

func TestProcessInBatches(t *testing.T) {

	Convey("Given an array of 10 items and a mock chunk processor function", t, func() {
		items := map[string]interface{}{"0": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9}
		processedChunks := []map[string]interface{}{}
		processor := func(chunk map[string]interface{}) { processedChunks = append(processedChunks, chunk) }

		Convey("Then processing in chunks of size 5 results in the function being called twice with the expected chunks", func() {
			numChunks := processInBatches(items, processor, 5)
			So(numChunks, ShouldEqual, 2)
			So(processedChunks[0], ShouldHaveLength, 5)
			So(processedChunks[1], ShouldHaveLength, 5)
			validateAllItems(items, processedChunks)
		})

		Convey("Then processing in chunks of size 3 results in the function being called four times with the expected chunks, the last one being containing the remaining items", func() {
			numChunks := processInBatches(items, processor, 3)
			So(numChunks, ShouldEqual, 4)
			So(processedChunks[0], ShouldHaveLength, 3)
			So(processedChunks[1], ShouldHaveLength, 3)
			So(processedChunks[2], ShouldHaveLength, 3)
			So(processedChunks[3], ShouldHaveLength, 1)
			validateAllItems(items, processedChunks)
		})
	})
}

func TestInConcurrentBatches(t *testing.T) {

	Convey("Given an array of 10 items", t, func() {
		items := map[string]interface{}{"0": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9}
		processedChunks := []map[string]interface{}{}
		lock := sync.Mutex{}

		Convey("And a successful mock chunk processor function that returns an empty map", func() {
			processor := func(chunk map[string]interface{}) (map[string]interface{}, error) {
				defer lock.Unlock()
				lock.Lock()
				processedChunks = append(processedChunks, chunk)
				return make(map[string]interface{}), nil
			}

			Convey("Then processing the chunks concurrently results in an aggregated empty array, "+
				"the expected number of chunks and no error being returned", func() {
				result, numChunks, errs := processInConcurrentBatches(items, processor, 5, 150)
				So(result, ShouldResemble, make(map[string]interface{}))
				So(numChunks, ShouldEqual, 2)
				So(errs, ShouldBeNil)
				So(processedChunks, ShouldHaveLength, 2)
				So(processedChunks[0], ShouldHaveLength, 5)
				So(processedChunks[1], ShouldHaveLength, 5)
				validateAllItems(items, processedChunks)
			})

			Convey("And an erroring mock chunk processor function", func() {
				testErr := errors.New("testErr")
				processor := func(chunk map[string]interface{}) (map[string]interface{}, error) {
					defer lock.Unlock()
					lock.Lock()
					processedChunks = append(processedChunks, chunk)
					return map[string]interface{}{"shouldBeIgnored": true}, testErr
				}

				Convey("Then processing the chunks concurrently results in all errors being returned", func() {
					result, numChunks, errs := processInConcurrentBatches(items, processor, 5, 150)
					So(result, ShouldResemble, make(map[string]interface{}))
					So(numChunks, ShouldEqual, 2)
					So(errs, ShouldResemble, []error{testErr, testErr})
					So(processedChunks, ShouldHaveLength, 2)
					So(processedChunks[0], ShouldHaveLength, 5)
					So(processedChunks[1], ShouldHaveLength, 5)
					validateAllItems(items, processedChunks)
				})
			})
		})
	})
}

func TestStatementSummary(t *testing.T) {

	Convey("A statement without any list of IDs or codes is summarized to itself", t, func() {
		original := "g.V().hasLabel('_hierarchy_node_instance_dim').id()"
		So(statementSummary(original), ShouldResemble, original)
	})

	Convey("A statement that starts querying a list of vertices by ID is summarized to the same without showing the IDs", t, func() {
		original := "g.V('node1','node2','node3').outE('clone_of').drop()"
		expected := "g.V(...).outE('clone_of').drop()"
		So(statementSummary(original), ShouldResemble, expected)
	})

	Convey("A statement that requests an element 'within' a list is summarized to the same without showing the list of elements", t, func() {
		original := "g.V().hasLabel('_generic_hierarchy_node_output-area-geography').has('code',within(['code1','code2','code3'])).id()"
		expected := "g.V().hasLabel('_generic_hierarchy_node_output-area-geography').has('code',within([...])).id()"
		So(statementSummary(original), ShouldResemble, expected)
	})
}
