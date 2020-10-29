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
			m := createMap(a, b)
			So(m, ShouldResemble, map[string]struct{}{"0": {}, "1": {}, "2": {}, "3": {}})
		})
	})
}

func TestCreateArray(t *testing.T) {

	Convey("Given an empty struct map", t, func() {
		m := map[string]struct{}{"0": {}, "1": {}, "2": {}}

		Convey("Then createArray returns an array of strings containing the keys, in any order", func() {
			a := createArray(m)
			So(len(a), ShouldEqual, 3)
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
			So(len(b), ShouldEqual, 3)
			So(b, ShouldContain, "0")
			So(b, ShouldContain, "1")
			So(b, ShouldContain, "2")
		})
	})
}

func TestProcessInBatches(t *testing.T) {

	Convey("Given an array of 10 items and a mock chunk processor function", t, func() {
		items := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
		processedChunks := [][]string{}
		processor := func(chunk []string) { processedChunks = append(processedChunks, chunk) }

		Convey("Then processing in chunks of size 5 results in the function being called twice with the expected chunks", func() {
			numChunks := processInBatches(items, processor, 5)
			So(numChunks, ShouldEqual, 2)
			So(processedChunks, ShouldResemble, [][]string{
				{"0", "1", "2", "3", "4"},
				{"5", "6", "7", "8", "9"}})
		})

		Convey("Then processing in chunks of size 3 results in the function being called four times with the expected chunks, the last one being containing the remaining items", func() {
			numChunks := processInBatches(items, processor, 3)
			So(numChunks, ShouldEqual, 4)
			So(processedChunks, ShouldResemble, [][]string{
				{"0", "1", "2"},
				{"3", "4", "5"},
				{"6", "7", "8"},
				{"9"}})
		})
	})
}

func TestInConcurrentBatches(t *testing.T) {

	Convey("Given an array of 10 items", t, func() {
		items := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
		processedChunks := [][]string{}
		lock := sync.Mutex{}
		chunk1 := []string{"0", "1", "2", "3", "4"}
		chunk2 := []string{"5", "6", "7", "8", "9"}

		Convey("And a successful mock chunk processor function that returns an empty array", func() {
			processor := func(chunk []string) ([]string, error) {
				defer lock.Unlock()
				lock.Lock()
				processedChunks = append(processedChunks, chunk)
				return []string{}, nil
			}

			Convey("Then processing the chunks concurrently results in an aggregated empty array, "+
				"the expected number of chunks and no error being returned", func() {
				result, numChunks, errs := processInConcurrentBatches(items, processor, 5)
				So(result, ShouldResemble, make(map[string]struct{}))
				So(numChunks, ShouldEqual, 2)
				So(errs, ShouldBeNil)
				So(len(processedChunks), ShouldEqual, 2)
				So(processedChunks, ShouldContain, chunk1)
				So(processedChunks, ShouldContain, chunk2)
			})

			Convey("And a successful mock chunk processor function that returns duplicated values", func() {
				numCall := 0
				processor := func(chunk []string) ([]string, error) {
					defer lock.Unlock()
					lock.Lock()
					processedChunks = append(processedChunks, chunk)
					numCall++
					if numCall == 1 {
						return []string{"a", "b", "b", "a"}, nil
					}
					return []string{"a", "c", "d", "c"}, nil
				}

				Convey("Then processing the chunks concurrently results in an aggregated array of the union of returned items, "+
					"the expected number of chunks and no error being returned", func() {
					result, numChunks, errs := processInConcurrentBatches(items, processor, 5)
					So(result, ShouldResemble, map[string]struct{}{"a": {}, "b": {}, "c": {}, "d": {}})
					So(numChunks, ShouldEqual, 2)
					So(errs, ShouldBeNil)
					So(len(processedChunks), ShouldEqual, 2)
					So(processedChunks, ShouldContain, chunk1)
					So(processedChunks, ShouldContain, chunk2)
				})
			})

			Convey("And an erroring mock chunk processor function", func() {
				testErr := errors.New("testErr")
				processor := func(chunk []string) ([]string, error) {
					defer lock.Unlock()
					lock.Lock()
					processedChunks = append(processedChunks, chunk)
					return []string{"shouldBeIgnored"}, testErr
				}

				Convey("Then processing the chunks concurrently results in all errors being returned", func() {
					result, numChunks, errs := processInConcurrentBatches(items, processor, 5)
					So(result, ShouldResemble, make(map[string]struct{}))
					So(numChunks, ShouldEqual, 2)
					So(errs, ShouldResemble, []error{testErr, testErr})
					So(len(processedChunks), ShouldEqual, 2)
					So(processedChunks, ShouldContain, chunk1)
					So(processedChunks, ShouldContain, chunk2)
				})
			})
		})
	})
}
