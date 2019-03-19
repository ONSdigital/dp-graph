package neo4j

import (
	"context"
	"fmt"
	"testing"

	"github.com/ONSdigital/dp-graph/neo4j/driver"
	"github.com/ONSdigital/dp-graph/neo4j/internal"
	"github.com/ONSdigital/dp-graph/observation"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	. "github.com/smartystreets/goconvey/convey"
)

var closeNoErr = func() error {
	return nil
}

var mockConnNoErr = &internal.BoltConnMock{
	CloseFunc: closeNoErr,
}

func TestStore_StreamCSVRows(t *testing.T) {

	Convey("Given an store with a mock DB connection", t, func() {

		filter := &observation.Filter{
			InstanceID: "888",
			DimensionFilters: []*observation.DimensionFilter{
				{Name: "age", Options: []string{"29", "30"}},
				{Name: "sex", Options: []string{"male", "female"}},
			},
		}

		expectedQuery := "MATCH (i:`_888_Instance`) RETURN i.header as row " +
			"UNION ALL " +
			"MATCH (o)-[:isValueOf]->(`age`:`_888_age`), (o)-[:isValueOf]->(`sex`:`_888_sex`) " +
			"WHERE (`age`.value='29' OR `age`.value='30') " +
			"AND (`sex`.value='male' OR `sex`.value='female') " +
			"RETURN o.value AS row"

		expectedCSVRow := "the,csv,row"

		mockBoltRows := &internal.BoltRowsMock{
			CloseFunc: closeNoErr,
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRow}, nil, nil
			},
		}

		driver := &internal.Neo4jDriverMock{
			StreamRowsFunc: func(query string) (*driver.BoltRowReader, error) {
				return driver.NewBoltRowReader(mockBoltRows, mockConnNoErr), nil
			},
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When StreamCSVRows is called without a limit", func() {

			rowReader, err := db.StreamCSVRows(context.Background(), filter, nil)

			Convey("The expected query is sent to the database", func() {

				actualQuery := driver.StreamRowsCalls()[0].Query

				So(len(driver.StreamRowsCalls()), ShouldEqual, 1)
				So(actualQuery, ShouldEqual, expectedQuery)
			})

			Convey("There is a row reader returned for the rows given by the database.", func() {
				So(err, ShouldBeNil)
				So(rowReader, ShouldNotBeNil)
			})
		})

		Convey("When StreamCSVRows is called with a limit of 20", func() {

			limitRows := 20
			rowReader, err := db.StreamCSVRows(context.Background(), filter, &limitRows)

			Convey("The expected query is sent to the database", func() {

				actualQuery := driver.StreamRowsCalls()[0].Query

				So(len(driver.StreamRowsCalls()), ShouldEqual, 1)
				So(actualQuery, ShouldEqual, expectedQuery+" LIMIT 20")
			})

			Convey("There is a row reader returned for the rows given by the database.", func() {
				So(err, ShouldBeNil)
				So(rowReader, ShouldNotBeNil)
			})
		})
	})
}

func TestStore_StreamCSVRowsEmptyFilter(t *testing.T) {
	filterID := "1234567890"
	InstanceID := "0987654321"

	expectedQuery := fmt.Sprintf("MATCH (i:`_%s_Instance`) RETURN i.header as row UNION ALL "+
		"MATCH(o: `_%s_observation`) return o.value as row", InstanceID, InstanceID)

	Convey("Given valid database connection", t, func() {

		expectedCSVRowHeader := "the,csv,row"
		expectedCSVRowData := "1,2,3"

		mockBoltRows := &internal.BoltRowsMock{
			CloseFunc: closeNoErr,
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRowHeader, expectedCSVRowData}, nil, nil
			},
		}

		driver := &internal.Neo4jDriverMock{
			StreamRowsFunc: func(query string) (*driver.BoltRowReader, error) {
				return driver.NewBoltRowReader(mockBoltRows, mockConnNoErr), nil
			},
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When StreamCSVRows is called a filter with nil dimensionFilters and no limit", func() {
			filter := &observation.Filter{
				FilterID:         filterID,
				InstanceID:       InstanceID,
				DimensionFilters: nil,
			}

			result, err := db.StreamCSVRows(context.Background(), filter, nil)
			assertEmptyFilterResults(result, expectedCSVRowHeader, err)
			assertEmptyFilterQueryInvocations(driver, expectedQuery)
		})

		Convey("When StreamCSVRows is called a filter with empty dimensionFilters and no limit", func() {
			filter := &observation.Filter{
				FilterID:         filterID,
				InstanceID:       InstanceID,
				DimensionFilters: []*observation.DimensionFilter{},
			}

			result, err := db.StreamCSVRows(context.Background(), filter, nil)
			assertEmptyFilterResults(result, expectedCSVRowHeader, err)
			assertEmptyFilterQueryInvocations(driver, expectedQuery)
		})

		Convey("When StreamCSVRows is called a filter with a list of empty dimensionFilters and no limit", func() {
			filter := &observation.Filter{
				FilterID:   filterID,
				InstanceID: InstanceID,
				DimensionFilters: []*observation.DimensionFilter{
					&observation.DimensionFilter{
						Name:    "",
						Options: []string{},
					},
				},
			}

			result, err := db.StreamCSVRows(context.Background(), filter, nil)
			assertEmptyFilterResults(result, expectedCSVRowHeader, err)
			assertEmptyFilterQueryInvocations(driver, expectedQuery)
		})
	})
}

func TestStore_StreamCSVRowsDimensionEmpty(t *testing.T) {

	Convey("Given an store with a mock DB connection", t, func() {

		filter := &observation.Filter{
			InstanceID: "888",
			DimensionFilters: []*observation.DimensionFilter{
				{Name: "age", Options: []string{"29", "30"}},
				{Name: "sex", Options: []string{}},
			},
		}

		expectedCSVRow := "the,csv,row"

		mockBoltRows := &internal.BoltRowsMock{
			CloseFunc: closeNoErr,
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRow}, nil, nil
			},
		}

		driver := &internal.Neo4jDriverMock{
			StreamRowsFunc: func(query string) (*driver.BoltRowReader, error) {
				return driver.NewBoltRowReader(mockBoltRows, mockConnNoErr), nil
			},
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When StreamCSVRows is called with a filter with an empty dimension options and no limit", func() {

			expectedQuery := "MATCH (i:`_888_Instance`) RETURN i.header as row " +
				"UNION ALL " +
				"MATCH (o)-[:isValueOf]->(`age`:`_888_age`) " +
				"WHERE (`age`.value='29' OR `age`.value='30') " +
				"RETURN o.value AS row"

			rowReader, err := db.StreamCSVRows(context.Background(), filter, nil)

			Convey("Then the expected query is sent to the database", func() {

				actualQuery := driver.StreamRowsCalls()[0].Query

				So(len(driver.StreamRowsCalls()), ShouldEqual, 1)
				So(actualQuery, ShouldEqual, expectedQuery)
			})

			Convey("There is a row reader returned for the rows given by the database.", func() {
				So(err, ShouldBeNil)
				So(rowReader, ShouldNotBeNil)
			})
		})
	})
}

func assertEmptyFilterResults(reader observation.StreamRowReader, expectedCSVRow string, err error) {
	Convey("The expected result is returned with no error", func() {
		So(err, ShouldBeNil)
		row, _ := reader.Read()
		So(row, ShouldEqual, expectedCSVRow+"\n")
	})
}

func assertEmptyFilterQueryInvocations(d *internal.Neo4jDriverMock, expectedQuery string) {
	Convey("Then the expected query is sent to the database one time", func() {
		So(len(d.StreamRowsCalls()), ShouldEqual, 1)
		So(d.StreamRowsCalls()[0].Query, ShouldEqual, expectedQuery)
	})
}
