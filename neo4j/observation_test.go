package neo4j

import (
	"context"
	"errors"
	"fmt"
	"testing"

	graph "github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/neo4j/internal"
	driver "github.com/ONSdigital/dp-graph/neo4j/neo4jdriver"
	"github.com/ONSdigital/dp-graph/observation"
	"github.com/ONSdigital/dp-observation-importer/models"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	neoErrors "github.com/ONSdigital/golang-neo4j-bolt-driver/errors"
	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/messages"
	. "github.com/smartystreets/goconvey/convey"
)

var closeNoErr = func() error {
	return nil
}

var mockConnNoErr = &internal.BoltConnMock{
	CloseFunc: closeNoErr,
}

func Test_StreamCSVRows(t *testing.T) {

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

func Test_StreamCSVRowsEmptyFilter(t *testing.T) {
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

func Test_StreamCSVRowsDimensionEmpty(t *testing.T) {

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

var validObservation = &models.Observation{
	InstanceID: "123",
	Row:        "the,row,content",
	RowIndex:   5678,
	DimensionOptions: []*models.DimensionOption{
		{DimensionName: "sex", Name: "Male"},
		{DimensionName: "age", Name: "45"},
	},
}

var uncachedObservation = &models.Observation{
	InstanceID: "234",
	Row:        "the,row,content",
	RowIndex:   456,
	DimensionOptions: []*models.DimensionOption{
		{DimensionName: "sex", Name: "Male"},
		{DimensionName: "age", Name: "45"},
	},
}

var ids = map[string]string{
	"123_sex_Male": "333",
	"123_age_45":   "666",
}

func Test_InsertObservationBatch(t *testing.T) {
	attempt := 1
	instanceID := "123"

	expectedQuery := "UNWIND $rows AS row" +
		" MATCH (`sex`:`_123_sex`), (`age`:`_123_age`)" +
		" WHERE id(`sex`) = toInt(row.`sex`) AND id(`age`) = toInt(row.`age`)" +
		" CREATE (o:`_123_observation` { value:row.v, rowIndex:row.i }), (o)-[:isValueOf]->(`sex`), (o)-[:isValueOf]->(`age`)"

	expectedParams := make(map[string]interface{})
	rows := make([]interface{}, 0)
	rows = append(rows, map[string]interface{}{
		"v":   "the,row,content",
		"i":   int64(5678),
		"sex": "333",
		"age": "666",
	},
		map[string]interface{}{
			"v":   "the,row,content",
			"i":   int64(5678),
			"sex": "333",
			"age": "666",
		},
		map[string]interface{}{
			"v":   "the,row,content",
			"i":   int64(5678),
			"sex": "333",
			"age": "666",
		},
	)

	expectedParams["rows"] = rows

	obs := []*models.Observation{validObservation, validObservation, validObservation}

	Convey("Given a valid database connection and observations that exist in the dimenson map", t, func() {

		res := &internal.ResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 1, nil
			},
		}

		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(context.Background(), attempt, instanceID, obs, ids)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
				So(driver.ExecCalls()[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given a valid database connection but an empty observation list", t, func() {
		res := &internal.ResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 1, nil
			},
		}

		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}
		empty := []*models.Observation{}

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(context.Background(), attempt, instanceID, empty, ids)

			Convey("Then an error should be returned and the database should not be called", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "failed to create query for batch")
				So(len(driver.ExecCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a valid database connection but an empty dimenson map", t, func() {

		res := &internal.ResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 1, nil
			},
		}

		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(context.Background(), attempt, instanceID, obs, make(map[string]string))

			Convey("Then an error should be returned and the database should not be called", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "failed to create query parameters for batch query")
				So(len(driver.ExecCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a flakey database connection and valid parameters", t, func() {
		res := &internal.ResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 1, nil
			},
		}

		count := 0
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				if count == 0 {
					count++
					msg := make(map[string]interface{})
					msg["code"] = "Neo.TransientError.Network.CommunicationError"
					return nil, neoErrors.Wrap(messages.NewFailureMessage(msg), "transient error")
				}
				count++
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(context.Background(), attempt, instanceID, obs, ids)

			Convey("Then no error should be returned but the database should be called twice", func() {
				So(err, ShouldBeNil)
				So(len(driver.ExecCalls()), ShouldEqual, 2)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
				So(driver.ExecCalls()[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given a malformed results but valid parameters", t, func() {
		res := &internal.ResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 0, errors.New("something went wrong")
			},
		}

		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(context.Background(), attempt, instanceID, obs, ids)

			Convey("Then an error should be returned and the database should only be called once", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "error attempting to get number of rows affected in query result")
				So(err.Error(), ShouldContainSubstring, "something went wrong")
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
				So(driver.ExecCalls()[0].Params, ShouldResemble, expectedParams)
				So(len(res.RowsAffectedCalls()), ShouldEqual, 1)
			})
		})
	})

	Convey("Given a complete failure in database connection but valid parameters", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, graph.ErrNonRetriable{errors.New("non retriable")}
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When InsertObservationBatch is called", func() {
			err := db.InsertObservationBatch(context.Background(), attempt, instanceID, obs, ids)

			Convey("Then no error should be returned but the database should be called twice", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "observation batch save failed")
				So(err.Error(), ShouldContainSubstring, "non retriable")
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})
		})
	})

}

func Test_createParams(t *testing.T) {

	Convey("Given a populated dimension cache and a single valid observation", t, func() {
		Convey("When create params is called with observations that exist in the cache", func() {
			results, err := createParams([]*models.Observation{validObservation}, ids)

			Convey("Then the returned map should contain new rows", func() {
				rows := make([]interface{}, 0)
				rows = append(rows, map[string]interface{}{
					"v":   "the,row,content",
					"i":   int64(5678),
					"sex": "333",
					"age": "666",
				})

				So(err, ShouldBeNil)
				So(results, ShouldNotBeNil)
				So(results["rows"], ShouldResemble, rows)

			})
		})
	})

	Convey("Given a populated dimension cache and a list of 3 valid observations", t, func() {
		obs := []*models.Observation{validObservation, validObservation, validObservation}

		Convey("When create params is called with observations that exist in the cache", func() {
			results, err := createParams(obs, ids)

			Convey("Then the returned map should contain 3 new rows", func() {
				rows := make([]interface{}, 0)
				rows = append(rows, map[string]interface{}{
					"v":   "the,row,content",
					"i":   int64(5678),
					"sex": "333",
					"age": "666",
				},
					map[string]interface{}{
						"v":   "the,row,content",
						"i":   int64(5678),
						"sex": "333",
						"age": "666",
					},
					map[string]interface{}{
						"v":   "the,row,content",
						"i":   int64(5678),
						"sex": "333",
						"age": "666",
					})

				So(err, ShouldBeNil)
				So(results, ShouldNotBeNil)
				So(results["rows"], ShouldResemble, rows)

			})
		})
	})

	Convey("Given a populated dimension cache and a list containing an uncached observation", t, func() {
		obs := []*models.Observation{validObservation, uncachedObservation, validObservation}

		Convey("When create params is called and observations cant be found in the cache", func() {
			results, err := createParams(obs, ids)

			Convey("Then the returned map should be empty and an error returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errors.New("No nodeId found for 234_sex_Male"))
				So(results, ShouldBeNil)
			})
		})
	})

	Convey("Given an empty dimension cache and a list of valid observations", t, func() {
		obs := []*models.Observation{validObservation, validObservation, validObservation}

		Convey("When create params is called and observations cant be found in the cache", func() {
			results, err := createParams(obs, make(map[string]string))

			Convey("Then the returned map should be empty and an error returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errors.New("No nodeId found for 123_sex_Male"))
				So(results, ShouldBeNil)
			})
		})
	})

	Convey("Given a populated dimension cache and no observations", t, func() {
		Convey("When create params is called and no observations exist", func() {
			results, err := createParams([]*models.Observation{}, ids)

			Convey("Then the returned map should not contain new rows", func() {
				So(err, ShouldBeNil)
				So(results, ShouldNotBeNil)
				So(results["rows"], ShouldBeEmpty)
			})
		})
	})

}

func Test_buildInsertObservationQuery(t *testing.T) {
	Convey("Given an instance ID and observations for one dimension", t, func() {
		id := "123"
		obs := []*models.Observation{
			&models.Observation{
				InstanceID: "123",
				DimensionOptions: []*models.DimensionOption{
					&models.DimensionOption{
						DimensionName: "age",
					},
				},
			},
		}

		Convey("When buildInsertObservationQuery is called", func() {
			r := buildInsertObservationQuery(id, obs)

			Convey("Then a valid query string is returned", func() {
				So(r, ShouldNotBeEmpty)
				So(r, ShouldEqual, "UNWIND $rows AS row MATCH (`age`:`_123_age`) WHERE id(`age`) = toInt(row.`age`) CREATE (o:`_123_observation` { value:row.v, rowIndex:row.i }), (o)-[:isValueOf]->(`age`)")
			})
		})
	})

	Convey("Given an instance ID and observations for two dimensions", t, func() {
		id := "123"
		obs := []*models.Observation{
			&models.Observation{
				InstanceID: "123",
				DimensionOptions: []*models.DimensionOption{
					&models.DimensionOption{
						DimensionName: "age",
					},
					&models.DimensionOption{
						DimensionName: "time",
					},
				},
			},
		}

		Convey("When buildInsertObservationQuery is called", func() {
			r := buildInsertObservationQuery(id, obs)

			Convey("Then a valid query string is returned", func() {
				So(r, ShouldNotBeEmpty)
				So(r, ShouldEqual, "UNWIND $rows AS row MATCH (`age`:`_123_age`), (`time`:`_123_time`) WHERE id(`age`) = toInt(row.`age`) AND id(`time`) = toInt(row.`time`) CREATE (o:`_123_observation` { value:row.v, rowIndex:row.i }), (o)-[:isValueOf]->(`age`), (o)-[:isValueOf]->(`time`)")
			})
		})
	})

	Convey("Given no instance ID and a valid list of observations", t, func() {
		id := ""
		obs := []*models.Observation{
			&models.Observation{
				InstanceID: "123",
				DimensionOptions: []*models.DimensionOption{
					&models.DimensionOption{
						DimensionName: "age",
					},
				},
			},
		}

		Convey("When buildInsertObservationQuery is called", func() {
			r := buildInsertObservationQuery(id, obs)

			Convey("Then an empty string is returned", func() {
				So(r, ShouldBeEmpty)
			})
		})
	})

	Convey("Given an instance ID but no observations", t, func() {
		id := "123"
		obs := []*models.Observation{}

		Convey("When buildInsertObservationQuery is called", func() {
			r := buildInsertObservationQuery(id, obs)

			Convey("Then an empty string is returned", func() {
				So(r, ShouldBeEmpty)
			})
		})
	})

	Convey("Given no instance ID or observations", t, func() {
		id := "123"
		obs := []*models.Observation{}

		Convey("When buildInsertObservationQuery is called", func() {
			r := buildInsertObservationQuery(id, obs)

			Convey("Then an empty string is returned", func() {
				So(r, ShouldBeEmpty)
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
