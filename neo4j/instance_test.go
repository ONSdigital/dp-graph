package neo4j

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-graph/models"
	"github.com/ONSdigital/dp-graph/neo4j/internal"
	"github.com/ONSdigital/dp-graph/neo4j/query"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testInstanceID = "666"
	testDatasetId  = "123"
	testEdition    = "2018"
	testVersion    = 1
)

var errTest = errors.New("test")

var errorMock = errors.New("I am Expected")

func Test_AddVersionDetailsToInstanceSuccess(t *testing.T) {
	Convey("AddVersionDetailsToInstance completes successfully", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": int64(3),
					},
				}
			},
		}
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		err := db.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)
		So(err, ShouldBeNil)

		So(len(driver.ExecCalls()), ShouldEqual, 1)
		So(driver.ExecCalls()[0].Params, ShouldResemble, map[string]interface{}{
			"dataset_id": testDatasetId,
			"edition":    testEdition,
			"version":    testVersion,
		})
		So(len(res.MetadataCalls()), ShouldEqual, 1)
	})
}

func Test_AddVersionDetailsToInstanceError(t *testing.T) {
	Convey("given Exec returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errTest
			},
		}

		db := &Neo4j{driver, 5, 30}

		err := db.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient AddVersionDetailsToInstance: error executing neo4j update statement"))
			So(len(driver.ExecCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result.Metadata() stats are not as expected", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": "invalid stats",
				}
			},
		}
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		err := db.AddVersionDetailsToInstance(context.Background(), testInstanceID, testDatasetId, testEdition, testVersion)

		Convey("then the expected error is returned", func() {
			So(err.Error(), ShouldContainSubstring, "neoClient AddVersionDetailsToInstance: invalid results")
			So(len(driver.ExecCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})
}

func Test_SetInstanceIsPublishedSuccess(t *testing.T) {
	Convey("SetInstanceIsPublished completes successfully", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": int64(1),
					},
				}
			},
		}
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		err := db.SetInstanceIsPublished(context.Background(), testInstanceID)
		So(err, ShouldBeNil)
		So(len(driver.ExecCalls()), ShouldEqual, 1)
		So(driver.ExecCalls()[0].Params, ShouldBeNil)

		So(len(res.MetadataCalls()), ShouldEqual, 1)
	})
}

func Test_SetInstanceIsPublishedError(t *testing.T) {

	Convey("given Exec returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errTest
			},
		}

		db := &Neo4j{driver, 5, 30}

		err := db.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err, ShouldResemble, errors.WithMessage(errTest, "neoClient SetInstanceIsPublished: error executing neo4j update statement"))
			So(len(driver.ExecCalls()), ShouldEqual, 1)
		})
	})

	Convey("given result.Metadata() stats are not as expected", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": "invalid stats",
				}
			},
		}
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return res, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		err := db.SetInstanceIsPublished(context.Background(), testInstanceID)

		Convey("then the expected error is returned", func() {
			So(err.Error(), ShouldContainSubstring, "neoClient SetInstanceIsPublished: invalid results")
			So(len(driver.ExecCalls()), ShouldEqual, 1)
			So(len(res.MetadataCalls()), ShouldEqual, 1)
		})
	})
}

func Test_AddDimensions(t *testing.T) {

	Convey("Given Neo4j.Exec returns an error", t, func() {
		dimensionNames := []interface{}{"one", "two", "three", "four"}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, p map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}

		db := &Neo4j{neo4jMock, 5, 30}

		instance := &models.Instance{
			InstanceID: instanceID,
			Dimensions: dimensionNames,
		}

		Convey("When AddDimensions is called", func() {
			err := db.AddDimensions(context.Background(), instance)

			Convey("Then the expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.Exec returned an error").Error())
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedStmt := fmt.Sprintf(query.AddInstanceDimensions, instanceID)
				So(calls[0].Query, ShouldEqual, expectedStmt)

				expectedParams := map[string]interface{}{"dimensions_list": dimensionNames}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})

	Convey("Given Neo4j.Exec does not return an error", t, func() {
		dimensionNames := []interface{}{"one", "two", "three", "four"}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		db := &Neo4j{neo4jMock, 5, 30}

		instance := &models.Instance{
			InstanceID: instanceID,
			Dimensions: dimensionNames,
		}

		Convey("When AddDimensions is called", func() {
			err := db.AddDimensions(context.Background(), instance)

			Convey("Then no error is returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedStmt := fmt.Sprintf(query.AddInstanceDimensions, instanceID)
				So(calls[0].Query, ShouldEqual, expectedStmt)

				expectedParams := map[string]interface{}{"dimensions_list": dimensionNames}
				So(calls[0].Params, ShouldResemble, expectedParams)
			})
		})
	})
}

func Test_CreateInstance(t *testing.T) {
	Convey("Given a Neo4j.Exec returns an error", t, func() {
		instance := &models.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}

		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateInstance is invoked", func() {
			err := db.CreateInstance(context.Background(), instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.Exec returned an error").Error())
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateInstance, instanceID, strings.Join(instance.CSVHeader, ","))
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})

	Convey("Given a Neo4j.Exec returns no error", t, func() {
		instance := &models.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateInstance is invoked", func() {
			err := db.CreateInstance(context.Background(), instance)

			Convey("Then no error is returned", func() {
				So(err, ShouldResemble, nil)
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateInstance, instanceID, strings.Join(instance.CSVHeader, ","))
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})
}

func Test_CreateInstanceConstraint_StatementError(t *testing.T) {

	Convey("Given mock Neo4j client that returns an error", t, func() {

		instance := &models.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}

		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateInstanceConstraint is invoked", func() {

			err := db.CreateInstanceConstraint(context.Background(), instance)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.Exec returned an error when creating observation constraint").Error())
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateInstanceObservationConstraint, instanceID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})
}

func Test_CreateInstanceConstraint(t *testing.T) {

	Convey("Given mock Neo4j client that returns no error", t, func() {

		instance := &models.Instance{
			InstanceID: instanceID,
			CSVHeader:  []string{"the", "csv", "header"},
		}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, nil
			},
		}

		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateInstanceConstraint is invoked", func() {

			err := db.CreateInstanceConstraint(context.Background(), instance)

			Convey("Then no error is returned", func() {
				So(err, ShouldEqual, nil)
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateInstanceObservationConstraint, instanceID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldEqual, nil)
			})
		})
	})
}

func Test_CreateCodeRelationship(t *testing.T) {

	codeListID := "432"
	code := "123"
	instance := &models.Instance{
		InstanceID: instanceID,
	}

	Convey("Given an empty code", t, func() {

		code := ""

		neo4jMock := &internal.Neo4jDriverMock{}
		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := db.CreateCodeRelationship(context.Background(), instance, codeListID, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.New("code is required but was empty").Error())
			})

			Convey("And Neo4j.Exec is never called", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a Neo4j.Exec returns an error", t, func() {

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errorMock
			},
		}
		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := db.CreateCodeRelationship(context.Background(), instance, codeListID, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.Exec returned an error").Error())
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateInstanceToCodeRelationship, instance.InstanceID, codeListID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldResemble, map[string]interface{}{
					"code": code,
				})
			})
		})
	})

	Convey("Given that result.RowsAffected returns an error", t, func() {

		resultMock := &internal.ResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return -1, errorMock
			},
		}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return resultMock, nil
			},
		}

		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := db.CreateCodeRelationship(context.Background(), instance, codeListID, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "result.RowsAffected() returned an error").Error())
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateInstanceToCodeRelationship, instance.InstanceID, codeListID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldResemble, map[string]interface{}{
					"code": code,
				})
			})
		})
	})

	Convey("Given that result.RowsAffected is not 1", t, func() {

		resultMock := &internal.ResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 0, nil
			},
		}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return resultMock, nil
			},
		}

		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := db.CreateCodeRelationship(context.Background(), instance, codeListID, code)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "unexpected number of rows affected. expected 1 but was 0")
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateInstanceToCodeRelationship, instance.InstanceID, codeListID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldResemble, map[string]interface{}{
					"code": code,
				})
			})
		})
	})

	Convey("Given a Neo4j.Exec returns no error", t, func() {

		resultMock := &internal.ResultMock{
			RowsAffectedFunc: func() (int64, error) {
				return 1, nil
			},
		}

		neo4jMock := &internal.Neo4jDriverMock{
			ExecFunc: func(query string, params map[string]interface{}) (bolt.Result, error) {
				return resultMock, nil
			},
		}
		db := Neo4j{neo4jMock, 5, 30}

		Convey("When CreateCodeRelationship is invoked", func() {
			err := db.CreateCodeRelationship(context.Background(), instance, codeListID, code)

			Convey("Then no error is returned", func() {
				So(err, ShouldResemble, nil)
			})

			Convey("And Neo4j.Exec is called 1 time with the expected parameters", func() {
				calls := neo4jMock.ExecCalls()
				So(len(calls), ShouldEqual, 1)

				expectedQuery := fmt.Sprintf(query.CreateInstanceToCodeRelationship, instance.InstanceID, codeListID)
				So(calls[0].Query, ShouldEqual, expectedQuery)
				So(calls[0].Params, ShouldResemble, map[string]interface{}{
					"code": code,
				})
			})
		})
	})
}

func Test_InstanceExists(t *testing.T) {
	Convey("Given the repository has been configured correctly", t, func() {
		neoMock := &internal.Neo4jDriverMock{
			CountFunc: func(query string) (int64, error) {
				return 1, nil
			},
		}

		db := Neo4j{neoMock, 5, 30}

		Convey("When InstanceExists is invoked for an existing instance", func() {
			exists, err := db.InstanceExists(context.Background(), instance)

			Convey("Then reposity returns the expected result", func() {
				So(exists, ShouldBeTrue)
				So(err, ShouldBeNil)
			})

			Convey("And neo4j.Count is called 1 time with the expected parameters", func() {
				So(len(neoMock.CountCalls()), ShouldEqual, 1)

				countStmt := fmt.Sprintf(query.CountInstance, instance.InstanceID)
				So(neoMock.CountCalls()[0].Query, ShouldEqual, countStmt)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neoMock.ExecCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given neo4j.Count returns an error", t, func() {
		neoMock := &internal.Neo4jDriverMock{
			CountFunc: func(query string) (int64, error) {
				return 0, errorMock
			},
		}

		db := Neo4j{neoMock, 5, 30}

		Convey("When InstanceExists is invoked for an existing instance", func() {
			exists, err := db.InstanceExists(context.Background(), instance)

			Convey("Then the error is propegated back to the caller", func() {
				So(exists, ShouldBeFalse)
				So(err.Error(), ShouldEqual, errors.Wrap(errorMock, "neo4j.Count returned an error").Error())
			})

			Convey("And neo4j.Count is called 1 time with the expected parameters", func() {
				So(len(neoMock.CountCalls()), ShouldEqual, 1)

				countStmt := fmt.Sprintf(query.CountInstance, instance.InstanceID)
				So(neoMock.CountCalls()[0].Query, ShouldEqual, countStmt)
			})

			Convey("And there are no other calls to neo4j", func() {
				So(len(neoMock.ExecCalls()), ShouldEqual, 0)
			})
		})
	})
}

func Test_checkPropertiesSet(t *testing.T) {
	Convey("given a mocked result affecting 1 property", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": int64(1),
					},
				}
			},
		}

		Convey("when checkPropertiesSet is called expecting 1 change", func() {
			err := checkPropertiesSet(res, 1)

			Convey("then no error is returned", func() {
				So(err, ShouldBeNil)
				So(len(res.MetadataCalls()), ShouldEqual, 1)
			})
		})
	})

	Convey("given result.Metadata() stats are not as expected", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": "invalid stats",
				}
			},
		}

		Convey("when checkPropertiesSet is called expecting 1 change", func() {
			err := checkPropertiesSet(res, 1)

			Convey("then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "error getting query result stats")
				So(len(res.MetadataCalls()), ShouldEqual, 1)
			})
		})
	})

	Convey("given result stats do not contain 'properties-set'", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{},
				}
			},
		}

		Convey("when checkPropertiesSet is called expecting 1 change", func() {
			err := checkPropertiesSet(res, 1)

			Convey("then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "error verifying query results")
				So(len(res.MetadataCalls()), ShouldEqual, 1)
			})
		})
	})

	Convey("given result stats properties-set is not the expected format", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": "1",
					},
				}
			},
		}

		Convey("when checkPropertiesSet is called expecting 1 change", func() {
			err := checkPropertiesSet(res, 1)

			Convey("then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "error verifying query results")
				So(len(res.MetadataCalls()), ShouldEqual, 1)
			})
		})
	})

	Convey("given result stats properties-set is not the expected value", t, func() {
		res := &internal.ResultMock{
			MetadataFunc: func() map[string]interface{} {
				return map[string]interface{}{
					"stats": map[string]interface{}{
						"properties-set": int64(666),
					},
				}
			},
		}

		Convey("when checkPropertiesSet is called expecting 1 change", func() {
			err := checkPropertiesSet(res, 1)

			Convey("then the expected error is returned", func() {
				So(err.Error(), ShouldEqual, "unexpected rows affected expected 1 but was 666")
				So(len(res.MetadataCalls()), ShouldEqual, 1)
			})
		})
	})
}
