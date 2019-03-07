package neo4j

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-graph/neo4j/internal"
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
var closeNoErr = func(ctx context.Context) error {
	return nil
}

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
