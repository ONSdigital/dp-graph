package neptune

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-graph/v3/neptune/internal"
	"github.com/ONSdigital/gremgo-neptune"
	"github.com/pkg/errors"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNeptuneDB_CreateCodeRelationship(t *testing.T) {

	createPoolMock := func() *internal.NeptunePoolMock {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
				return []string{"_code_codeListID_codeID"}, nil
			},
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return []gremgo.Response{}, nil
			},
		}
		return poolMock
	}

	ctx := context.Background()
	instanceID := "instanceID"
	codeListID := "codeListID"
	code := "code"

	expectedGetDimStmt := "g.V().hasLabel('_code').has('value',\"code\").where(out('usedBy').hasLabel('_code_list').has('listID','codeListID')).id()"
	expectedCreateDimStmt := "g.V('_instanceID_Instance').as('i').V('_code_codeListID_codeID').addE('inDataset').to('i')"

	Convey("Given an empty instance ID", t, func() {
		instanceID := ""
		poolMock := createPoolMock()
		db := mockDB(poolMock)

		Convey("When CreateCodeRelationship is called", func() {

			err := db.CreateCodeRelationship(ctx, instanceID, codeListID, code)

			Convey("Then the expected err returned", func() {
				expectedErr := "instance id is required but was empty"
				So(err.Error(), ShouldEqual, expectedErr)
			})

			Convey("Then the graph DB is not called", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 0)
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an empty code", t, func() {
		code := ""
		poolMock := createPoolMock()
		db := mockDB(poolMock)

		Convey("When CreateCodeRelationship is called", func() {

			err := db.CreateCodeRelationship(ctx, instanceID, codeListID, code)

			Convey("Then the expected err returned", func() {
				expectedErr := "error creating relationship from instance to code: code is required but was empty"
				So(err.Error(), ShouldEqual, expectedErr)
			})

			Convey("Then the graph DB is not called", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 0)
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given a code that does not exist", t, func() {

		poolMock := createPoolMock()
		poolMock.GetStringListFunc = func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
			return []string{}, nil
		}
		db := mockDB(poolMock)

		Convey("When CreateCodeRelationship is called", func() {

			err := db.CreateCodeRelationship(ctx, instanceID, codeListID, code)

			Convey("Then the graph DB is queried to see if the code exists", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedGetDimStmt)
			})

			Convey("Then the expected error is returned", func() {
				expectedErr := "error creating relationship from instance to code: code or code list not found: map[code:code code_list:codeListID instance_id:instanceID]"
				So(err.Error(), ShouldEqual, expectedErr)
			})

			Convey("Then the graph DB is not called to insert the instance to code relationship", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an error is returned from the code lookup", t, func() {

		poolMock := createPoolMock()
		expectedErr := errors.New(" INVALID REQUEST ARGUMENTS ") // specific error that does not trigger retries
		poolMock.GetStringListFunc = func(query string, bindings map[string]string, rebindings map[string]string) ([]string, error) {
			return nil, expectedErr
		}
		db := mockDB(poolMock)

		Convey("When CreateCodeRelationship is called", func() {

			err := db.CreateCodeRelationship(ctx, instanceID, codeListID, code)

			Convey("Then the graph DB is queried to see if the code exists", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedGetDimStmt)
			})

			Convey("Then the expected error is returned", func() {
				So(err, ShouldEqual, expectedErr)
			})

			Convey("Then the graph DB is not called to insert the instance to code relationship", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 0)
			})
		})
	})

	Convey("Given an error is returned when adding the relationship to the DB", t, func() {

		poolMock := createPoolMock()
		expectedErr := errors.New(" INVALID REQUEST ARGUMENTS ") // specific error that does not trigger retries
		poolMock.ExecuteFunc = func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
			return nil, expectedErr
		}
		db := mockDB(poolMock)

		Convey("When CreateCodeRelationship is called", func() {

			err := db.CreateCodeRelationship(ctx, instanceID, codeListID, code)

			Convey("Then the graph DB is queried to see if the code exists", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedGetDimStmt)
			})

			Convey("Then the graph DB is called to insert the instance to code relationship", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 1)
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedCreateDimStmt)
			})

			Convey("Then the expected error is returned", func() {
				So(err, ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given an existing code", t, func() {

		poolMock := createPoolMock()
		db := mockDB(poolMock)

		Convey("When CreateCodeRelationship is called", func() {

			err := db.CreateCodeRelationship(ctx, instanceID, codeListID, code)

			Convey("Then the graph DB is queried to see if the code exists", func() {
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedGetDimStmt)
			})

			Convey("Then the graph DB is called to insert the instance to code relationship", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 1)
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedCreateDimStmt)
			})

			Convey("Then the err returned is nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
