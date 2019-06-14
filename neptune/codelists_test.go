package neptune

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/neptune/internal"
)

func TestGetCodeLists(t *testing.T) {
	Convey("Given a database that will return a hard-coded CodeListResults that contains 3 Code Lists", t, func() {
		poolMock := &internal.NeptunePoolMock{GetFunc: internal.ReturnThreeCodeLists}
		db := mockDB(poolMock)
		Convey("When GetCodeLists() is called without a filterBy param", func() {
			filterBy := ""
			codeLists, err := db.GetCodeLists(context.Background(), filterBy)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver GetVertices function should be called once", func() {
				calls := poolMock.GetCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := "wont be this"
					actualQry := calls[0].Query
					So(actualQry, ShouldEqual, expectedQry)
				})
			})
			Convey("Then the returned results should reflect the hard coded CodeListIDs", func() {
				So(codeLists, ShouldNotBeNil)
				// Todo content tests
			})
		})
	})

	Convey("Given a database that raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnMalformedRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeLists() is called with a filterBy param", func() {
			filterBy := "arbitraryFilter"
			_, err := db.GetCodeLists(context.Background(), filterBy)
			expectedErr := "wontbethis"
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
}

func TestGetCodeList(t *testing.T) {
	Convey("Given a database that will return that the CodeList ID exists", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnOne}
		db := mockDB(poolMock)
		Convey("When GetCodeList() is called", func() {
			codeListID := "arbitrary"
			codeList, err := db.GetCodeList(context.Background(), codeListID)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver GetCount function should be called once", func() {
				calls := poolMock.GetCountCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := "g.V().hasLabel('_code_list').has('listID', 'arbitrary').count()"
					actualQry := calls[0].Q
					So(actualQry, ShouldEqual, expectedQry)
				})
			})
			Convey("Then a non nil structure returned", func() {
				So(codeList, ShouldNotBeNil)
			})
		})
	})

	Convey("Given a database that raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc: internal.ReturnMalformedRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeList() is called", func() {
			codeListID := "arbitrary"
			_, err := db.GetCodeList(context.Background(), codeListID)
			expectedErr := `Gremlin query failed: "g.V().hasLabel('_code_list').has('listID', 'arbitrary').count()":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given a database that returns that the CodeList ID does not exist", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnZero}
		db := mockDB(poolMock)
		Convey("When GetCodeList() is called", func() {
			codeListID := "arbitrary"
			_, err := db.GetCodeList(context.Background(), codeListID)
			Convey("Then the returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	})

	Convey("Given a database that returns that multiple CodeLists with this ID exist", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnTwo}
		db := mockDB(poolMock)
		Convey("When GetCodeList() is called", func() {
			codeListID := "arbitrary"
			_, err := db.GetCodeList(context.Background(), codeListID)
			Convey("Then the returned error should should object to there being multiple", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, `Cannot provide a single CodeList because multiple exist with ID "arbitrary"`)
			})
		})
	})
}
