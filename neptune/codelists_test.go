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
					expectedQry := "g.V().hasLabel('_code_list')"
					actualQry := calls[0].Query
					So(actualQry, ShouldEqual, expectedQry)
				})
			})
			Convey("Then the returned results should reflect the hard coded CodeListIDs", func() {
				So(codeLists, ShouldNotBeNil)
				So(len(codeLists.Items), ShouldEqual, 3)
				codeList := codeLists.Items[2]
				So(codeList.ID, ShouldEqual, "listID_2")
				links := codeList.Links
				So(links.Self.ID, ShouldEqual, "listID_2")
			})
		})

		Convey("When GetCodeLists() is called *with* a filterBy param", func() {
			filterBy := "listID_2"
			_, err := db.GetCodeLists(context.Background(), filterBy)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the driver GetVertices function should be called once", func() {
				calls := poolMock.GetCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a different (more-qualified) query string", func() {
					expectedQry := `g.V().hasLabel('_code_list').has('listID_2', 'true')`
					actualQry := calls[0].Query
					So(actualQry, ShouldEqual, expectedQry)
				})
			})
		})
	})

	Convey("Given a database that raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnMalformedNilInterfaceRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeLists() is called", func() {
			filterBy := "unusedFilter"
			_, err := db.GetCodeLists(context.Background(), filterBy)
			expectedErr := `Gremlin query failed: "g.V().hasLabel('_code_list'` +
				`).has('unusedFilter', 'true')":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given a database that provides malformed code list vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnThreeUselessVertices,
		}
		db := mockDB(poolMock)
		Convey("When GetCodeLists() is called", func() {
			filterBy := "unusedFilter"
			_, err := db.GetCodeLists(context.Background(), filterBy)
			Convey("Then an error should be raised about the missing ListID property", func() {
				expectedErr := `Error reading "listID" property on Code List vertex: property not found`
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
			GetCountFunc: internal.ReturnMalformedIntRequestErr,
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
				So(err, ShouldEqual, driver.ErrMultipleFound)
			})
		})
	})
}

func TestGetEdition(t *testing.T) {
	Convey("Given a database that raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnMalformedNilInterfaceRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetEdition(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `Gremlin query failed: "g.V().hasLabel('_code_list').has('listID', 'unused-id')":  ` +
				`MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
	Convey("Given a database that returns zero vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnZeroVertices,
		}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetEdition(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then the returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	})
	Convey("Given a database that returns vertices that don't have an edition property", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnThreeUselessVertices,
		}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetEdition(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then the returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNoSuchProperty)
			})
		})
	})
	Convey("Given a database that returns vertices with an edition property of the wrong type", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnThreeCodeListsWithWronglyTypedEdition,
		}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetEdition(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `GetProperty("edition") failed: property value could not be cast into expected type`
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
	Convey("Given a database that returns only CodeList(s) with unwanted editions", t, func() {
		poolMock := &internal.NeptunePoolMock{
			// This sets edition to "my-test-edition"
			GetFunc: internal.ReturnThreeCodeLists,
		}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			unusedCodeListID := "unused-id"
			requiredEdition := "different-from-my-test-edition"
			_, err := db.GetEdition(context.Background(), unusedCodeListID, requiredEdition)
			Convey("Then the returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	})
	Convey("Given a database that returns three matching CodeList(s)", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnThreeIdenticalCodeLists,
		}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			matchingCodeListID := "listId_1"
			matchingEdition := "my-test-edition"
			_, err := db.GetEdition(context.Background(), matchingCodeListID, matchingEdition)
			Convey("Then the returned error should be ErrMultipleFound", func() {
				So(err, ShouldEqual, driver.ErrMultipleFound)
			})
		})
	})
	Convey("Given a database that returns one matching CodeList(s)", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnOneWellFormedCodeList,
		}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			matchingCodeListID := "listId_1"
			matchingEdition := "my-test-edition"
			codeList, err := db.GetEdition(context.Background(), matchingCodeListID, matchingEdition)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver Getfunction should be called once", func() {
				calls := poolMock.GetCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := `g.V().hasLabel('_code_list').has('listID', 'listId_1')`
					actualQry := calls[0].Query
					So(actualQry, ShouldEqual, expectedQry)
				})
			})
			Convey("Then a non nil structure returned", func() {
				So(codeList, ShouldNotBeNil)
			})
			Convey("Then the fibble field should be set right", func() {
				So(codeList.Links.Self.ID, ShouldEqual, "my-test-edition")
			})
		})
	})
}
