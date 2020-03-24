package neptune

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
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
					expectedQry := `g.V().hasLabel('_code_list').has('listID_2', true)`
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
				`).has('unusedFilter', true)":  MALFORMED REQUEST `
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
			GetCountFunc: internal.ReturnMalformedIntRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetEdition(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `Gremlin query failed: "g.V().hasLabel('_code_list').has('listID', ` +
				`'unused-id').has('edition', 'unused-edition').count()":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
	Convey("Given a database that returns zero editions", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnZero}
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
	Convey("Given a database that returns two editions", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnTwo}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetEdition(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then the returned error should be ErrMultipleFound", func() {
				So(err, ShouldEqual, driver.ErrMultipleFound)
			})
		})
	})
	Convey("Given a database that returns one edition", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnOne}
		db := mockDB(poolMock)
		Convey("When GetEdition() is called", func() {
			listID := "listId_1"
			requestedEdition := "my-test-edition"
			editionResponse, err := db.GetEdition(context.Background(), listID, requestedEdition)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver GetCount function should be called once", func() {
				calls := poolMock.GetCountCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := `g.V().hasLabel('_code_list').has('listID', 'listId_1').has(` +
						`'edition', 'my-test-edition').count()`
					actualQry := calls[0].Q
					So(actualQry, ShouldEqual, expectedQry)
				})
			})
			Convey("Then a non nil structure returned", func() {
				So(editionResponse, ShouldNotBeNil)
			})
			Convey("Then the ID field should be set right", func() {
				So(editionResponse.Edition, ShouldEqual, "my-test-edition")
			})
		})
	})
}

func TestGetEditions(t *testing.T) {
	Convey("Given a database that raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnMalformedNilInterfaceRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetEditions() is called", func() {
			unusedCodeListID := "unused-id"
			_, err := db.GetEditions(context.Background(), unusedCodeListID)
			expectedErr := `Gremlin query failed: "g.V().hasLabel('_code_list').has('listID', ` +
				`'unused-id')":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
	Convey("Given a database that returns zero edition vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{GetFunc: internal.ReturnZeroVertices}
		db := mockDB(poolMock)
		Convey("When GetEditions() is called", func() {
			unusedCodeListID := "unused-id"
			_, err := db.GetEditions(context.Background(), unusedCodeListID)
			Convey("Then the returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	})
	Convey("Given a database that returns three edition vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{GetFunc: internal.ReturnThreeEditionVertices}
		db := mockDB(poolMock)
		Convey("When GetEditions() is called", func() {
			unusedCodeListID := "unused-id"
			editionsResponse, err := db.GetEditions(context.Background(), unusedCodeListID)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver Get function should be called once", func() {
				calls := poolMock.GetCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := `g.V().hasLabel('_code_list').has('listID', 'unused-id')`
					actualQry := calls[0].Query
					So(actualQry, ShouldEqual, expectedQry)
					Convey("Then a non nil structure returned", func() {
						So(editionsResponse, ShouldNotBeNil)
						So(len(editionsResponse.Items), ShouldEqual, 3)
						Convey("Then set right", func() {
							sampleEdition := editionsResponse.Items[1]
							So(sampleEdition.Edition, ShouldEqual, "edition_1")
						})
					})
				})
			})
		})
	})
}

func TestGetCodes(t *testing.T) {
	Convey("Given a database that raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnMalformedNilInterfaceRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `Gremlin query failed: "g.V().hasLabel('_code_list').has('listID', 'unused-id').` +
				`has('edition', 'unused-edition').in('usedBy').hasLabel('_code')":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
	Convey("Given a database that returns zero code vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{GetFunc: internal.ReturnZeroVertices}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then the returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	})
	Convey("Given a database that provides malformed code vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetFunc: internal.ReturnThreeUselessVertices,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			_, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `Error reading "value" property on Code vertex: property not found`
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})
	Convey("Given a database that returns three code vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{GetFunc: internal.ReturnThreeCodeVertices}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			unusedCodeListID := "unused-id"
			unusedEdition := "unused-edition"
			codesResponse, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver Get function should be called once", func() {
				calls := poolMock.GetCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := `g.V().hasLabel('_code_list').has('listID', 'unused-id').has('edition', ` +
						`'unused-edition').in('usedBy').hasLabel('_code')`
					actualQry := calls[0].Query
					So(actualQry, ShouldEqual, expectedQry)
					Convey("Then a non nil structure returned", func() {
						So(codesResponse, ShouldNotBeNil)
						So(len(codesResponse.Items), ShouldEqual, 3)
						Convey("Then set right", func() {
							sampleCode := codesResponse.Items[1]
							So(sampleCode.Code, ShouldEqual, "code_1")
						})
					})
				})
			})
		})
	})
}

func TestGetCode(t *testing.T) {
	Convey("Given a database that will return that the Code exists", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnOne}
		db := mockDB(poolMock)
		Convey("When GetCode() is called", func() {
			unusedCodeList := "unused-code-list"
			unusedEdition := "unused-edition"
			unusedCode := "unused-code"
			code, err := db.GetCode(context.Background(), unusedCodeList, unusedEdition, unusedCode)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver GetCount function should be called once", func() {
				calls := poolMock.GetCountCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := `g.V().hasLabel('_code_list').has('listID', 'unused-code-list').has('edition', ` +
						`'unused-edition').in('usedBy').has('value', 'unused-code').count()`
					actualQry := calls[0].Q
					So(actualQry, ShouldEqual, expectedQry)
				})
			})
			Convey("Then a non nil structure returned", func() {
				So(code, ShouldNotBeNil)
			})
		})
	})

	Convey("Given a database that raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc: internal.ReturnMalformedIntRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCode() is called", func() {
			unusedCodeList := "unused-code-list"
			unusedEdition := "unused-edition"
			unusedCode := "unused-code"
			_, err := db.GetCode(context.Background(), unusedCodeList, unusedEdition, unusedCode)
			expectedErr := `Gremlin query failed: "g.V().hasLabel('_code_list').has('listID', ` +
				`'unused-code-list').has('edition', 'unused-edition').in('usedBy').has('value', ` +
				`'unused-code').count()":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given a database that returns that the Code does not exist", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnZero}
		db := mockDB(poolMock)
		Convey("When GetCode() is called", func() {
			unusedCodeList := "unused-code-list"
			unusedEdition := "unused-edition"
			unusedCode := "unused-code"
			_, err := db.GetCode(context.Background(), unusedCodeList, unusedEdition, unusedCode)
			Convey("Then the returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	})

	Convey("Given a database that returns that multiple Codes exist", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnTwo}
		db := mockDB(poolMock)
		Convey("When GetCode() is called", func() {
			unusedCodeList := "unused-code-list"
			unusedEdition := "unused-edition"
			unusedCode := "unused-code"
			_, err := db.GetCode(context.Background(), unusedCodeList, unusedEdition, unusedCode)
			Convey("Then the returned error should should object to there being multiple", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, driver.ErrMultipleFound)
			})
		})
	})
}
