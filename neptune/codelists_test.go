package neptune

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/graphson"
	"github.com/ONSdigital/gremgo-neptune"
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
				So(editionResponse.ID, ShouldEqual, "my-test-edition")
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
							So(sampleEdition.ID, ShouldEqual, "edition_1")
						})
					})
				})
			})
		})
	})
}

func TestCountCodes(t *testing.T) {
	unusedCodeListID := "unused-id"
	unusedEdition := "unused-edition"

	Convey("Given a database that raises a non-transient error when trying to count items", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc: internal.ReturnMalformedIntRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When CountCodes() is called", func() {
			_, err := db.CountCodes(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `Gremlin query failed: "g.V().has('_code_list','listID', 'unused-id').has('edition', 'unused-edition').` +
				`in('usedBy').count()":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given a database that returns a count of two items", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc: internal.ReturnTwo,
		}
		db := mockDB(poolMock)
		Convey("When CountCodes() is called", func() {
			response, err := db.CountCodes(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver Get function should be called once", func() {
				calls := poolMock.GetCountCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := `g.V().has('_code_list','listID', 'unused-id').has('edition', 'unused-edition').` +
						`in('usedBy').count()`
					actualQry := calls[0].Q
					So(actualQry, ShouldEqual, expectedQry)
					Convey("Then a non nil structure returned", func() {
						So(response, ShouldEqual, int64(2))
					})
				})
			})
		})
	})
}

func TestGetCodes(t *testing.T) {
	unusedCodeListID := "unused-id"
	unusedEdition := "unused-edition"

	Convey("Given a database that raises a non-transient error when trying to get the order", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc: internal.ReturnMalformedIntRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			_, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `Gremlin query failed: "g.V().has('_code_list','listID', 'unused-id').has('edition', 'unused-edition').` +
				`inE('usedBy').has('order').count()":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given a database that has no order and raises a non-transient error when trying to get a list of strings", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc:      internal.ReturnZero,
			GetStringListFunc: internal.ReturnMalformedStringListRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			_, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `Gremlin query failed: "g.V().has('_code_list','listID', 'unused-id').has('edition', 'unused-edition').` +
				`inE('usedBy').as('usedBy').outV().order().by('value',asc).as('code').select('usedBy', 'code').by('label').by('value').` +
				`unfold().select(values)":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one containing the unsorted query", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given a database with order and raises a non-transient error when trying to get a list of strings", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc:      internal.ReturnTwo,
			GetStringListFunc: internal.ReturnMalformedStringListRequestErr,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			_, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `Gremlin query failed: "g.V().has('_code_list', 'listID', 'unused-id').has('edition', 'unused-edition').` +
				`inE('usedBy').order().by('order',asc).as('usedBy').outV().as('code').select('usedBy', 'code').by('label').by('value').` +
				`unfold().select(values)":  MALFORMED REQUEST `
			Convey("Then the returned error should wrap the underlying one containing the sorted query", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given a database that returns no code values", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc:      internal.ReturnZero,
			GetStringListFunc: internal.ReturnEmptyCodesList,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			_, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then the returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	})

	Convey("Given a database that provides invalid code values", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc:      internal.ReturnZero,
			GetStringListFunc: internal.ReturnInvalidCodeData,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			_, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			expectedErr := `list length is not divisible by 2`
			Convey("Then the returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	})

	Convey("Given a database that has no order and returns three code vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc:      internal.ReturnZero,
			GetStringListFunc: internal.ReturnThreeCodes,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			codesResponse, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver Get function should be called once", func() {
				calls := poolMock.GetStringListCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := `g.V().has('_code_list','listID', 'unused-id').has('edition', 'unused-edition').` +
						`inE('usedBy').as('usedBy').` +
						`outV().order().by('value',asc).as('code').` +
						`select('usedBy', 'code').by('label').by('value').` +
						`unfold().select(values)`
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

	Convey("Given a database with order that returns three code vertices", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc:      internal.ReturnThree,
			GetStringListFunc: internal.ReturnThreeCodes,
		}
		db := mockDB(poolMock)
		Convey("When GetCodes() is called", func() {
			codesResponse, err := db.GetCodes(context.Background(), unusedCodeListID, unusedEdition)
			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("Then the driver Get function should be called once", func() {
				calls := poolMock.GetStringListCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := `g.V().has('_code_list', 'listID', 'unused-id').has('edition', 'unused-edition').` +
						`inE('usedBy').order().by('order',asc).as('usedBy').` +
						`outV().as('code').` +
						`select('usedBy', 'code').by('label').by('value').` +
						`unfold().select(values)`
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
						`'unused-edition').in('usedBy').has('value', "unused-code").count()`
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
				`\"unused-code\").count()":  MALFORMED REQUEST `
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

func TestGetCodeOrderFromMap(t *testing.T) {

	Convey("Given a valid code-edge map with order, then the values are correctly extracted by getCodeOrderFromMap", t, func() {
		expectedCode := "mar"
		expectedOrder := 2

		m := mockCodeEdgeMap(expectedCode, &expectedOrder)

		code, order, err := getCodeOrderFromMap(m)
		So(err, ShouldBeNil)
		So(code, ShouldEqual, expectedCode)
		So(*order, ShouldEqual, expectedOrder)
	})

	Convey("Given a valid code-edge map without order, then the values are correctly extracted by getCodeOrderFromMap", t, func() {
		expectedCode := "mar"

		m := mockCodeEdgeMap(expectedCode, nil)

		code, order, err := getCodeOrderFromMap(m)
		So(err, ShouldBeNil)
		So(code, ShouldEqual, expectedCode)
		So(order, ShouldBeNil)
	})

	Convey("Given a code-edge map without a key for code, then getCodeOrderFromMap fails with ErrNotFound", t, func() {
		m := map[string]json.RawMessage{
			"usedBy": {},
		}

		_, _, err := getCodeOrderFromMap(m)
		So(err, ShouldResemble, driver.ErrNotFound)
	})

	Convey("Given a code-edge map without a key for usedBy edge, then getCodeOrderFromMap fails with ErrNotFound", t, func() {
		m := map[string]json.RawMessage{
			"code": {},
		}

		_, _, err := getCodeOrderFromMap(m)
		So(err, ShouldResemble, driver.ErrNotFound)
	})

	Convey("Given a code-edge map with an invalid edge value, then getCodeOrderFromMap fails with the expected error", t, func() {
		rawCode, err := json.Marshal("mar")
		So(err, ShouldBeNil)

		m := map[string]json.RawMessage{
			"code":   rawCode,
			"usedBy": {1, 2, 3},
		}

		_, _, err = getCodeOrderFromMap(m)
		So(err.Error(), ShouldResemble, "invalid character '\\x01' looking for beginning of value")
	})

	Convey("Given a code-edge map with an edge without Properties, then getCodeOrderFromMap fails with the expected error", t, func() {
		rawCode, err := json.Marshal("mar")
		So(err, ShouldBeNil)

		edge := mockUsedByEdge(nil)
		edge.Value.Properties = nil
		rawEdge, err := json.Marshal(edge)
		So(err, ShouldBeNil)

		m := map[string]json.RawMessage{
			"code":   rawCode,
			"usedBy": rawEdge,
		}

		_, _, err = getCodeOrderFromMap(m)
		So(err.Error(), ShouldResemble, "unexpected nil Propertie for 'usedBy' edge")
	})

	Convey("Given a code-edge map with an edge with an unexpected order Property type, then getCodeOrderFromMap fails with the expected error", t, func() {
		rawCode, err := json.Marshal("mar")
		So(err, ShouldBeNil)

		edge := mockUsedByEdge(nil)
		edge.Value.Properties["order"] = graphson.EdgeProperty{
			Type: "g:List",
		}
		rawEdge, err := json.Marshal(edge)
		So(err, ShouldBeNil)

		m := map[string]json.RawMessage{
			"code":   rawCode,
			"usedBy": rawEdge,
		}

		_, _, err = getCodeOrderFromMap(m)
		So(err.Error(), ShouldResemble, "DeserializeSingleFromBytes: Expected `g:Int32` type, but got ")
	})
}

func TestGetCodesOrder(t *testing.T) {

	testCodeListID := "mmm"
	testCodes := []string{"mar", "apr"}

	testOrderMar := 2
	testOrderApr := 3

	mockGremgoResponse := func(expectedCodesAndOrders map[string]*int) []gremgo.Response {
		values := []json.RawMessage{}
		for code, order := range expectedCodesAndOrders {
			rawMap := mockCodeEdgeMapResponse(code, order)
			values = append(values, rawMap)
		}

		testData := graphson.RawSlice{
			Type:  "g:List",
			Value: values,
		}
		rawTestData, err := json.Marshal(testData)
		So(err, ShouldBeNil)

		return []gremgo.Response{
			{
				RequestID: "89ed2475-6eb8-452b-a955-7f7697de2ff9",
				Status:    gremgo.Status{Message: "", Code: 200},
				Result: gremgo.Result{
					Data: rawTestData,
				},
			},
		}
	}

	Convey("Given a database containing valid 'usedBy' edge with order", t, func() {
		response := mockGremgoResponse(map[string]*int{
			"mar": &testOrderMar,
			"apr": &testOrderApr,
		})

		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return response, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When GetCodesOrder() is called", func() {
			codeOrders, err := db.GetCodesOrder(context.Background(), testCodeListID, testCodes)

			Convey("Then the expected order should be returned withour error", func() {
				So(err, ShouldBeNil)
				So(codeOrders, ShouldHaveLength, 2)
				So(*codeOrders["mar"], ShouldEqual, testOrderMar)
				So(*codeOrders["apr"], ShouldEqual, testOrderApr)
			})

			Convey("Then the driver Execute function should be called once with the expected query", func() {
				expectedQry := `g.V().hasLabel('_code_list').has('_code_list', 'listID', 'mmm').inE('usedBy').where(otherV().has('value', within('mar','apr'))).as('usedBy').outV().values('value').as('code').union(select('code', 'usedBy'))`
				So(poolMock.ExecuteCalls(), ShouldHaveLength, 1)
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedQry)
			})
		})

		Convey("When GetCodesOrder() is called with no codes", func() {
			codeOrders, err := db.GetCodesOrder(context.Background(), testCodeListID, []string{})

			Convey("Then an empty map is returned with no error", func() {
				So(err, ShouldBeNil)
				So(codeOrders, ShouldHaveLength, 0)
			})

			Convey("Then the driver Execute function should not be called", func() {
				So(poolMock.ExecuteCalls(), ShouldHaveLength, 0)
			})
		})
	})

	Convey("Given a database containing valid 'usedBy' edge without order", t, func() {
		response := mockGremgoResponse(map[string]*int{
			"mar": nil,
		})

		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return response, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When GetCodesOrder() is called", func() {
			codeOrders, err := db.GetCodesOrder(context.Background(), testCodeListID, []string{"mar"})

			Convey("Then a nil order should be returned withour error", func() {
				So(err, ShouldBeNil)
				So(codeOrders, ShouldHaveLength, 1)
				So(codeOrders["mar"], ShouldBeNil)
			})

			Convey("Then the driver Execute function should be called once with the expected query", func() {
				expectedQry := `g.V().hasLabel('_code_list').has('_code_list', 'listID', 'mmm').inE('usedBy').where(otherV().has('value', within('mar'))).as('usedBy').outV().values('value').as('code').union(select('code', 'usedBy'))`
				So(poolMock.ExecuteCalls(), ShouldHaveLength, 1)
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedQry)
			})
		})
	})

	Convey("Given a database that fails to execute a query", t, func() {
		errExecute := errors.New("execute failed")
		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return nil, errExecute
			},
		}
		db := mockDB(poolMock)

		Convey("When GetCodesOrder() is called", func() {
			_, err := db.GetCodesOrder(context.Background(), testCodeListID, testCodes)

			Convey("Then the expected error should be returned", func() {
				expectedErr := "number of attempts exceeded: execute failed"
				So(err.Error(), ShouldResemble, expectedErr)
			})
		})
	})

	Convey("Given a database that returns a valid empty response", t, func() {
		response := mockGremgoResponse(map[string]*int{})

		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return response, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When GetCodesOrder() is called", func() {
			_, err := db.GetCodesOrder(context.Background(), testCodeListID, testCodes)

			Convey("Then a notFound error should be returned", func() {
				So(err, ShouldResemble, driver.ErrNotFound)
			})
		})
	})

	Convey("Given a database that returns a valid response with missing items", t, func() {
		response := mockGremgoResponse(map[string]*int{
			"mar": &testOrderMar,
		})

		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) ([]gremgo.Response, error) {
				return response, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When GetCodesOrder() is called", func() {
			codeOrders, err := db.GetCodesOrder(context.Background(), testCodeListID, testCodes)

			Convey("Then the found codeOrders should be returned long a notFound error should be returned", func() {
				So(err, ShouldResemble, driver.ErrNotFound)
				So(codeOrders, ShouldHaveLength, 1)
				So(*codeOrders["mar"], ShouldEqual, testOrderMar)
			})
		})
	})
}

// mockUsedByEdge generates an Edge struct fort testing
// if order is not nil, it will be encoded as an 'order' edge property
func mockUsedByEdge(order *int) graphson.Edge {
	edge := graphson.Edge{
		Type: "g:Edge",
		Value: graphson.EdgeValue{
			Label:      "usedBy",
			Properties: map[string]graphson.EdgeProperty{},
		},
	}

	if order != nil {
		orderValue, err := json.Marshal(order)
		So(err, ShouldBeNil)
		orderProperty := graphson.Raw{
			Type:  "g:Int32",
			Value: orderValue,
		}
		orderPropertyValue, err := json.Marshal(orderProperty)
		So(err, ShouldBeNil)

		edge.Value.Properties["order"] = graphson.EdgeProperty{
			Type: "g.Property",
			Value: graphson.EdgePropertyValue{
				Label: "order",
				Value: orderPropertyValue,
			},
		}
	}

	return edge
}

// mockCodeEdgeMap generates a code-edge map with the expected code and order property for the usedBy edge
func mockCodeEdgeMap(expectedCode string, expectedOrder *int) map[string]json.RawMessage {
	rawCode, err := json.Marshal(expectedCode)
	So(err, ShouldBeNil)

	edge := mockUsedByEdge(expectedOrder)
	rawEdge, err := json.Marshal(edge)
	So(err, ShouldBeNil)

	return map[string]json.RawMessage{
		"code":   rawCode,
		"usedBy": rawEdge,
	}
}

// mockCodeEdgeMapResponse generates a code-edge map with the expected code and order property for the usedBy edge,
// as returned by Neptune before being processed by graphson into a map (slice representation of the map)
func mockCodeEdgeMapResponse(expectedCode string, expectedOrder *int) json.RawMessage {
	m := mockCodeEdgeMap(expectedCode, expectedOrder)
	rawMap, err := SerializeMap(m)
	So(err, ShouldBeNil)
	return rawMap
}

// SerializeMap converts the provided map to the json.RawMessage as used by Neptune,
// where type is g:Map and the values are an array of jsonRawMessages with even items being serialized keys and odd items being serialized values
func SerializeMap(inputMap map[string]json.RawMessage) (json.RawMessage, error) {

	mapSliceRepresentation := graphson.RawSlice{
		Type:  "g:Map",
		Value: []json.RawMessage{},
	}

	for key, val := range inputMap {
		rawKey, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}

		rawVal, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}

		mapSliceRepresentation.Value = append(mapSliceRepresentation.Value, rawKey, rawVal)
	}

	return json.Marshal(mapSliceRepresentation)
}
