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
				`inE('usedBy').as('usedBy').outV().order().by('value',incr).as('code').select('usedBy', 'code').by('label').by('value').` +
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
				`inE('usedBy').order().by('order',incr).as('usedBy').outV().as('code').select('usedBy', 'code').by('label').by('value').` +
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
						`outV().order().by('value',incr).as('code').` +
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
						`inE('usedBy').order().by('order',incr).as('usedBy').` +
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

// func TestGetCodesOrder(t *testing.T) {

// 	testCodeListID := "mmm"
// 	testCode := "mar"

// 	Convey("Given a database containing valid 'usedBy' edge with order", t, func() {
// 		expectedOrder := PropertyValueInt{
// 			Value: 2,
// 		}

// 		orderValue, err := json.Marshal(&expectedOrder)
// 		So(err, ShouldBeNil)

// 		poolMock := &internal.NeptunePoolMock{
// 			GetEFunc: func(q string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
// 				return []graphson.Edge{
// 					{
// 						Type: "g:Edge",
// 						Value: graphson.EdgeValue{
// 							Label: "usedBy",
// 							Properties: map[string]graphson.EdgeProperty{
// 								"order": {
// 									Type: "g.Property",
// 									Value: graphson.EdgePropertyValue{
// 										Label: "order",
// 										Value: orderValue,
// 									},
// 								},
// 							},
// 						},
// 					},
// 				}, nil
// 			},
// 		}
// 		db := mockDB(poolMock)

// 		Convey("When GetCodeOrder() is called", func() {
// 			order, err := db.GetCodeOrder(context.Background(), testCodeListID, testCode)

// 			Convey("Then the expected order should be returned withour error", func() {
// 				So(err, ShouldBeNil)
// 				So(*order, ShouldEqual, expectedOrder.Value)
// 			})

// 			Convey("Then the driver GetE function should be called once with the expected query", func() {
// 				expectedQry := `g.V().hasId('_code_mmm_mar')` +
// 					`.outE('usedBy')` +
// 					`.where(otherV().hasLabel('_code_list').has('_code_list', 'listID', 'mmm'))`
// 				So(poolMock.GetECalls(), ShouldHaveLength, 1)
// 				So(poolMock.GetECalls()[0].Q, ShouldEqual, expectedQry)
// 			})
// 		})
// 	})

// 	Convey("Given a database containing valid 'usedBy' edge without order", t, func() {
// 		poolMock := &internal.NeptunePoolMock{
// 			GetEFunc: func(q string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
// 				return []graphson.Edge{
// 					{
// 						Type: "g:Edge",
// 						Value: graphson.EdgeValue{
// 							Label:      "usedBy",
// 							Properties: map[string]graphson.EdgeProperty{},
// 						},
// 					},
// 				}, nil
// 			},
// 		}
// 		db := mockDB(poolMock)

// 		Convey("When GetCodeOrder() is called", func() {
// 			order, err := db.GetCodeOrder(context.Background(), testCodeListID, testCode)

// 			Convey("Then a nil order should be returned withour error", func() {
// 				So(err, ShouldBeNil)
// 				So(order, ShouldBeNil)
// 			})

// 			Convey("Then the driver GetE function should be called once with the expected query", func() {
// 				expectedQry := `g.V().hasId('_code_mmm_mar')` +
// 					`.outE('usedBy')` +
// 					`.where(otherV().hasLabel('_code_list').has('_code_list', 'listID', 'mmm'))`
// 				So(poolMock.GetECalls(), ShouldHaveLength, 1)
// 				So(poolMock.GetECalls()[0].Q, ShouldEqual, expectedQry)
// 			})
// 		})
// 	})

// 	Convey("Given a database that fails to get edges", t, func() {
// 		errGetE := errors.New("getE failed")
// 		poolMock := &internal.NeptunePoolMock{
// 			GetEFunc: func(q string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
// 				return []graphson.Edge{}, errGetE
// 			},
// 		}
// 		db := mockDB(poolMock)

// 		Convey("When GetCodeOrder() is called", func() {
// 			_, err := db.GetCodeOrder(context.Background(), testCodeListID, testCode)

// 			Convey("Then the wrapped error should be returned", func() {
// 				expectedErr := "Gremlin query failed: \"g.V().hasId('_code_mmm_mar').outE('usedBy').where(otherV().hasLabel('_code_list').has('_code_list', 'listID', 'mmm'))\": number of attempts exceeded: getE failed"
// 				So(err.Error(), ShouldResemble, expectedErr)
// 			})
// 		})
// 	})

// 	Convey("Given a database that does not return any 'usedBy' edge", t, func() {
// 		poolMock := &internal.NeptunePoolMock{
// 			GetEFunc: func(q string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
// 				return []graphson.Edge{}, nil
// 			},
// 		}
// 		db := mockDB(poolMock)

// 		Convey("When GetCodeOrder() is called", func() {
// 			_, err := db.GetCodeOrder(context.Background(), testCodeListID, testCode)

// 			Convey("Then a notFound error should be returned", func() {
// 				So(err, ShouldResemble, driver.ErrNotFound)
// 			})
// 		})
// 	})

// 	Convey("Given a database containing multiple 'usedBy' edges for the same code-codelist pair", t, func() {
// 		poolMock := &internal.NeptunePoolMock{
// 			GetEFunc: func(q string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
// 				return []graphson.Edge{
// 					{
// 						Type: "g:Edge",
// 						Value: graphson.EdgeValue{
// 							Label:      "usedBy",
// 							Properties: map[string]graphson.EdgeProperty{},
// 						},
// 					},
// 					{
// 						Type: "g:Edge",
// 						Value: graphson.EdgeValue{
// 							Label:      "usedBy",
// 							Properties: map[string]graphson.EdgeProperty{},
// 						},
// 					},
// 				}, nil
// 			},
// 		}
// 		db := mockDB(poolMock)

// 		Convey("When GetCodeOrder() is called", func() {
// 			_, err := db.GetCodeOrder(context.Background(), testCodeListID, testCode)

// 			Convey("Then a multipleFound error should be returned", func() {
// 				So(err, ShouldResemble, driver.ErrMultipleFound)
// 			})
// 		})
// 	})

// 	Convey("Given a database containing a 'usedBy' edge with invalid order value", t, func() {
// 		poolMock := &internal.NeptunePoolMock{
// 			GetEFunc: func(q string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
// 				return []graphson.Edge{
// 					{
// 						Type: "g:Edge",
// 						Value: graphson.EdgeValue{
// 							Label: "usedBy",
// 							Properties: map[string]graphson.EdgeProperty{
// 								"order": {
// 									Type: "g.Property",
// 									Value: graphson.EdgePropertyValue{
// 										Label: "order",
// 										Value: []byte{1, 2, 3, 4, 5},
// 									},
// 								},
// 							},
// 						},
// 					},
// 				}, nil
// 			},
// 		}
// 		db := mockDB(poolMock)

// 		Convey("When GetCodeOrder() is called", func() {
// 			_, err := db.GetCodeOrder(context.Background(), testCodeListID, testCode)

// 			Convey("Then the expected unmarshal order should be returned", func() {
// 				So(err.Error(), ShouldResemble, "invalid character '\\x01' looking for beginning of value")
// 			})
// 		})
// 	})
// }
