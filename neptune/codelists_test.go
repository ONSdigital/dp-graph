package neptune

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/ONSdigital/dp-graph/graph/	driver"
	"github.com/ONSdigital/dp-graph/neptune/internal"
)

func TestGetCodeListInNonErrorScenario(t *testing.T) {
	Convey("When the database says the CodeList ID exists", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnOne}
		db := mockDB(poolMock)
		Convey("And GetCodeList() is called", func() {
			codeListID := "arbitrary"
			codeList, err := db.GetCodeList(context.Background(), codeListID)
			Convey("The driver GetCount function should be called once", func() {
				calls := poolMock.GetCountCalls()
				So(len(calls), ShouldEqual, 1)
				Convey("With a well formed query string", func() {
					expectedQry := "g.V().hasLabel('_code_list_arbitrary').count()"
					So(calls[0].Q, ShouldEqual, expectedQry)
				})
			})
			Convey("And no error should be returned", func() {
				So(err, ShouldBeNil)
			})
			Convey("And a non nil structure returned", func() {
				So(codeList, ShouldNotBeNil)
			})
		})
	},
	)
}

func TestGetCodeListErrorHandlingForNonTransientError(t *testing.T) {
	Convey("When the database raises a non-transient error", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc: internal.ReturnMalformedRequestErr,
		}
		db := mockDB(poolMock)
		Convey("And GetCodeList() is called", func() {
			codeListID := "arbitrary"
			_, err := db.GetCodeList(context.Background(), codeListID)
			expectedErr := `Gremlin query failed: "g.V().hasLabel(` +
				`'_code_list_arbitrary').count()":  MALFORMED REQUEST `
			Convey("The returned error should wrap the underlying one", func() {
				So(err.Error(), ShouldEqual, expectedErr)
			})
		})
	},
	)
}

func TestGetCodeListErrorHandlingForNotFound(t *testing.T) {
	Convey("When the database says the CodeList ID does not exist", t, func() {
		poolMock := &internal.NeptunePoolMock{GetCountFunc: internal.ReturnZero}
		db := mockDB(poolMock)
		Convey("And GetCodeList() is called", func() {
			codeListID := "arbitrary"
			_, err := db.GetCodeList(context.Background(), codeListID)
			Convey("The returned error should be ErrNotFound", func() {
				So(err, ShouldEqual, driver.ErrNotFound)
			})
		})
	},
	)
}
