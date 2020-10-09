package observation_test

import (
	"github.com/ONSdigital/dp-graph/v2/observation"
	"github.com/ONSdigital/dp-graph/v2/observation/observationtest"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"testing"
)

func TestCompositeRowReader_Read(t *testing.T) {

	Convey("Given a composite reader with a mock header and observation reader", t, func() {

		headerContent := "header1,header2,header3"
		headerRead := false
		mockHeaderReader := &observationtest.StreamRowReaderMock{
			ReadFunc: func() (string, error) {
				if headerRead {
					return "", io.EOF
				}
				headerRead = true
				return headerContent, nil
			},
		}

		rowContent := "csv,row,content"
		rowRead := false
		mockRowReader := &observationtest.StreamRowReaderMock{
			ReadFunc: func() (string, error) {
				if rowRead {
					return "", io.EOF
				}
				rowRead = true
				return rowContent, nil
			},
		}

		reader := observation.NewCompositeRowReader(mockHeaderReader, mockRowReader)

		Convey("When read is called", func() {

			row, err := reader.Read()
			So(err, ShouldBeNil)
			So(row, ShouldEqual, headerContent)

			row, err = reader.Read()
			So(err, ShouldBeNil)
			So(row, ShouldEqual, rowContent)

			row, err = reader.Read()
			So(err, ShouldEqual, io.EOF)
			So(row, ShouldEqual, "")
		})
	})
}
