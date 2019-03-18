package driver_test

import (
	"io"
	"testing"

	"github.com/ONSdigital/dp-graph/neo4j/driver"
	"github.com/ONSdigital/dp-graph/neo4j/internal"
	"github.com/ONSdigital/dp-graph/observation"
	. "github.com/smartystreets/goconvey/convey"
)

var closeNoErr = func() error {
	return nil
}

func TestBoltRowReader_Read(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader", t, func() {

		expectedCSVRow := "the,csv,row"

		mockBoltRows := &internal.BoltRowsMock{
			CloseFunc: closeNoErr,
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{expectedCSVRow, "1,2,3"}, nil, nil
			},
		}

		mockBoltConn := &internal.BoltConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := driver.NewBoltRowReader(mockBoltRows, mockBoltConn)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The expected csv row is returned", func() {
				So(err, ShouldBeNil)
				So(row, ShouldEqual, expectedCSVRow+"\n")
			})
		})
	})
}

func TestBoltRowReader_ReadError(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader that returns io.EOF", t, func() {

		mockBoltRows := &internal.BoltRowsMock{
			CloseFunc: closeNoErr,
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return nil, nil, io.EOF
			},
		}

		mockBoltConn := &internal.BoltConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := driver.NewBoltRowReader(mockBoltRows, mockBoltConn)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The error from the Bolt reader is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, observation.ErrNoInstanceFound)
				So(row, ShouldEqual, "")
			})
		})
	})
}

func TestBoltRowReader_Read_NoDataError(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader that returns a row with no data.", t, func() {

		mockBoltRows := &internal.BoltRowsMock{
			CloseFunc: closeNoErr,
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{}, nil, nil
			},
		}

		mockBoltConn := &internal.BoltConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := driver.NewBoltRowReader(mockBoltRows, mockBoltConn)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, observation.ErrNoDataReturned)
				So(row, ShouldEqual, "")
			})
		})
	})
}

func TestBoltRowReader_Read_TypeError(t *testing.T) {

	Convey("Given a row reader with a mock Bolt reader that returns a row with no data.", t, func() {

		mockBoltRows := &internal.BoltRowsMock{
			CloseFunc: closeNoErr,
			NextNeoFunc: func() ([]interface{}, map[string]interface{}, error) {
				return []interface{}{666, 666}, nil, nil
			},
		}

		mockBoltConn := &internal.BoltConnMock{
			CloseFunc: func() error {
				return nil
			},
		}

		rowReader := driver.NewBoltRowReader(mockBoltRows, mockBoltConn)

		Convey("When read is called", func() {

			row, err := rowReader.Read()

			Convey("The expected error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, observation.ErrUnrecognisedType)
				So(row, ShouldEqual, "")
			})
		})
	})
}
