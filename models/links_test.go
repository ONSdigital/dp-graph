package models

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

var id = "id"

func TestCreateLink(t *testing.T) {
	Convey("Given a valid domain config and fully qualified url", t, func() {
		domain := "http://api.example.com/v1"
		hrefURL := "http://localhost:22400/code-list/local-authority/codes/E01000064"

		Convey("When the CreateLink function is called", func() {
			link, err := CreateLink(id, hrefURL, domain)

			Convey("Then the returned values should be as expected", func() {
				So(err, ShouldBeNil)
				So(link.Href, ShouldEqual, "http://api.example.com/v1/code-list/local-authority/codes/E01000064")
				So(link.ID, ShouldEqual, id)
			})
		})
	})

	Convey("Given default config and fully qualified url", t, func() {
		domain := "http://localhost:22400"
		hrefURL := "http://somedomain/code-list/local-authority/codes/E01000064"

		Convey("When the CreateLink function is called", func() {
			link, err := CreateLink(id, hrefURL, domain)

			Convey("Then the returned values should be as expected", func() {
				So(err, ShouldBeNil)
				So(link.Href, ShouldEqual, "http://localhost:22400/code-list/local-authority/codes/E01000064")
				So(link.ID, ShouldEqual, id)
			})
		})
	})

	Convey("Given a malformed url", t, func() {
		domain := "http://localhost:22400"
		hrefURL := "/code-list/local%!1234"

		Convey("When the CreateLink function is called", func() {
			_, err := CreateLink(id, hrefURL, domain)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldResemble, "parse /code-list/local%!1234: invalid URL escape \"%!1\"")
			})
		})
	})

	Convey("Given a malformed domain", t, func() {
		domain := "http://localhost:%!1234"
		hrefURL := "http://somedomain/code-list/local-authority/codes/E01000064"

		Convey("When the CreateLink function is called", func() {
			_, err := CreateLink(id, hrefURL, domain)

			Convey("Then the expected error is returned", func() {
				So(err.Error(), ShouldResemble, "parse http://localhost:%!1234: invalid port \":%!1234\" after host")
			})
		})
	})
}
