package graph

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-graph/graph/driver"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_New(t *testing.T) {

	Convey("Given all subsets are requested", t, func() {
		Convey("When New is called", func() {
			db, err := New(context.Background(), Subsets{true, true, true})

			Convey("Then the returned error should be nil and the returned db should satisfy all interfaces", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.CodeList = (*DB)(db)
				var _ driver.Hierarchy = (*DB)(db)
				var _ driver.Instance = (*DB)(db)
			})
		})
	})

	Convey("Given only 1 subset is requested", t, func() {
		Convey("When New is called", func() {
			db, err := New(context.Background(), Subsets{true, false, false})

			Convey("Then the returned error should be nil and the returned db should satisfy only that interface", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.CodeList = (*DB)(db)

				So(func() {
					db.CreateInstanceHierarchyConstraints(context.Background(), 1, "instance_id", "dimension_name")
				}, ShouldPanic)

				So(func() { db.CountInsertedObservations(context.Background(), "instance_id") }, ShouldPanic)
			})
		})
	})
}

func Test_NewCodeListStore(t *testing.T) {
	Convey("Given only code list subset is requested", t, func() {
		Convey("When NewCodeListStore is called", func() {
			db, err := NewCodeListStore(context.Background())

			Convey("Then the returned error should be nil and the returned db should satisfy only that interface", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.CodeList = (*DB)(db)

				So(func() {
					db.CreateInstanceHierarchyConstraints(context.Background(), 1, "instance_id", "dimension_name")
				}, ShouldPanic)

				So(func() { db.CountInsertedObservations(context.Background(), "instance_id") }, ShouldPanic)
			})
		})
	})
}

func Test_NewHierarchyStore(t *testing.T) {
	Convey("Given only code list subset is requested", t, func() {
		Convey("When NewHierarchyStore is called", func() {
			db, err := NewHierarchyStore(context.Background())

			Convey("Then the returned error should be nil and the returned db should satisfy only that interface", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.Hierarchy = (*DB)(db)

				So(func() {
					db.GetCodeList(context.Background(), "list_id")
				}, ShouldPanic)

				So(func() { db.CountInsertedObservations(context.Background(), "instance_id") }, ShouldPanic)
			})
		})
	})
}
