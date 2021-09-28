package graph

import (
	"context"
	"os"
	"testing"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_New(t *testing.T) {
	os.Setenv("GRAPH_DRIVER_TYPE", "mock")

	Convey("Given all subsets are requested", t, func() {
		Convey("When New is called", func() {
			db, err := New(context.Background(), Subsets{true, true, true, true, true})

			Convey("Then the returned error should be nil and the returned db should satisfy all interfaces", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.CodeList = (*DB)(db)
				var _ driver.Hierarchy = (*DB)(db)
				var _ driver.Instance = (*DB)(db)
				var _ driver.Observation = (*DB)(db)
				var _ driver.Dimension = (*DB)(db)
			})
		})
	})

	Convey("Given only 1 subset is requested", t, func() {
		Convey("When New is called", func() {
			db, err := New(context.Background(), Subsets{true, false, false, false, false})

			Convey("Then the returned error should be nil and the returned db should satisfy only that interface", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.CodeList = (*DB)(db)

				So(func() {
					db.CreateInstanceHierarchyConstraints(context.Background(), 1, "instance_id", "dimension_name")
				}, ShouldPanic)

				So(func() {
					db.AddVersionDetailsToInstance(context.Background(), "instance_id", "dataset_id", "edition", 1)
				}, ShouldPanic)
			})
		})
	})
}

func Test_NewCodeListStore(t *testing.T) {
	os.Setenv("GRAPH_DRIVER_TYPE", "mock")

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

				So(func() {
					db.AddVersionDetailsToInstance(context.Background(), "instance_id", "dataset_id", "edition", 1)
				}, ShouldPanic)

				So(func() {
					db.StreamCSVRows(context.Background(), "", "", nil, nil)
				}, ShouldPanic)

				So(func() {
					db.InsertDimension(context.Background(), nil, nil, "", nil)
				}, ShouldPanic)

				So(func() {
					db.GetCodeList(context.Background(), "list_id")
				}, ShouldNotPanic)
			})
		})
	})
}

func Test_NewHierarchyStore(t *testing.T) {
	os.Setenv("GRAPH_DRIVER_TYPE", "mock")

	Convey("Given only hierarchy subset is requested", t, func() {
		Convey("When NewHierarchyStore is called", func() {
			db, err := NewHierarchyStore(context.Background())

			Convey("Then the returned error should be nil and the returned db should satisfy only that interface", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.Hierarchy = (*DB)(db)

				So(func() {
					db.GetCodeList(context.Background(), "list_id")
				}, ShouldPanic)

				So(func() {
					db.AddVersionDetailsToInstance(context.Background(), "instance_id", "dataset_id", "edition", 1)
				}, ShouldPanic)

				So(func() {
					db.StreamCSVRows(context.Background(), "", "", nil, nil)
				}, ShouldPanic)

				So(func() {
					db.InsertDimension(context.Background(), nil, nil, "", nil)
				}, ShouldPanic)

				So(func() {
					db.CreateInstanceHierarchyConstraints(context.Background(), 1, "instance_id", "dimension_name")
				}, ShouldNotPanic)
			})
		})
	})
}

func Test_NewInstanceStore(t *testing.T) {
	os.Setenv("GRAPH_DRIVER_TYPE", "mock")

	Convey("Given only instance subset is requested", t, func() {
		Convey("When NewInstanceStore is called", func() {
			db, err := NewInstanceStore(context.Background())

			Convey("Then the returned error should be nil and the returned db should satisfy only that interface", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.Instance = (*DB)(db)

				So(func() {
					db.GetCodeList(context.Background(), "list_id")
				}, ShouldPanic)

				So(func() {
					db.CreateInstanceHierarchyConstraints(context.Background(), 1, "instance_id", "dimension_name")
				}, ShouldPanic)

				So(func() {
					db.StreamCSVRows(context.Background(), "", "", nil, nil)
				}, ShouldPanic)

				So(func() {
					db.InsertDimension(context.Background(), nil, nil, "", nil)
				}, ShouldPanic)

				So(func() {
					db.AddVersionDetailsToInstance(context.Background(), "instance_id", "dataset_id", "edition", 1)
				}, ShouldNotPanic)
			})
		})
	})
}

func Test_NewObservationStore(t *testing.T) {
	os.Setenv("GRAPH_DRIVER_TYPE", "mock")

	Convey("Given only observation subset is requested", t, func() {
		Convey("When NewObservationStore is called", func() {
			db, err := NewObservationStore(context.Background())

			Convey("Then the returned error should be nil and the returned db should satisfy only that interface", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.Observation = (*DB)(db)

				So(func() {
					db.GetCodeList(context.Background(), "list_id")
				}, ShouldPanic)

				So(func() {
					db.CreateInstanceHierarchyConstraints(context.Background(), 1, "instance_id", "dimension_name")
				}, ShouldPanic)

				So(func() {
					db.AddVersionDetailsToInstance(context.Background(), "instance_id", "dataset_id", "edition", 1)
				}, ShouldPanic)

				So(func() {
					db.InsertDimension(context.Background(), nil, nil, "", nil)
				}, ShouldPanic)

				So(func() {
					db.StreamCSVRows(context.Background(), "", "", nil, nil)
				}, ShouldNotPanic)
			})
		})
	})
}

func Test_NewDimensionStore(t *testing.T) {
	os.Setenv("GRAPH_DRIVER_TYPE", "mock")

	Convey("Given only dimension subset is requested", t, func() {
		Convey("When NewDimensionStore is called", func() {
			db, err := NewDimensionStore(context.Background())

			Convey("Then the returned error should be nil and the returned db should satisfy only that interface", func() {
				So(err, ShouldBeNil)

				var _ driver.Driver = (*DB)(db)
				var _ driver.Dimension = (*DB)(db)

				So(func() {
					db.GetCodeList(context.Background(), "list_id")
				}, ShouldPanic)

				So(func() {
					db.CreateInstanceHierarchyConstraints(context.Background(), 1, "instance_id", "dimension_name")
				}, ShouldPanic)

				So(func() {
					db.AddVersionDetailsToInstance(context.Background(), "instance_id", "dataset_id", "edition", 1)
				}, ShouldPanic)

				So(func() {
					db.StreamCSVRows(context.Background(), "", "", nil, nil)
				}, ShouldPanic)

				So(func() {
					db.InsertDimension(context.Background(), nil, nil, "", nil)
				}, ShouldNotPanic)
			})
		})
	})
}
