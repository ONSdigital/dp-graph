package neptune

import (
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/graphson"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func Test_buildHierarchyNode(t *testing.T) {

	Convey("Given an example hierarchy node vertex returned from neptune", t, func() {

		poolMock := &internal.NeptunePoolMock{}
		db := mockDB(poolMock)

		expectedLabel := "code-label"
		expectedCode := "code"
		expectedHasData := true
		expectedNumberOfChildren := 0
		vertex := internal.MakeHierarchyVertex("vertex-label", expectedCode, expectedLabel, expectedNumberOfChildren, expectedHasData)

		wantBreadcrumbs := false
		dimension := "dimension"
		instanceID := "instance-id"

		Convey("When buildHierarchyNode is called", func() {

			hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)

			Convey("Then the expected values are mapped onto the returned hierarchy response", func() {
				So(err, ShouldBeNil)
				So(hierarchyNode.ID, ShouldEqual, expectedCode)
				So(hierarchyNode.Label, ShouldEqual, expectedLabel)
				So(hierarchyNode.NoOfChildren, ShouldEqual, expectedNumberOfChildren)
				So(hierarchyNode.HasData, ShouldEqual, expectedHasData)
			})
		})
	})

	Convey("Given a hierarchy node with child nodes", t, func() {
		expectedLabel := "child-label"
		expectedCode := "child-code"
		expectedHasData := true
		expectedNumberOfChildren := 1

		// mock the database to return a single child node
		poolMock := &internal.NeptunePoolMock{GetFunc: func(query string, bindings map[string]string, rebindings map[string]string) (vertices []graphson.Vertex, err error) {
			return []graphson.Vertex{
				internal.MakeHierarchyVertex("vertex-label", expectedCode, expectedLabel, expectedNumberOfChildren, expectedHasData),
			}, nil
		}}
		db := mockDB(poolMock)

		vertex := internal.MakeHierarchyVertex("vertex-label", "code", "label", 1, true)

		wantBreadcrumbs := true
		dimension := "dimension"
		instanceID := "instance-id"

		Convey("When buildHierarchyNode is called", func() {

			hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)

			Convey("Then the expected values are mapped onto the returned child nodes", func() {
				So(err, ShouldBeNil)
				So(len(hierarchyNode.Children), ShouldEqual, 1)
				So(hierarchyNode.Children[0].NoOfChildren, ShouldEqual, expectedNumberOfChildren)
				So(hierarchyNode.Children[0].ID, ShouldEqual, expectedCode)
				So(hierarchyNode.Children[0].Label, ShouldEqual, expectedLabel)
				So(hierarchyNode.Children[0].HasData, ShouldEqual, expectedHasData)
			})
		})
	})
}
