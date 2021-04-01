package neptune

import (
	"testing"

	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/graphson"
	. "github.com/smartystreets/goconvey/convey"
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
		// expected paramters for neptune pool mock
		var (
			expectedLabel            = "child-label"
			expectedCode             = "child-code"
			expectedHasData          = true
			expectedNumberOfChildren = 1
		)

		// expected gremlin queries
		var (
			expectedCountQuery             = "g.V().hasLabel('_hierarchy_node_instance-id_dimension').has('code','code').in('hasParent').has('order').count()"
			expectedGetWithOrderQuery      = "g.V().hasLabel('_hierarchy_node_instance-id_dimension').has('code','code').in('hasParent').order().by('order',incr)"
			expectedGetAlphabeticallyQuery = "g.V().hasLabel('_hierarchy_node_instance-id_dimension').has('code','code').in('hasParent').order().by('label')"
			expectedGetAncestryQuery       = "g.V().hasLabel('_hierarchy_node_instance-id_dimension').has('code', 'code').repeat(out('hasParent')).emit()"
		)

		// mock the database to return a single child node
		poolMock := &internal.NeptunePoolMock{
			GetFunc: func(query string, bindings map[string]string, rebindings map[string]string) (vertices []graphson.Vertex, err error) {
				return []graphson.Vertex{
					internal.MakeHierarchyVertex("vertex-label", expectedCode, expectedLabel, expectedNumberOfChildren, expectedHasData),
				}, nil
			},
		}

		vertex := internal.MakeHierarchyVertex("vertex-label", "code", "label", 1, true)

		wantBreadcrumbs := true
		dimension := "dimension"
		instanceID := "instance-id"

		Convey("Where the hierarchy node contains an 'order' property", func() {
			poolMock.GetCountFunc = func(q string, bindings map[string]string, rebindings map[string]string) (int64, error) {
				return 1, nil
			}
			db := mockDB(poolMock)

			Convey("When buildHierarchyNode is called", func() {

				hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)

				Convey("Then the returned error is nil", func() {
					So(err, ShouldBeNil)
				})

				Convey("Then CountChildrenWithOrder query is executed", func() {
					So(poolMock.GetCountCalls(), ShouldHaveLength, 1)
					So(poolMock.GetCountCalls()[0].Q, ShouldResemble, expectedCountQuery)
				})

				Convey("Then the children are queried to be sorted according to their order property, before getting ancestry", func() {
					So(poolMock.GetCalls(), ShouldHaveLength, 2)
					So(poolMock.GetCalls()[0].Query, ShouldResemble, expectedGetWithOrderQuery)
					So(poolMock.GetCalls()[1].Query, ShouldResemble, expectedGetAncestryQuery)
				})

				Convey("Then the expected values are mapped onto the returned child nodes", func() {
					So(len(hierarchyNode.Children), ShouldEqual, 1)
					So(hierarchyNode.Children[0].NoOfChildren, ShouldEqual, expectedNumberOfChildren)
					So(hierarchyNode.Children[0].ID, ShouldEqual, expectedCode)
					So(hierarchyNode.Children[0].Label, ShouldEqual, expectedLabel)
					So(hierarchyNode.Children[0].HasData, ShouldEqual, expectedHasData)
				})
			})
		})

		Convey("Where the hierarchy node does not contain an 'order' property", func() {
			poolMock.GetCountFunc = func(q string, bindings map[string]string, rebindings map[string]string) (int64, error) {
				return 0, nil
			}
			db := mockDB(poolMock)

			Convey("When buildHierarchyNode is called", func() {

				hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)

				Convey("Then the returned error is nil", func() {
					So(err, ShouldBeNil)
				})

				Convey("Then CountChildrenWithOrder query is executed", func() {
					So(poolMock.GetCountCalls(), ShouldHaveLength, 1)
					So(poolMock.GetCountCalls()[0].Q, ShouldResemble, expectedCountQuery)
				})

				Convey("Then the children are queried to be sorted alphabetically according to their label, before getting ancestry", func() {
					So(poolMock.GetCalls(), ShouldHaveLength, 2)
					So(poolMock.GetCalls()[0].Query, ShouldResemble, expectedGetAlphabeticallyQuery)
					So(poolMock.GetCalls()[1].Query, ShouldResemble, expectedGetAncestryQuery)
				})

				Convey("Then the expected values are mapped onto the returned child nodes", func() {
					So(len(hierarchyNode.Children), ShouldEqual, 1)
					So(hierarchyNode.Children[0].NoOfChildren, ShouldEqual, expectedNumberOfChildren)
					So(hierarchyNode.Children[0].ID, ShouldEqual, expectedCode)
					So(hierarchyNode.Children[0].Label, ShouldEqual, expectedLabel)
					So(hierarchyNode.Children[0].HasData, ShouldEqual, expectedHasData)
				})
			})
		})
	})
}
