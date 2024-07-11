package neptune

import (
	"testing"

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/graphson"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_buildHierarchyNode(t *testing.T) {

	Convey("Given an example hierarchy node vertex returned from neptune", t, func() {

		poolMock := &internal.NeptunePoolMock{}
		db := mockDB(poolMock)

		var (
			expectedLabel                    = "code-label"
			expectedCode                     = "code"
			expectedHasData                  = true
			numberOfChildren         float64 = 0
			expectedNumberOfChildren int64   = 0
		)
		vertex, err := internal.MakeHierarchyVertex("vertex-label", expectedCode, expectedLabel, numberOfChildren, expectedHasData)
		if err != nil {
			t.Fail()
		}

		wantBreadcrumbs := false
		dimension := "dimension"
		instanceID := "instance-id"

		Convey("Where the hierarchy node does not have an order property", func() {
			Convey("When buildHierarchyNode is called", func() {
				hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)
				Convey("Then the expected values are mapped onto the returned hierarchy response", func() {
					So(err, ShouldBeNil)
					So(*hierarchyNode, ShouldResemble, models.HierarchyResponse{
						ID:           expectedCode,
						Label:        expectedLabel,
						NoOfChildren: expectedNumberOfChildren,
						HasData:      expectedHasData,
					})
				})
			})
		})

		Convey("Where the hierarchy node has an order property", func() {
			var (
				order         float64 = 123
				expectedOrder int64   = 123
			)

			if err := internal.SetOrder(&vertex, order); err != nil {
				t.Fail()
			}

			Convey("When buildHierarchyNode is called", func() {
				hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)
				Convey("Then the expected values are mapped onto the returned hierarchy response", func() {
					So(err, ShouldBeNil)
					So(*hierarchyNode, ShouldResemble, models.HierarchyResponse{
						ID:           expectedCode,
						Label:        expectedLabel,
						NoOfChildren: expectedNumberOfChildren,
						HasData:      expectedHasData,
						Order:        &expectedOrder,
					})
				})
			})
		})
	})

	Convey("Given a hierarchy node with child nodes", t, func() {
		// expected paramters for neptune pool mock
		var (
			expectedLabel                    = "child-label"
			expectedCode                     = "child-code"
			expectedHasData                  = true
			expectedNumberOfChildren float64 = 1
		)

		// expected gremlin queries
		var (
			expectedCountQuery             = "g.V().hasLabel('_hierarchy_node_instance-id_dimension').has('code','code').in('hasParent').has('order').count()"
			expectedGetWithOrderQuery      = "g.V().hasLabel('_hierarchy_node_instance-id_dimension').has('code','code').in('hasParent').order().by('order',asc)"
			expectedGetAlphabeticallyQuery = "g.V().hasLabel('_hierarchy_node_instance-id_dimension').has('code','code').in('hasParent').order().by('label')"
			expectedGetAncestryQuery       = "g.V().hasLabel('_hierarchy_node_instance-id_dimension').has('code', 'code').repeat(out('hasParent')).emit()"
		)

		// mock the database to return a single child node
		poolMock := &internal.NeptunePoolMock{
			GetFunc: func(query string, bindings map[string]string, rebindings map[string]string) (vertices []graphson.Vertex, err error) {
				vertex, err := internal.MakeHierarchyVertex("vertex-label", expectedCode, expectedLabel, expectedNumberOfChildren, expectedHasData)
				if err != nil {
					t.Fail()
				}
				return []graphson.Vertex{vertex}, nil
			},
		}

		vertex, err := internal.MakeHierarchyVertex("vertex-label", "code", "label", 1, true)
		if err != nil {
			t.Fail()
		}

		wantBreadcrumbs := true
		dimension := "dimension"
		instanceID := "instance-id"

		Convey("Where the hierarchy node contains an 'order' property", func() {
			poolMock.GetCountFunc = func(q string, bindings map[string]string, rebindings map[string]string) (int64, error) {
				return 1, nil // '1' selects GetChildrenWithOrder()
			}
			db := mockDB(poolMock)

			Convey("When buildHierarchyNode is called", func() {

				hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)

				Convey("Then the returned error is nil", func() {
					So(err, ShouldBeNil)
				})

				Convey("Then CountChildrenWithOrder query is executed", func() {
					So(poolMock.GetCountCalls(), ShouldHaveLength, 1)
					So(poolMock.GetCountCalls()[0].Q, ShouldEqual, expectedCountQuery)
				})

				Convey("Then the children are queried to be sorted according to their order property, before getting ancestry", func() {
					So(poolMock.GetCalls(), ShouldHaveLength, 2)
					So(poolMock.GetCalls()[0].Query, ShouldEqual, expectedGetWithOrderQuery)
					So(poolMock.GetCalls()[1].Query, ShouldEqual, expectedGetAncestryQuery)
				})

				Convey("Then the expected values are mapped onto the returned child nodes", func() {
					So(hierarchyNode.Children, ShouldHaveLength, 1)
					So(*hierarchyNode.Children[0], ShouldResemble, models.HierarchyElement{
						NoOfChildren: int64(expectedNumberOfChildren),
						ID:           expectedCode,
						Label:        expectedLabel,
						HasData:      expectedHasData,
					})
				})
			})
		})

		Convey("Where the hierarchy node does not contain an 'order' property", func() {
			poolMock.GetCountFunc = func(q string, bindings map[string]string, rebindings map[string]string) (int64, error) {
				return 0, nil // '0' selects GetChildrenAlphabetically()
			}
			db := mockDB(poolMock)

			Convey("When buildHierarchyNode is called", func() {

				hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)

				Convey("Then the returned error is nil", func() {
					So(err, ShouldBeNil)
				})

				Convey("Then CountChildrenWithOrder query is executed", func() {
					So(poolMock.GetCountCalls(), ShouldHaveLength, 1)
					So(poolMock.GetCountCalls()[0].Q, ShouldEqual, expectedCountQuery)
				})

				Convey("Then the children are queried to be sorted alphabetically according to their label, before getting ancestry", func() {
					So(poolMock.GetCalls(), ShouldHaveLength, 2)
					So(poolMock.GetCalls()[0].Query, ShouldEqual, expectedGetAlphabeticallyQuery)
					So(poolMock.GetCalls()[1].Query, ShouldEqual, expectedGetAncestryQuery)
				})

				Convey("Then the expected values are mapped onto the returned child nodes", func() {
					So(hierarchyNode.Children, ShouldHaveLength, 1)
					So(*hierarchyNode.Children[0], ShouldResemble, models.HierarchyElement{
						NoOfChildren: int64(expectedNumberOfChildren),
						ID:           expectedCode,
						Label:        expectedLabel,
						HasData:      expectedHasData,
					})
				})
			})
		})
	})
}
