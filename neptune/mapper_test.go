package neptune

import (
	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/graphson"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func Test_buildHierarchyNode(t *testing.T) {

	poolMock := &internal.NeptunePoolMock{GetFunc: internal.ReturnThreeCodeLists}
	db := mockDB(poolMock)

	Convey("Given an example vertex returned from neptune", t, func() {

		expectedLabel := "code-label"
		expectedCode := "code"
		vertex := makeDummyVertex("test-id", "vertex-label",
			map[string]interface{}{
				"code":             expectedCode,
				"label":            expectedLabel,
				"numberOfChildren": map[string]interface{}{"@type": "g:Int64", "@value": float64(0)},
				"hasData":          true,
			})
		wantBreadcrumbs := false
		dimension := "dimension"
		instanceID := "instance-id"

		Convey("When buildHierarchyNode is called", func() {

			hierarchyNode, err := db.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs)

			Convey("Then the expected values are mapped onto the returned hierarchy response", func() {
				So(err, ShouldBeNil)
				So(hierarchyNode.ID, ShouldEqual, expectedCode)
				So(hierarchyNode.Label, ShouldEqual, expectedLabel)
				So(hierarchyNode.NoOfChildren, ShouldEqual, 0)
				So(hierarchyNode.HasData, ShouldEqual, true)
			})
		})
	})
}

func makeDummyVertex(vertexID, vertexLabel string, params map[string]interface{}) graphson.Vertex {
	properties := make(map[string][]graphson.VertexProperty)
	for label, value := range params {
		var vp []graphson.VertexProperty
		vSlice, ok := value.([]interface{})
		if ok {
			for _, p := range vSlice {
				vertexProperty := makeDummyVertexProperty(label, p)
				vp = append(vp, vertexProperty)
			}
		} else {
			vertexProperty := makeDummyVertexProperty(label, value)
			vp = append(vp, vertexProperty)
		}
		properties[label] = vp
	}
	vertexValue := graphson.VertexValue{
		ID:         vertexID,
		Label:      vertexLabel,
		Properties: properties,
	}
	return graphson.Vertex{
		Type:  "g:Vertex",
		Value: vertexValue,
	}
}

func makeDummyVertexProperty(label string, value interface{}) graphson.VertexProperty {
	return graphson.VertexProperty{
		Type: "g:VertexProperty",
		Value: graphson.VertexPropertyValue{
			ID: graphson.GenericValue{
				Type:  "Type",
				Value: 1,
			},
			Value: value,
			Label: label,
		},
	}
}
