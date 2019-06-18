package internal

import (
	"fmt"

	"github.com/gedge/graphson"
)

/*
This module provides a handful of mock convenience functions that can be
used to inject behaviour into NeptunePoolMock.
*/

import (
	"errors"
)

// ReturnOne is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 1.
var ReturnOne = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 1, nil
}

// ReturnTwo is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 2.
var ReturnTwo = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 2, nil
}

// ReturnZero is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 0.
var ReturnZero = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 0, nil
}

// ReturnMalformedIntRequestErr is a mock implementation for NeptunePool.GetCount()
// that always returns an error that is judged to be not transient by
// neptune.isTransientError
var ReturnMalformedIntRequestErr = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return -1, errors.New(" MALFORMED REQUEST ")
}

// ReturnMalformedNilInterfaceRequestErr is a mock implementation for
// NeptunePool functions that return  (Interface{}, error) which always returns an
// error that is judged to be not transient by neptune.isTransientError
var ReturnMalformedNilInterfaceRequestErr = func(q string, bindings, rebindings map[string]string) (interface{}, error) {
	return nil, errors.New(" MALFORMED REQUEST ")
}

// ReturnThreeCodeLists is mock implementation for NeptunePool.Get() that always
// returns a slice of three graphson.Vertex(s):
// - of type "_code_list"
// - with a "listID" property set to "listID_0", "listID_1", and "ListID_2" respectively.
// - with an "edition" property set to "my-test-edition"
var ReturnThreeCodeLists = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	codeLists := []graphson.Vertex{}
	for i := 0; i < 3; i++ {
		vertex := makeVertex("_code_list")
		setVertexProperty(&vertex, "listID", fmt.Sprintf("listID_%d", i))
		setVertexProperty(&vertex, "edition", "my-test-edition")
		codeLists = append(codeLists, vertex)
	}
	return codeLists, nil
}

// ReturnThreeUselessVertices is mock implementation for NeptunePool.Get() that always
// returns a slice of three graphson.Vertex(s) of type "_useless_vertex_type", and with
// no properties set.
var ReturnThreeUselessVertices = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	codeLists := []graphson.Vertex{}
	for i := 0; i < 3; i++ {
		vertex := makeVertex("_useless_vertex_type")
		codeLists = append(codeLists, vertex)
	}
	return codeLists, nil
}

/*
makeVertex makes a graphson.Vertex of a given type (e.g. "_code_list").
*/
func makeVertex(vertexType string) graphson.Vertex {
	vertexValue := graphson.VertexValue{
		ID:         "unused_vertex_value_ID",
		Label:      vertexType,
		Properties: map[string][]graphson.VertexProperty{},
	}
	vertex := graphson.Vertex{Type: vertexType, Value: vertexValue}
	return vertex
}

/*
setVertexProperty adds the given key/value to a vertex.
*/
func setVertexProperty(vertex *graphson.Vertex, key string, value string) {
	gv := graphson.GenericValue{Type: "string", Value: key}
	pv := graphson.VertexPropertyValue{
		ID:    gv,
		Label: key,
		Value: value,
	}
	vertexProperty := graphson.VertexProperty{Type: "string", Value: pv}
	vertexProperties := []graphson.VertexProperty{vertexProperty}
	vertex.Value.Properties[key] = vertexProperties
}
