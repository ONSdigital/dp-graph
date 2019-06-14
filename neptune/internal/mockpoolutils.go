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

// ReturnMalformedIntNilInterfaceRequestErr is a mock implementation for
// NeptunePool functions that return  (Interface{}, error) which always returns an
// error that is judged to be not transient by neptune.isTransientError
var ReturnMalformedNilInterfaceRequestErr = func(q string, bindings, rebindings map[string]string) (interface{}, error) {
	return nil, errors.New(" MALFORMED REQUEST ")
}

// ReturnThreeCodeLists is mock implementation for NeptunePool.Get() that always
// returns a slice of three graphson.Vertex(s) of type "_code_list" and with
// a "listID" property set to "listID0", "listID1", and "ListID2" respectively.
var ReturnThreeCodeLists = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	codeLists := []graphson.Vertex{}
	for i := 0; i < 3; i++ {
		vertex := makeVertexWithProperty("_code_list", "listID", i)
		codeLists = append(codeLists, vertex)
	}
	return codeLists, nil
}

/*
makeVertexProperty makes a graphson.Vertex of a given type (e.g. "_code_list").
It will form a string like "listID_2", where "listID" is the IDRoot parameter,
and "2" is the IDCount. It additionally has a single property set named
"listID" and with the value as the listID_2.
*/
func makeVertexWithProperty(vertexType string, IDRoot string, IDCount int) graphson.Vertex {
	thisListID := fmt.Sprintf("%s_%d", IDRoot, IDCount)
	gv := graphson.GenericValue{Type: "string", Value: IDRoot}
	pv := graphson.VertexPropertyValue{
		ID:    gv,
		Label: IDRoot,
		Value: thisListID,
	}
	vertexProperty := graphson.VertexProperty{Type: "string", Value: pv}
	vertexProperties := []graphson.VertexProperty{vertexProperty}
	vertexValue := graphson.VertexValue{
		ID:         "unused_vertex_value_ID",
		Label:      IDRoot,
		Properties: map[string][]graphson.VertexProperty{IDRoot: vertexProperties},
	}
	vertex := graphson.Vertex{Type: vertexType, Value: vertexValue}
	return vertex
}
