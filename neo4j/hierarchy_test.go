package neo4j

import (
	"context"
	"errors"
	"fmt"
	"github.com/ONSdigital/dp-graph/v2/neo4j/mapper"
	"github.com/ONSdigital/dp-graph/v2/neo4j/query"
	"testing"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	graph "github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/neo4j/internal"
	bolt "github.com/ONSdigital/golang-neo4j-bolt-driver"
	neoErrors "github.com/ONSdigital/golang-neo4j-bolt-driver/errors"
	boltstructures "github.com/ONSdigital/golang-neo4j-bolt-driver/structures/graph"
	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/messages"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	q             string
	instanceID    = "instanceID"
	dimensionName = "dimensionName"
	codeListID    = "codeListID"

	errExec = errors.New("error executing neo4j query")
)

func Test_CreateInstanceHierarchyConstraints(t *testing.T) {

	Convey("Given a bolt connection", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When CreateInstanceHierarchyConstraints is called", func() {
			err := db.CreateInstanceHierarchyConstraints(context.Background(), 1, instanceID, dimensionName)

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then db.Exec should be called once for the expected query", func() {
				expectedQuery := fmt.Sprintf(
					"CREATE CONSTRAINT ON (n:`_hierarchy_node_%s_%s`) ASSERT n.code IS UNIQUE;",
					instanceID,
					dimensionName,
				)

				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func Test_CreateInstanceHierarchyConstraints_NeoErrExec(t *testing.T) {

	Convey("Given a bolt connection that returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errExec
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When CreateInstanceHierarchyConstraints is called", func() {
			err := db.CreateInstanceHierarchyConstraints(context.Background(), 1, instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})

			Convey("Then the returned error should be that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrNonRetriable{errExec})
			})
		})
	})
}

func TestStore_CreateInstanceHierarchyConstraints_NeoExecRetry(t *testing.T) {
	Convey("Given a bolt connection that returns a transient error", t, func() {
		transientNeoErr := neoErrors.Wrap(messages.FailureMessage{Metadata: map[string]interface{}{"code": "Neo.TransientError.Transaction.ConstraintsChanged"}}, "constraint error msg")

		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, transientNeoErr
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When CreateInstanceHierarchyConstraints is called", func() {
			err := db.CreateInstanceHierarchyConstraints(context.Background(), 1, instanceID, dimensionName)

			Convey("Then boltConn.ExecNeo should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 5)
			})

			Convey("Then the returned error should wrap that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrAttemptsExceededLimit{transientNeoErr})
			})
		})
	})
}

func TestStore_CloneNodes(t *testing.T) {

	expectedQuery := fmt.Sprintf(
		"MATCH (n:`_generic_hierarchy_node_%s`) WITH n "+
			"MERGE (:`_hierarchy_node_%s_%s` { code:n.code,label:n.label,code_list:{code_list}, hasData:false });",
		codeListID,
		instanceID,
		dimensionName,
	)

	Convey("Given a bolt connection", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When CloneNodes is called", func() {
			err := db.CloneNodes(context.Background(), 1, instanceID, codeListID, dimensionName)

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestStore_CloneNodes_NeoerrExec(t *testing.T) {

	Convey("Given a bolt connection that returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errExec
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When CloneNodes is called", func() {
			err := db.CloneNodes(context.Background(), 1, instanceID, codeListID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})

			Convey("Then the returned error should be that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrNonRetriable{errExec})
			})
		})
	})
}

func TestStore_CloneRelationships(t *testing.T) {

	expectedQuery := fmt.Sprintf(
		"MATCH (genericNode:`_generic_hierarchy_node_%s`)-[r:hasParent]->(genericParent:`_generic_hierarchy_node_%s`)"+
			" WITH genericNode, genericParent"+
			" MATCH (node:`_hierarchy_node_%s_%s` { code:genericNode.code })"+
			", (parent:`_hierarchy_node_%s_%s` { code:genericParent.code }) "+
			"MERGE (node)-[r:hasParent]->(parent);",
		codeListID,
		codeListID,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	Convey("Given a bolt connection", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When CloneRelationships is called", func() {
			err := db.CloneRelationships(context.Background(), 1, instanceID, codeListID, dimensionName)

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestStore_CloneRelationships_NeoErrExec(t *testing.T) {

	Convey("Given a bolt connection that returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errExec
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When cloneRelationships is called", func() {

			err := db.CloneRelationships(context.Background(), 1, instanceID, codeListID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})

			Convey("Then the returned error should be that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrNonRetriable{errExec})
			})
		})
	})
}

func TestStore_SetNumberOfChildren(t *testing.T) {

	expectedQuery := fmt.Sprintf(
		"MATCH (n:`_hierarchy_node_%s_%s`)"+
			" with n SET n.numberOfChildren = size((n)<-[:hasParent]-(:`_hierarchy_node_%s_%s`))",
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	Convey("Given a bolt connection", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When SetNumberOfChildren is called", func() {
			err := db.SetNumberOfChildren(context.Background(), 1, instanceID, dimensionName)

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestStore_SetNumberOfChildren_NeoErrExec(t *testing.T) {

	Convey("Given a bolt connection that returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errExec
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When SetNumberOfChildren is called", func() {
			err := db.SetNumberOfChildren(context.Background(), 1, instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})

			Convey("Then the returned error should be that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrNonRetriable{errExec})
			})
		})
	})
}

func TestStore_SetHasData(t *testing.T) {

	expectedQuery := fmt.Sprintf("MATCH (n:`_hierarchy_node_%s_%s`), (p:`_%s_%s`) "+
		"WHERE n.code = p.value SET n.hasData=true",
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	Convey("Given a bolt connection", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When SetHasData is called", func() {
			err := db.SetHasData(context.Background(), 1, instanceID, dimensionName)

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestStore_SetHasData_NeoErrExec(t *testing.T) {

	Convey("Given a bolt connection that returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errExec
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When SetHasData is called", func() {
			err := db.SetHasData(context.Background(), 1, instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})

			Convey("Then the returned error should be that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrNonRetriable{errExec})
			})
		})
	})
}

func TestStore_MarkNodesToRemain(t *testing.T) {

	expectedQuery := fmt.Sprintf("MATCH (parent:`_hierarchy_node_%s_%s`)<-[:hasParent*]-(child:`_hierarchy_node_%s_%s`) "+
		"WHERE child.hasData=true set parent.remain=true set child.remain=true",
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	Convey("Given a bolt connection", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When MarkNodesToRemain is called", func() {
			err := db.MarkNodesToRemain(context.Background(), 1, instanceID, dimensionName)

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestStore_MarkNodesToRemain_NeoErrExec(t *testing.T) {

	Convey("Given a bolt connection that returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errExec
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When MarkNodesToRemain is called", func() {
			err := db.MarkNodesToRemain(context.Background(), 1, instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})

			Convey("Then the returned error should be that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrNonRetriable{errExec})
			})
		})
	})
}

func TestStore_RemoveNodesNotMarkedToRemain(t *testing.T) {

	expectedQuery := fmt.Sprintf("MATCH (node:`_hierarchy_node_%s_%s`) WHERE NOT EXISTS(node.remain) DETACH DELETE node",
		instanceID,
		dimensionName,
	)

	Convey("Given a bolt connection", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When RemoveNodesNotMarkedToRemain is called", func() {
			err := db.RemoveNodesNotMarkedToRemain(context.Background(), 1, instanceID, dimensionName)

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestStore_RemoveNodesNotMarkedToRemain_NeoErrExec(t *testing.T) {

	Convey("Given a bolt connection that returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errExec
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When RemoveNodesNotMarkedToRemain is called", func() {
			err := db.RemoveNodesNotMarkedToRemain(context.Background(), 1, instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})

			Convey("Then the returned error should be that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrNonRetriable{errExec})
			})
		})
	})
}

func TestStore_RemoveRemainMarker(t *testing.T) {

	expectedQuery := fmt.Sprintf("MATCH (node:`_hierarchy_node_%s_%s`) REMOVE node.remain",
		instanceID,
		dimensionName,
	)

	Convey("Given a bolt connection", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return &internal.ResultMock{}, nil
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When RemoveRemainMarker is called", func() {
			err := db.RemoveRemainMarker(context.Background(), 1, instanceID, dimensionName)

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
				So(driver.ExecCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestStore_RemoveRemainMarker_NeoErrExec(t *testing.T) {

	Convey("Given a bolt connection that returns an error", t, func() {
		driver := &internal.Neo4jDriverMock{
			ExecFunc: func(q string, params map[string]interface{}) (bolt.Result, error) {
				return nil, errExec
			},
		}

		db := &Neo4j{driver, 5, 30}

		Convey("When RemoveRemainMarker is called", func() {
			err := db.RemoveRemainMarker(context.Background(), 1, instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				So(len(driver.ExecCalls()), ShouldEqual, 1)
			})

			Convey("Then the returned error should be that returned from the exec call", func() {
				So(err, ShouldResemble, graph.ErrNonRetriable{errExec})
			})
		})
	})
}

func TestStore_HierarchyExists(t *testing.T) {

	node := boltstructures.Node{
		Properties: map[string]interface{}{},
	}
	mockNeoResponse := &mapper.Result{
		Data:  []interface{}{node},
		Meta:  nil,
		Index: 0,
	}

	Convey("Given a bolt connection that returns a node", t, func() {
		neoDriverMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
				mapp(mockNeoResponse)
				return nil
			},
		}

		db := &Neo4j{neoDriverMock, 5, 30}

		Convey("When HierarchyExists is called", func() {
			hierarchyExists, err := db.HierarchyExists(context.Background(), instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				expectedQuery := fmt.Sprintf(query.HierarchyExists, instanceID, dimensionName)
				So(len(neoDriverMock.ReadWithParamsCalls()), ShouldEqual, 1)
				So(neoDriverMock.ReadWithParamsCalls()[0].Query, ShouldEqual, expectedQuery)
			})

			Convey("Then the return value should be true", func() {
				So(hierarchyExists, ShouldBeTrue)
			})

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})

	Convey("Given a bolt connection that returns multiple nodes", t, func() {
		neoDriverMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
				mapp(mockNeoResponse)
				mapp(mockNeoResponse)
				mapp(mockNeoResponse)
				return nil
			},
		}

		db := &Neo4j{neoDriverMock, 5, 30}

		Convey("When HierarchyExists is called", func() {
			hierarchyExists, err := db.HierarchyExists(context.Background(), instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				expectedQuery := fmt.Sprintf(query.HierarchyExists, instanceID, dimensionName)
				So(len(neoDriverMock.ReadWithParamsCalls()), ShouldEqual, 1)
				So(neoDriverMock.ReadWithParamsCalls()[0].Query, ShouldEqual, expectedQuery)
			})

			Convey("Then the return value should be false", func() {
				So(hierarchyExists, ShouldBeFalse)
			})

			Convey("Then the expected error is returned", func() {
				So(err, ShouldEqual, driver.ErrMultipleFound)
			})
		})
	})

	Convey("Given a bolt connection that returns an empty result set", t, func() {
		neoDriverMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
				return nil
			},
		}

		db := &Neo4j{neoDriverMock, 5, 30}

		Convey("When HierarchyExists is called", func() {
			hierarchyExists, err := db.HierarchyExists(context.Background(), instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				expectedQuery := fmt.Sprintf(query.HierarchyExists, instanceID, dimensionName)
				So(len(neoDriverMock.ReadWithParamsCalls()), ShouldEqual, 1)
				So(neoDriverMock.ReadWithParamsCalls()[0].Query, ShouldEqual, expectedQuery)
			})

			Convey("Then the return value should be false", func() {
				So(hierarchyExists, ShouldBeFalse)
			})

			Convey("Then the returned error should be nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})

	Convey("Given a bolt connection that returns a not found error", t, func() {

		expectedError := driver.ErrNotFound

		neoDriverMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
				return expectedError
			},
		}

		db := &Neo4j{neoDriverMock, 5, 30}

		Convey("When HierarchyExists is called", func() {
			hierarchyExists, err := db.HierarchyExists(context.Background(), instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				expectedQuery := fmt.Sprintf(query.HierarchyExists, instanceID, dimensionName)
				So(len(neoDriverMock.ReadWithParamsCalls()), ShouldEqual, 1)
				So(neoDriverMock.ReadWithParamsCalls()[0].Query, ShouldEqual, expectedQuery)
			})

			Convey("Then the return value should be false", func() {
				So(hierarchyExists, ShouldBeFalse)
			})

			Convey("Then the error returned is nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})

	Convey("Given a bolt connection that returns an error", t, func() {

		expectedError := errors.New("test error")

		neoDriverMock := &internal.Neo4jDriverMock{
			ReadWithParamsFunc: func(query string, params map[string]interface{}, mapp mapper.ResultMapper, single bool) error {
				return expectedError
			},
		}

		db := &Neo4j{neoDriverMock, 5, 30}

		Convey("When HierarchyExists is called", func() {
			hierarchyExists, err := db.HierarchyExists(context.Background(), instanceID, dimensionName)

			Convey("Then db.Exec should be called once for the expected query", func() {
				expectedQuery := fmt.Sprintf(query.HierarchyExists, instanceID, dimensionName)
				So(len(neoDriverMock.ReadWithParamsCalls()), ShouldEqual, 1)
				So(neoDriverMock.ReadWithParamsCalls()[0].Query, ShouldEqual, expectedQuery)
			})

			Convey("Then the return value should be false", func() {
				So(hierarchyExists, ShouldBeFalse)
			})

			Convey("Then the expected error is returned", func() {
				So(err, ShouldEqual, expectedError)
			})
		})
	})
}
