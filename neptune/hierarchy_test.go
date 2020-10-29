package neptune

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-graph/v2/neptune/internal"
	"github.com/ONSdigital/graphson"
	"github.com/ONSdigital/gremgo-neptune"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx               = context.Background()
	testCodeListID    = "cpih1dim1aggid"
	testInstanceID    = "f0a2f3f2-cc86-4bbb-a549-ffc99c89292c"
	testDimensionName = "aggregate"
	testAttempt       = 1
	testCodes         = []string{"cpih1dim1S90401", "cpih1dim1S90402"}
	testIds           = []string{"cpih1dim1aggid--cpih1dim1S90401", "cpih1dim1aggid--cpih1dim1S90402"}
	testAllIds        = []string{"cpih1dim1aggid--cpih1dim1S90401", "cpih1dim1aggid--cpih1dim1S90402",
		"cpih1dim1aggid--cpih1dim1G90400", "cpih1dim1aggid--cpih1dim1G90400",
		"cpih1dim1aggid--cpih1dim1T90000", "cpih1dim1aggid--cpih1dim1T90000",
		"cpih1dim1aggid--cpih1dim1A0", "cpih1dim1aggid--cpih1dim1A0"}
	testClonedIds = []string{
		"62bab579-e923-7cb2-3be0-34d09dc0567b",
		"acbab579-e923-87df-e59a-9daf2ffed388",
		"b6bab57a-604d-8a7f-59f5-1d496c9b3ca5",
		"08bab57a-604d-9cd9-492f-e879cee05502",
		"6cbab57a-604d-f176-9370-c60c19369801",
	}
)

func TestNeptuneDB_GetCodesWithData(t *testing.T) {

	Convey("Given a mocked neptune DB that returns a code list", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnCodesList,
		}
		db := mockDB(poolMock)

		Convey("When GetCodesWithData is called", func() {
			codes, err := db.GetCodesWithData(ctx, testAttempt, testInstanceID, testDimensionName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected list of codes is returned", func() {
				So(len(codes), ShouldEqual, 2)
				So(codes, ShouldContain, "cpih1dim1S90401")
				So(codes, ShouldContain, "cpih1dim1S90402")
				expectedQuery := `g.V().hasLabel('_f0a2f3f2-cc86-4bbb-a549-ffc99c89292c_aggregate').values('value')`
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestNeptuneDB_GetGenericHierarchyNodeIDs(t *testing.T) {

	Convey("Given a mocked neptune DB that returns a list of generic hierarchy node IDs (leaves)", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnGenericHierarchyLeavesIDs,
		}
		db := mockDB(poolMock)

		Convey("When GetGenericHierarchyNodeIDs is called", func() {
			ids, err := db.GetGenericHierarchyNodeIDs(ctx, testAttempt, testCodeListID, testCodes)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected list of IDs is returned and the expected query is executed, in any order of IDs", func() {
				So(len(ids), ShouldEqual, 2)
				So(ids, ShouldContain, "cpih1dim1aggid--cpih1dim1S90401")
				So(ids, ShouldContain, "cpih1dim1aggid--cpih1dim1S90402")
				expectedQueryOp1 := `g.V().hasLabel('_generic_hierarchy_node_cpih1dim1aggid').has('code',within(["cpih1dim1S90401","cpih1dim1S90402"])).id()`
				expectedQueryOp2 := `g.V().hasLabel('_generic_hierarchy_node_cpih1dim1aggid').has('code',within(["cpih1dim1S90402","cpih1dim1S90401"])).id()`
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldBeIn, []string{expectedQueryOp1, expectedQueryOp2})
			})
		})
	})
}

func TestNeptuneDB_GetGenericHierarchyAncestriesIDs(t *testing.T) {

	Convey("Given a mocked neptune DB that returns a list of generic ancestry hierarchy node IDs, with duplicates", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnGenericHierarchyAncestryIDs,
		}
		db := mockDB(poolMock)

		Convey("When GetGenericHierarchyAncestriesIDs is called", func() {
			ids, err := db.GetGenericHierarchyAncestriesIDs(ctx, testAttempt, testCodeListID, testCodes)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected list of unique IDs is returned and teh expected is executed, in any order of IDs", func() {
				So(len(ids), ShouldEqual, 3)
				So(ids, ShouldContain, "cpih1dim1aggid--cpih1dim1G90400")
				So(ids, ShouldContain, "cpih1dim1aggid--cpih1dim1T90000")
				So(ids, ShouldContain, "cpih1dim1aggid--cpih1dim1A0")
				expectedQueryOp1 := `g.V().hasLabel('_generic_hierarchy_node_cpih1dim1aggid').has('code',within(["cpih1dim1S90401","cpih1dim1S90402"])).repeat(out('hasParent')).emit().id()`
				expectedQueryOp2 := `g.V().hasLabel('_generic_hierarchy_node_cpih1dim1aggid').has('code',within(["cpih1dim1S90402","cpih1dim1S90401"])).repeat(out('hasParent')).emit().id()`
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldBeIn, []string{expectedQueryOp1, expectedQueryOp2})
			})
		})
	})
}

func TestNeptuneDB_CloneNodes(t *testing.T) {

	Convey("Given a mocked neptune DB", t, func() {
		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) (responses []gremgo.Response, err error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When CloneNodes is called", func() {
			err := db.CloneNodesFromIDs(ctx, testAttempt, testInstanceID, testCodeListID, testDimensionName, testIds, true)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected query is sent to  Neptune to clone the nodes with the provided ids", func() {
				expectedQueryFmt := `g.V('%s','%s').as('old')` +
					`.addV('_hierarchy_node_f0a2f3f2-cc86-4bbb-a549-ffc99c89292c_aggregate')` +
					`.property(single,'code',select('old').values('code'))` +
					`.property(single,'label',select('old').values('label'))` +
					`.property(single,'hasData', true)` +
					`.property('code_list','cpih1dim1aggid').as('new')` +
					`.addE('clone_of').to('old')`
				expectedQueryOp1 := fmt.Sprintf(expectedQueryFmt, "cpih1dim1aggid--cpih1dim1S90401", "cpih1dim1aggid--cpih1dim1S90402")
				expectedQueryOp2 := fmt.Sprintf(expectedQueryFmt, "cpih1dim1aggid--cpih1dim1S90402", "cpih1dim1aggid--cpih1dim1S90401")
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 1)
				So(poolMock.ExecuteCalls()[0].Query, ShouldBeIn, []string{expectedQueryOp1, expectedQueryOp2})
			})
		})
	})
}

func TestNeptuneDB_CountNodes(t *testing.T) {

	Convey("Given a mocked neptune DB", t, func() {
		var expectedCount int64 = 123
		poolMock := &internal.NeptunePoolMock{
			GetCountFunc: func(q string, bindings map[string]string, rebindings map[string]string) (int64, error) {
				return expectedCount, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When CountNodes is called", func() {
			count, err := db.CountNodes(ctx, testInstanceID, testDimensionName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected query is sent to Neptune and the expected count is returned", func() {
				So(count, ShouldEqual, expectedCount)
				expectedQuery := `g.V().hasLabel('_hierarchy_node_f0a2f3f2-cc86-4bbb-a549-ffc99c89292c_aggregate').count()`
				So(len(poolMock.GetCountCalls()), ShouldEqual, 1)
				So(poolMock.GetCountCalls()[0].Q, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestNeptuneDB_CloneRelationshipsFromIDs(t *testing.T) {

	Convey("Given a mocked neptune DB", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetEFunc: func(q string, bindings, rebindings map[string]string) (resp interface{}, err error) {
				return []graphson.Edge{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When CloneRelationShips is called with duplicated IDs", func() {
			err := db.CloneRelationshipsFromIDs(ctx, testAttempt, testInstanceID, testDimensionName, testAllIds)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected query is sent to Neptune to clone the nodes with the unique provided IDs in any order", func() {
				expectedQPrefix := `g.V('`
				expectedQSuffix := `').as('oc')` +
					`.out('hasParent')` +
					`.in('clone_of').hasLabel('_hierarchy_node_f0a2f3f2-cc86-4bbb-a549-ffc99c89292c_aggregate').as('p')` +
					`.select('oc').in('clone_of').hasLabel('_hierarchy_node_f0a2f3f2-cc86-4bbb-a549-ffc99c89292c_aggregate')` +
					`.addE('hasParent').to('p')`
				So(len(poolMock.GetECalls()), ShouldEqual, 1)
				So(strings.HasPrefix(poolMock.GetECalls()[0].Q, expectedQPrefix), ShouldBeTrue)
				So(strings.Count(poolMock.GetECalls()[0].Q, "'cpih1dim1aggid--cpih1dim1S90401'"), ShouldEqual, 1)
				So(strings.Count(poolMock.GetECalls()[0].Q, "'cpih1dim1aggid--cpih1dim1S90402'"), ShouldEqual, 1)
				So(strings.Count(poolMock.GetECalls()[0].Q, "'cpih1dim1aggid--cpih1dim1G90400'"), ShouldEqual, 1)
				So(strings.Count(poolMock.GetECalls()[0].Q, "'cpih1dim1aggid--cpih1dim1T90000'"), ShouldEqual, 1)
				So(strings.Count(poolMock.GetECalls()[0].Q, "'cpih1dim1aggid--cpih1dim1A0'"), ShouldEqual, 1)
				So(strings.HasSuffix(poolMock.GetECalls()[0].Q, expectedQSuffix), ShouldBeTrue)
			})
		})
	})
}

func TestNeptuneDB_RemoveCloneEdges(t *testing.T) {

	Convey("Given a mocked neptune DB", t, func() {
		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) (responses []gremgo.Response, err error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When RemoveCloneEdges is called", func() {
			err := db.RemoveCloneEdges(ctx, testAttempt, testInstanceID, testDimensionName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the clone relationships are removed", func() {
				expectedQuery := `g.V().hasLabel('_hierarchy_node_f0a2f3f2-cc86-4bbb-a549-ffc99c89292c_aggregate').outE('clone_of').drop()`
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 1)
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}

func TestNeptuneDB_RemoveCloneEdgesFromSourceIDs(t *testing.T) {

	Convey("Given a mocked neptune DB", t, func() {
		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) (responses []gremgo.Response, err error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When RemoveCloneEdgesFromSourceIDs is called", func() {
			err := db.RemoveCloneEdgesFromSourceIDs(ctx, testAttempt, testClonedIds)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the clone relationships are removed", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 1)
				expectedQPrefix := `g.V('`
				expectedQSuffix := `').outE('clone_of').drop()`
				So(strings.HasPrefix(poolMock.ExecuteCalls()[0].Query, expectedQPrefix), ShouldBeTrue)
				for _, id := range testClonedIds {
					So(strings.Count(poolMock.ExecuteCalls()[0].Query, id), ShouldEqual, 1)
				}
				So(strings.HasSuffix(poolMock.ExecuteCalls()[0].Query, expectedQSuffix), ShouldBeTrue)
			})
		})
	})
}

func TestNeptuneDB_GetHierarchyNodeIDs(t *testing.T) {

	Convey("Given a mocked neptune DB", t, func() {
		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnHierarchyNodeIDs,
		}
		db := mockDB(poolMock)

		Convey("When GetHierarchyNodeIDs is called", func() {
			ids, err := db.GetHierarchyNodeIDs(ctx, testAttempt, testInstanceID, testDimensionName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected query is sent to Neptune to obtain the cloned hierarchy node IDs, and the expected IDs are returned", func() {
				So(len(ids), ShouldEqual, 5)
				for _, id := range testClonedIds {
					So(ids, ShouldContain, id)
				}
				expectedQuery := `g.V().hasLabel('_hierarchy_node_f0a2f3f2-cc86-4bbb-a549-ffc99c89292c_aggregate').id()`
				So(len(poolMock.GetStringListCalls()), ShouldEqual, 1)
				So(poolMock.GetStringListCalls()[0].Query, ShouldResemble, expectedQuery)
			})
		})
	})
}

func TestNeptuneDB_SetNumberOfChildren(t *testing.T) {

	Convey("Given a mocked neptune DB", t, func() {
		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) (responses []gremgo.Response, err error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When SetNumberOfChildren is called", func() {
			err := db.SetNumberOfChildren(ctx, testAttempt, testInstanceID, testDimensionName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected query is sent to Neptune to clone the nodes with the unique provided IDs in any order", func() {
				expectedQuery := `g.V().hasLabel('_hierarchy_node_f0a2f3f2-cc86-4bbb-a549-ffc99c89292c_aggregate').property(single,'numberOfChildren',__.in('hasParent').count())`
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 1)
				So(poolMock.ExecuteCalls()[0].Query, ShouldResemble, expectedQuery)
			})
		})
	})
}

func TestNeptuneDB_SetNumberOfChildrenFromIDs(t *testing.T) {

	Convey("Given a mocked neptune DB", t, func() {
		poolMock := &internal.NeptunePoolMock{
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) (responses []gremgo.Response, err error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When SetNumberOfChildrenFromIDs is called", func() {
			err := db.SetNumberOfChildrenFromIDs(ctx, testAttempt, testClonedIds)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected query is sent to Neptune to set the number of children for all provided nodeIDs", func() {
				So(len(poolMock.ExecuteCalls()), ShouldEqual, 1)
				expectedQPrefix := `g.V('`
				expectedQSuffix := `').property(single,'numberOfChildren',__.in('hasParent').count())`
				So(strings.HasPrefix(poolMock.ExecuteCalls()[0].Query, expectedQPrefix), ShouldBeTrue)
				for _, id := range testClonedIds {
					So(strings.Count(poolMock.ExecuteCalls()[0].Query, id), ShouldEqual, 1)
				}
				So(strings.HasSuffix(poolMock.ExecuteCalls()[0].Query, expectedQSuffix), ShouldBeTrue)
			})
		})
	})
}

func TestNeptuneDB_SetHasData(t *testing.T) {

	Convey("Given a mocked neptune DB that returns a code list", t, func() {

		ctx := context.Background()
		attempt := 1
		instanceID := "instanceID"
		dimensionName := "dimensionName"

		poolMock := &internal.NeptunePoolMock{
			GetStringListFunc: internal.ReturnCodesList,
			ExecuteFunc: func(query string, bindings map[string]string, rebindings map[string]string) (responses []gremgo.Response, err error) {
				return []gremgo.Response{}, nil
			},
		}
		db := mockDB(poolMock)

		Convey("When SetHasData is called", func() {
			err := db.SetHasData(ctx, attempt, instanceID, dimensionName)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("Then the expected query is sent to Neptune to set the hasData property", func() {
				expectedQuery := `g.V().hasLabel('_hierarchy_node_instanceID_dimensionName').as('v').has('code',within(["cpih1dim1S90401","cpih1dim1S90402"])).property(single,'hasData',true)`
				So(poolMock.ExecuteCalls()[0].Query, ShouldEqual, expectedQuery)
			})
		})
	})
}
