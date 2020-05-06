package neptune

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	"github.com/ONSdigital/graphson"
	"github.com/ONSdigital/log.go/log"
)

// Type check to ensure that NeptuneDB implements the driver.Hierarchy interface
var _ driver.Hierarchy = (*NeptuneDB)(nil)

func (n *NeptuneDB) CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return nil
}

func (n *NeptuneDB) CloneNodes(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.CloneHierarchyNodes,
		codeListID,
		instanceID,
		dimensionName,
		codeListID,
	)
	logData := log.Data{"fn": "CloneNodes",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"code_list_id":   codeListID,
		"dimension_name": dimensionName,
	}
	log.Event(ctx, "cloning nodes from the generic hierarchy", log.INFO, logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.Event(ctx, "cannot get vertices during cloning", log.ERROR, logData, log.Error(err))
		return
	}

	return
}

func (n *NeptuneDB) CountNodes(ctx context.Context, instanceID, dimensionName string) (count int64, err error) {
	gremStmt := fmt.Sprintf(query.CountHierarchyNodes, instanceID, dimensionName)
	logData := log.Data{
		"fn":             "CountNodes",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
	}
	log.Event(ctx, "counting nodes in the new instance hierarchy", log.INFO, logData)

	if count, err = n.getNumber(gremStmt); err != nil {
		log.Event(ctx, "cannot count nodes in a hierarchy", log.ERROR, logData, log.Error(err))
		return
	}
	return
}

func (n *NeptuneDB) CloneRelationships(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.CloneHierarchyRelationships,
		codeListID,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"fn":             "CloneRelationships",
		"instance_id":    instanceID,
		"code_list_id":   codeListID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}
	log.Event(ctx, "cloning relationships from the generic hierarchy", log.INFO, logData)

	if _, err = n.getEdges(gremStmt); err != nil {
		log.Event(ctx, "cannot find edges while cloning relationships", log.ERROR, logData, log.Error(err))
		return
	}

	return n.RemoveCloneEdges(ctx, attempt, instanceID, dimensionName)
}

func (n *NeptuneDB) RemoveCloneEdges(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.RemoveCloneMarkers,
		instanceID,
		dimensionName,
	)
	logData := log.Data{
		"fn":             "RemoveCloneEdges",
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}
	log.Event(ctx, "removing edges to generic hierarchy", log.INFO, logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.Event(ctx, "exec failed while removing edges during removal of unwanted cloned edges", log.ERROR, logData, log.Error(err))
		return
	}
	return
}

func (n *NeptuneDB) SetNumberOfChildren(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.SetNumberOfChildren,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"fn":             "SetNumberOfChildren",
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}

	log.Event(ctx, "setting number-of-children property value on the instance hierarchy nodes", log.INFO, logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.Event(ctx, "cannot find vertices while setting nChildren on hierarchy nodes", log.ERROR, logData, log.Error(err))
		return
	}

	return
}

func (n *NeptuneDB) SetHasData(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(
		query.SetHasData,
		instanceID,
		dimensionName,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}

	log.Event(ctx, "setting has-data property on the instance hierarchy", log.INFO, logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.Event(ctx, "cannot find vertices while setting hasData on hierarchy nodes", log.ERROR, logData, log.Error(err))
		return
	}

	return
}

func (n *NeptuneDB) MarkNodesToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(query.MarkNodesToRemain,
		instanceID,
		dimensionName,
		// instanceID,
		// dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}

	log.Event(ctx, "marking nodes to remain after trimming sparse branches", log.INFO, logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.Event(ctx, "cannot find vertices while marking hierarchy nodes to keep", log.ERROR, logData, log.Error(err))
		return
	}

	return
}

func (n *NeptuneDB) RemoveNodesNotMarkedToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(query.RemoveNodesNotMarkedToRemain, instanceID, dimensionName)
	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        gremStmt,
	}

	log.Event(ctx, "removing nodes not marked to remain after trimming sparse branches", log.INFO, logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.Event(ctx, "exec query failed while removing hierarchy nodes to cull", log.ERROR, logData, log.Error(err))
		return
	}
	return
}

func (n *NeptuneDB) RemoveRemainMarker(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {
	gremStmt := fmt.Sprintf(query.RemoveRemainMarker, instanceID, dimensionName)
	logData := log.Data{
		"fn":             "RemoveRemainMarker",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
	}
	log.Event(ctx, "removing the remain property from the nodes that remain", log.INFO, logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.Event(ctx, "exec query failed while removing spent remain markers from hierarchy nodes", log.ERROR, logData, log.Error(err))
		return
	}
	return
}

func (n *NeptuneDB) GetHierarchyCodelist(ctx context.Context, instanceID, dimension string) (codelistID string, err error) {
	gremStmt := fmt.Sprintf(query.HierarchyExists, instanceID, dimension)
	logData := log.Data{
		"fn":             "GetHierarchyCodelist",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"dimension_name": dimension,
	}

	var vertex graphson.Vertex
	if vertex, err = n.getVertex(gremStmt); err != nil {
		log.Event(ctx, "cannot get vertices  while searching for code list node related to hierarchy node", log.ERROR, logData, log.Error(err))
		return
	}
	if codelistID, err = vertex.GetProperty("code_list"); err != nil {
		log.Event(ctx, "cannot read code_list property from node", log.ERROR, logData, log.Error(err))
		return
	}
	return
}

func (n *NeptuneDB) GetHierarchyRoot(ctx context.Context, instanceID, dimension string) (node *models.HierarchyResponse, err error) {
	gremStmt := fmt.Sprintf(query.GetHierarchyRoot, instanceID, dimension)
	logData := log.Data{
		"fn":             "GetHierarchyRoot",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"dimension_name": dimension,
	}

	var vertices []graphson.Vertex
	if vertices, err = n.getVertices(gremStmt); err != nil {
		log.Event(ctx, "getVertices failed: cannot find hierarchy root node candidates ", log.ERROR, logData, log.Error(err))
		return
	}
	if len(vertices) == 0 {
		err = driver.ErrNotFound
		log.Event(ctx, "Cannot find hierarchy root node", log.ERROR, logData, log.Error(err))
		return
	}
	if len(vertices) > 1 {
		err = driver.ErrMultipleFound
		log.Event(ctx, "Cannot identify hierarchy root node because are multiple candidates", log.ERROR, logData, log.Error(err))
		return
	}
	var vertex graphson.Vertex
	vertex = vertices[0]
	// Note the call to buildHierarchyNodeFromGraphsonVertex below does much more than meets the eye,
	// including launching new queries in of itself to fetch child nodes, and
	// breadcrumb nodes.
	wantBreadcrumbs := false // Because meaningless for a root node
	if node, err = n.buildHierarchyNodeFromGraphsonVertex(vertex, instanceID, dimension, wantBreadcrumbs); err != nil {
		log.Event(ctx, "Cannot extract related information needed from hierarchy node", log.ERROR, logData, log.Error(err))
		return
	}
	return
}

func (n *NeptuneDB) GetHierarchyElement(ctx context.Context, instanceID, dimension, code string) (node *models.HierarchyResponse, err error) {
	gremStmt := fmt.Sprintf(query.GetHierarchyElement, instanceID, dimension, code)
	logData := log.Data{
		"fn":             "GetHierarchyElement",
		"gremlin":        gremStmt,
		"instance_id":    instanceID,
		"code_list_id":   code,
		"dimension_name": dimension,
	}

	var vertex graphson.Vertex
	if vertex, err = n.getVertex(gremStmt); err != nil {
		log.Event(ctx, "Cannot find vertex when looking for specific hierarchy node", log.ERROR, logData, log.Error(err))
		return
	}
	// Note the call to buildHierarchyNodeFromGraphsonVertex below does much more than meets the eye,
	// including launching new queries in of itself to fetch child nodes, and
	// breadcrumb nodes.
	wantBreadcrumbs := true // Because we are at depth in the hierarchy
	if node, err = n.buildHierarchyNodeFromGraphsonVertex(vertex, instanceID, dimension, wantBreadcrumbs); err != nil {
		log.Event(ctx, "Cannot extract related information needed from hierarchy node", log.ERROR, logData, log.Error(err))
		return
	}
	return
}
