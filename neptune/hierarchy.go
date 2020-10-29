package neptune

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	"github.com/ONSdigital/graphson"
	"github.com/ONSdigital/log.go/log"
)

// batchSize is the size of each batch for queries that are run concurrently in batches
const batchSize = 250

// Type check to ensure that NeptuneDB implements the driver.Hierarchy interface
var _ driver.Hierarchy = (*NeptuneDB)(nil)

func (n *NeptuneDB) CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error {
	return nil
}

// GetCodesWithData returns a list of values that are present in nodes with label _{instanceID}_{dimensionName}
func (n *NeptuneDB) GetCodesWithData(ctx context.Context, attempt int, instanceID, dimensionName string) (codes []string, err error) {
	codesWithDataStmt := fmt.Sprintf(
		query.GetCodesWithData,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
	}

	log.Event(ctx, "getting instance dimension codes that have data", log.INFO, logData)

	codes, err = n.getStringList(codesWithDataStmt)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", codesWithDataStmt)
	}
	return
}

// GetGenericHierarchyNodeIDs obtains a list of node IDs for generic hierarchy nodes for the provided codeListID, which have a code in the provided list.
func (n *NeptuneDB) GetGenericHierarchyNodeIDs(ctx context.Context, attempt int, codeListID string, codes []string) (nodeIDs []string, err error) {
	return n.doGetGenericHierarchyNodeIDs(ctx, attempt, codeListID, codes, false)
}

// GetGenericHierarchyAncestriesIDs obtains a list of node IDs for the parents of the hierarchy nodes that have a code in the provided list.
func (n *NeptuneDB) GetGenericHierarchyAncestriesIDs(ctx context.Context, attempt int, codeListID string, codes []string) (nodeIDs []string, err error) {
	return n.doGetGenericHierarchyNodeIDs(ctx, attempt, codeListID, codes, true)
}

func (n *NeptuneDB) doGetGenericHierarchyNodeIDs(ctx context.Context, attempt int, codeListID string, codes []string, ancestries bool) (nodeIDs []string, err error) {
	codes = unique(codes)

	logData := log.Data{
		"fn":           "GetGenericHierarchyNodeIDs",
		"code_list_id": codeListID,
		"num_codes":    len(codes),
	}

	processBatch := func(chunkCodes []string) (ret []string, err error) {
		codesString := `["` + strings.Join(chunkCodes, `","`) + `"]`
		var stmt string
		if ancestries {
			stmt = fmt.Sprintf(
				query.GetGenericHierarchyAncestryIDs,
				codeListID,
				codesString,
			)
			log.Event(ctx, "getting generic hierarchy node ancestry ids for the provided codes", log.INFO, logData)
		} else {
			stmt = fmt.Sprintf(
				query.GetGenericHierarchyNodeIDs,
				codeListID,
				codesString,
			)
			log.Event(ctx, "getting generic hierarchy node leaf ids for the provided codes", log.INFO, logData)
		}

		ids, err := n.getStringList(stmt)
		if err != nil {
			return nil, errors.Wrapf(err, "Gremlin query failed: %q", stmt)
		}
		return unique(ids), nil
	}

	ids, _, errs := processInConcurrentBatches(codes, processBatch, batchSize)
	if errs != nil && len(errs) > 0 {
		return []string{}, errs[0]
	}
	return createArray(ids), nil
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
	log.Event(ctx, "cloning all nodes from the generic hierarchy", log.INFO, logData)

	if _, err = n.exec(gremStmt); err != nil {
		log.Event(ctx, "cannot get vertices during cloning", log.ERROR, logData, log.Error(err))
		return
	}

	return
}

// CloneNodesFromIDs clones the generic hierarchy nodes which have a code that is present in the provided codes array.
func (n *NeptuneDB) CloneNodesFromIDs(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string, ids []string, hasData bool) (err error) {
	ids = unique(ids)

	processBatch := func(chunkIDs []string) (ret []string, err error) {
		idsStr := `'` + strings.Join(chunkIDs, `','`) + `'`
		gremStmt := fmt.Sprintf(
			query.CloneHierarchyNodesFromIDs,
			idsStr,
			instanceID,
			dimensionName,
			hasData,
			codeListID,
		)
		logData := log.Data{"fn": "CloneNodes",
			"gremlin":        gremStmt,
			"instance_id":    instanceID,
			"dimension_name": dimensionName,
			"code_list_id":   codeListID,
			"has_data":       hasData,
			"num_ids":        len(chunkIDs),
		}
		log.Event(ctx, "cloning necessary nodes from the generic hierarchy", log.INFO, logData)

		if _, err = n.exec(gremStmt); err != nil {
			log.Event(ctx, "cannot get vertices during cloning", log.ERROR, logData, log.Error(err))
			return nil, err
		}
		return nil, nil
	}

	_, _, errs := processInConcurrentBatches(ids, processBatch, batchSize)
	if errs != nil && len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// CountNodes returns the number of hierarchy nodes for the provided instanceID and dimensionName
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

// CloneRelationshipsFromIDs clones the hs_parent edges between clones that have parent relationship according to the generic hierarchy nodes
func (n *NeptuneDB) CloneRelationshipsFromIDs(ctx context.Context, attempt int, instanceID, dimensionName string, ids []string) error {
	ids = unique(ids)

	processBatch := func(chunkIDs []string) (ret []string, err error) {
		idsStr := `'` + strings.Join(chunkIDs, `','`) + `'`
		gremStmt := fmt.Sprintf(
			query.CloneHierarchyRelationshipsFromIDs,
			idsStr,
			instanceID,
			dimensionName,
			instanceID,
			dimensionName,
		)

		logData := log.Data{
			"fn":             "CloneRelationships",
			"instance_id":    instanceID,
			"dimension_name": dimensionName,
			"num_ids":        len(chunkIDs),
			"gremlin":        gremStmt,
		}
		log.Event(ctx, "cloning relationships from the generic hierarchy", log.INFO, logData)

		if _, err := n.getEdges(gremStmt); err != nil {
			log.Event(ctx, "cannot find edges while cloning relationships", log.ERROR, logData, log.Error(err))
			return nil, err
		}
		return nil, nil
	}

	_, _, errs := processInConcurrentBatches(ids, processBatch, batchSize)
	if errs != nil && len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// GetHierarchyNodeIDs returns a list of IDs for the cloned hierarchy nodes for a provided instanceID and dimensionName
func (n *NeptuneDB) GetHierarchyNodeIDs(ctx context.Context, attempt int, instanceID, dimensionName string) (ids []string, err error) {
	stmt := fmt.Sprintf(
		query.GetHierarchyNodeIDs,
		instanceID,
		dimensionName,
	)
	logData := log.Data{
		"fn":             "GetHierarchyNodeIDs",
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
		"gremlin":        stmt,
	}
	log.Event(ctx, "getting ids of cloned hierarchy nodes", log.INFO, logData)

	ids, err = n.getStringList(stmt)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", stmt)
	}
	return unique(ids), nil
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

// RemoveCloneEdgesFromSourceIDs removes the 'clone-of' edges between a set of cloned nodes and their corresponding generic hierarchy nodes
func (n *NeptuneDB) RemoveCloneEdgesFromSourceIDs(ctx context.Context, attempt int, ids []string) (err error) {
	ids = unique(ids)
	logData := log.Data{
		"fn":      "RemoveCloneEdges",
		"num_ids": len(ids),
	}

	processBatch := func(chunkIDs []string) (ret []string, err error) {
		idsStr := `'` + strings.Join(chunkIDs, `','`) + `'`
		gremStmt := fmt.Sprintf(
			query.RemoveCloneMarkersFromSourceIDs,
			idsStr,
		)
		log.Event(ctx, "removing edges to generic hierarchy", log.INFO, logData)

		if _, err = n.exec(gremStmt); err != nil {
			log.Event(ctx, "exec failed while removing edges during removal of unwanted cloned edges", log.ERROR, logData, log.Error(err))
			return
		}
		return
	}

	_, _, errs := processInConcurrentBatches(ids, processBatch, batchSize)
	if errs != nil && len(errs) > 0 {
		return errs[0]
	}
	return nil
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

// SetNumberOfChildrenFromIDs sets a property called 'numberOfChildren' to the value indegree of edges 'hasParent'
// ids are the node IDs that will be updated
func (n *NeptuneDB) SetNumberOfChildrenFromIDs(ctx context.Context, attempt int, ids []string) (err error) {
	ids = unique(ids)
	logData := log.Data{
		"fn":      "SetNumberOfChildren",
		"num_ids": len(ids),
	}

	processBatch := func(chunkIDs []string) (ret []string, err error) {
		idsStr := `'` + strings.Join(chunkIDs, `','`) + `'`
		gremStmt := fmt.Sprintf(
			query.SetNumberOfChildrenFromIDs,
			idsStr,
		)

		log.Event(ctx, "setting number-of-children property value on the instance hierarchy nodes", log.INFO, logData)

		if _, err = n.exec(gremStmt); err != nil {
			log.Event(ctx, "cannot find vertices while setting nChildren on hierarchy nodes", log.ERROR, logData, log.Error(err))
			return
		}
		return
	}

	_, _, errs := processInConcurrentBatches(ids, processBatch, batchSize)
	if errs != nil && len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (n *NeptuneDB) SetHasData(ctx context.Context, attempt int, instanceID, dimensionName string) (err error) {

	codesWithDataStmt := fmt.Sprintf(
		query.GetCodesWithData,
		instanceID,
		dimensionName,
	)

	logData := log.Data{
		"instance_id":    instanceID,
		"dimension_name": dimensionName,
	}

	log.Event(ctx, "getting instance dimension codes that have data", log.INFO, logData)

	codes, err := n.getStringList(codesWithDataStmt)
	if err != nil {
		return errors.Wrapf(err, "Gremlin query failed: %q", codesWithDataStmt)
	}

	codesString := `["` + strings.Join(codes, `","`) + `"]`

	gremStmt := fmt.Sprintf(
		query.SetHasData,
		instanceID,
		dimensionName,
		codesString,
	)

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
	// Note the call to buildHierarchyNode below does much more than meets the eye,
	// including launching new queries in of itself to fetch child nodes, and
	// breadcrumb nodes.
	wantBreadcrumbs := false // Because meaningless for a root node
	if node, err = n.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs); err != nil {
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
	// Note the call to buildHierarchyNode below does much more than meets the eye,
	// including launching new queries in of itself to fetch child nodes, and
	// breadcrumb nodes.
	wantBreadcrumbs := true // Because we are at depth in the hierarchy
	if node, err = n.buildHierarchyNode(vertex, instanceID, dimension, wantBreadcrumbs); err != nil {
		log.Event(ctx, "Cannot extract related information needed from hierarchy node", log.ERROR, logData, log.Error(err))
		return
	}
	return
}
