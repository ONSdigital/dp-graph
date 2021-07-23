package neptune

/*
This module is dedicated to the needs of the hierarchy API.
*/

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	"github.com/ONSdigital/graphson"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

func (n *NeptuneDB) buildHierarchyNode(v graphson.Vertex, instanceID, dimension string, wantBreadcrumbs bool) (res *models.HierarchyResponse, err error) {
	ctx := context.Background()
	logData := log.Data{"fn": "buildHierarchyNode"}

	res = &models.HierarchyResponse{}
	// Note we are using the vertex' *code* property for the response model's
	// ID field - because in the case of a hierarchy node, this is the ID
	// used to format links.
	if res.ID, err = v.GetProperty("code"); err != nil {
		log.Error(ctx, "bad GetProp code", err, logData)
		return
	}

	if res.Label, err = v.GetProperty("label"); err != nil {
		log.Error(ctx, "bad label", err, logData)
		return
	}
	if res.NoOfChildren, err = v.GetPropertyInt64("numberOfChildren"); err != nil {
		log.Error(ctx, "bad numberOfChildren", err, logData)
		return
	}
	if res.HasData, err = v.GetPropertyBool("hasData"); err != nil {
		log.Error(ctx, "bad hasData", err, logData)
		return
	}
	if res.Order, err = getOptionalPropertyInt64(v, "order"); err != nil {
		log.Error(ctx, "bad order", err, logData)
		return
	}
	// Fetch new data from the database concerned with the node's children.
	if res.NoOfChildren > 0 && instanceID != "" {

		// Check if order is defined
		var orderCount int64
		gremStmt := fmt.Sprintf(query.CountChildrenWithOrder, instanceID, dimension, res.ID)
		orderCount, err = n.getNumber(gremStmt)
		if err != nil {
			return nil, errors.Wrapf(err, "Gremlin query failed: %q", gremStmt)
		}

		// query depending on the presence of order property in child nodes
		if orderCount > 0 {
			gremStmt = fmt.Sprintf(query.GetChildrenWithOrder, instanceID, dimension, res.ID)
		} else {
			gremStmt = fmt.Sprintf(query.GetChildrenAlphabetically, instanceID, dimension, res.ID)
		}
		logData["statement"] = gremStmt

		var childVertices []graphson.Vertex
		if childVertices, err = n.getVertices(gremStmt); err != nil {
			log.Error(ctx, "get", err, logData)
			return
		}
		if int64(len(childVertices)) != res.NoOfChildren {
			logData["num_children_prop"] = res.NoOfChildren
			logData["num_children_get"] = len(childVertices)
			logData["node_id"] = res.ID
			log.Warn(ctx, "child count mismatch", logData)
		}
		var childElement *models.HierarchyElement
		for _, child := range childVertices {
			if childElement, err = convertVertexToElement(child); err != nil {
				log.Error(ctx, "converting child", err, logData)
				return
			}
			res.Children = append(res.Children, childElement)
		}
	}
	// Fetch new data from the database concerned with the node's breadcrumbs.
	if wantBreadcrumbs {
		res.Breadcrumbs, err = n.buildBreadcrumbs(instanceID, dimension, res.ID)
		if err != nil {
			log.Error(ctx, "building breadcrumbs", err, logData)
		}
	}
	return
}

/*
buildBreadcrumbs launches a new query to the database, to trace the (recursive)
parentage of a hierarchy node. It converts the returned chain of parent
graphson vertices into a chain of models.HierarchyElement, and returns this list of
elements.
*/
func (n *NeptuneDB) buildBreadcrumbs(instanceID, dimension, code string) ([]*models.HierarchyElement, error) {
	ctx := context.Background()
	logData := log.Data{"fn": "buildBreadcrumbs"}
	gremStmt := fmt.Sprintf(query.GetAncestry, instanceID, dimension, code)
	logData["statement"] = gremStmt
	ancestorVertices, err := n.getVertices(gremStmt)
	if err != nil {
		log.Error(ctx, "getVertices", err, logData)
		return nil, err
	}
	elements := []*models.HierarchyElement{}
	for _, ancestor := range ancestorVertices {
		element, err := convertVertexToElement(ancestor)
		if err != nil {
			log.Error(ctx, "convertVertexToElement", err, logData)
			return nil, err
		}
		elements = append(elements, element)
	}
	return elements, nil
}

func convertVertexToElement(v graphson.Vertex) (res *models.HierarchyElement, err error) {
	ctx := context.Background()
	logData := log.Data{"fn": "convertVertexToElement"}
	res = &models.HierarchyElement{}
	// Note we are using the vertex' *code* property for the response model's
	// ID field - because in the case of a hierarchy node, this is the ID
	// used to format links.
	if res.ID, err = v.GetProperty("code"); err != nil {
		log.Error(ctx, "bad GetProp code", err, logData)
		return
	}

	if res.Label, err = v.GetProperty("label"); err != nil {
		log.Error(ctx, "bad label", err, logData)
		return
	}
	if res.NoOfChildren, err = v.GetPropertyInt64("numberOfChildren"); err != nil {
		log.Error(ctx, "bad numberOfChildren", err, logData)
		return
	}
	if res.HasData, err = v.GetPropertyBool("hasData"); err != nil {
		log.Error(ctx, "bad hasData", err, logData)
		return
	}
	if res.Order, err = getOptionalPropertyInt64(v, "order"); err != nil {
		log.Error(ctx, "bad order", err, logData)
		return
	}
	return
}

// getOptionalPropertyInt64 returns the single *int64 value for a given property `key`
// will return nil if the property is not found
// will return an error if the property exists and is not a single string
func getOptionalPropertyInt64(v graphson.Vertex, key string) (*int64, error) {
	val, err := v.GetPropertyInt64(key)
	if err == graphson.ErrorPropertyNotFound {
		return nil, nil
	}
	return &val, err
}
