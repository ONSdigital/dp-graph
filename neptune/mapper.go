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
	"github.com/ONSdigital/log.go/log"
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
		log.Event(ctx, "bad GetProp code", log.ERROR, logData, log.Error(err))
		return
	}

	if res.Label, err = v.GetProperty("label"); err != nil {
		log.Event(ctx, "bad label", log.ERROR, logData, log.Error(err))
		return
	}
	if res.NoOfChildren, err = v.GetPropertyInt64("numberOfChildren"); err != nil {
		log.Event(ctx, "bad numberOfChildren", log.ERROR, logData, log.Error(err))
		return
	}
	if res.HasData, err = v.GetPropertyBool("hasData"); err != nil {
		log.Event(ctx, "bad hasData", log.ERROR, logData, log.Error(err))
		return
	}
	if res.Order, err = v.GetPropertyInt64("order"); err != nil {
		if err != graphson.ErrorPropertyNotFound {
			log.Event(ctx, "bad order", log.ERROR, logData, log.Error(err))
			return
		}
		log.Event(ctx, "order not defined for this hierarchy node", log.INFO, logData)
		err = nil
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
			log.Event(ctx, "get", log.ERROR, logData, log.Error(err))
			return
		}
		if int64(len(childVertices)) != res.NoOfChildren {
			logData["num_children_prop"] = res.NoOfChildren
			logData["num_children_get"] = len(childVertices)
			logData["node_id"] = res.ID
			log.Event(ctx, "child count mismatch", log.WARN, logData)
		}
		var childElement *models.HierarchyElement
		for _, child := range childVertices {
			if childElement, err = convertVertexToElement(child); err != nil {
				log.Event(ctx, "converting child", log.ERROR, logData, log.Error(err))
				return
			}
			res.Children = append(res.Children, childElement)
		}
	}
	// Fetch new data from the database concerned with the node's breadcrumbs.
	if wantBreadcrumbs {
		res.Breadcrumbs, err = n.buildBreadcrumbs(instanceID, dimension, res.ID)
		if err != nil {
			log.Event(ctx, "building breadcrumbs", log.ERROR, logData, log.Error(err))
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
		log.Event(ctx, "getVertices", log.ERROR, logData, log.Error(err))
		return nil, err
	}
	elements := []*models.HierarchyElement{}
	for _, ancestor := range ancestorVertices {
		element, err := convertVertexToElement(ancestor)
		if err != nil {
			log.Event(ctx, "convertVertexToElement", log.ERROR, logData, log.Error(err))
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
		log.Event(ctx, "bad GetProp code", log.ERROR, logData, log.Error(err))
		return
	}

	if res.Label, err = v.GetProperty("label"); err != nil {
		log.Event(ctx, "bad label", log.ERROR, logData, log.Error(err))
		return
	}
	if res.NoOfChildren, err = v.GetPropertyInt64("numberOfChildren"); err != nil {
		log.Event(ctx, "bad numberOfChildren", log.ERROR, logData, log.Error(err))
		return
	}
	if res.HasData, err = v.GetPropertyBool("hasData"); err != nil {
		log.Event(ctx, "bad hasData", log.ERROR, logData, log.Error(err))
		return
	}
	if res.Order, err = v.GetPropertyInt64("order"); err != nil {
		if err != graphson.ErrorPropertyNotFound {
			log.Event(ctx, "bad order", log.ERROR, logData, log.Error(err))
			return
		}
		log.Event(ctx, "order not defined for this hierarchy node", log.INFO, logData)
		err = nil
	}
	return
}
