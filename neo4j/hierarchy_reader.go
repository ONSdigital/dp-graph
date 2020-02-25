package neo4j

import (
	"context"
	"fmt"

	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/neo4j/mapper"
	"github.com/ONSdigital/dp-graph/neo4j/query"
	"github.com/ONSdigital/dp-hierarchy-api/models"
	"github.com/ONSdigital/log.go/log"
)

type neoArgMap map[string]interface{}

// GetHierarchyCodelist obtains the codelist id for this hierarchy (also, check that it exists)
func (n *Neo4j) GetHierarchyCodelist(ctx context.Context, instanceID, dimension string) (string, error) {
	neoStmt := fmt.Sprintf(query.HierarchyExists, instanceID, dimension)
	logData := log.Data{"statement": neoStmt}

	//create a pointer to a string for the mapper func
	codelistID := new(string)

	if err := n.Read(neoStmt, mapper.HierarchyCodelist(codelistID), false); err != nil {
		log.Event(ctx, "getProps query", log.ERROR, logData, log.Error(err))
		return "", err
	}

	return *codelistID, nil
}

// GetHierarchyRoot returns the upper-most node for a given hierarchy
func (n *Neo4j) GetHierarchyRoot(ctx context.Context, instanceID, dimension string) (*models.Response, error) {
	neoStmt := fmt.Sprintf(query.GetHierarchyRoot, instanceID, dimension)
	return n.queryResponse(instanceID, dimension, neoStmt, nil)
}

// GetHierarchyElement gets a node in a given hierarchy for a given code
func (n *Neo4j) GetHierarchyElement(ctx context.Context, instanceID, dimension, code string) (res *models.Response, err error) {
	neoStmt := fmt.Sprintf(query.GetHierarchyElement, instanceID, dimension)

	if res, err = n.queryResponse(instanceID, dimension, neoStmt, neoArgMap{"code": code}); err != nil {
		return
	}

	if res.Breadcrumbs, err = n.getAncestry(instanceID, dimension, code); err != nil {
		return
	}

	return
}

// queryResponse performs DB query (neoStmt, neoArgs) returning Response (should be singular)
func (n *Neo4j) queryResponse(instanceID, dimension string, neoStmt string, neoArgs neoArgMap) (*models.Response, error) {
	ctx := context.Background()
	logData := log.Data{"statement": neoStmt, "neo_args": neoArgs}
	log.Event(ctx, "QueryResponse executing get query", log.INFO, logData)

	res := &models.Response{}
	var err error

	if err = n.ReadWithParams(neoStmt, neoArgs, mapper.Hierarchy(res), false); err != nil {
		return nil, err
	}

	if res.Children, err = n.getChildren(instanceID, dimension, res.ID); err != nil && err != driver.ErrNotFound {
		return nil, err
	}

	return res, nil
}

func (n *Neo4j) getChildren(instanceID, dimension, code string) ([]*models.Element, error) {
	ctx := context.Background()
	log.Event(ctx, "get children", log.INFO, log.Data{"instance": instanceID, "dimension": dimension, "code": code})
	neoStmt := fmt.Sprintf(query.GetChildren, instanceID, dimension)

	return n.queryElements(instanceID, dimension, neoStmt, neoArgMap{"code": code})
}

// getAncestry retrieves a list of ancestors for this code - as breadcrumbs (ordered, nearest first)
func (n *Neo4j) getAncestry(instanceID, dimension, code string) ([]*models.Element, error) {
	ctx := context.Background()
	log.Event(ctx, "get ancestry", log.INFO, log.Data{"instance_id": instanceID, "dimension": dimension, "code": code})
	neoStmt := fmt.Sprintf(query.GetAncestry, instanceID, dimension)

	return n.queryElements(instanceID, dimension, neoStmt, neoArgMap{"code": code})
}

// queryElements returns a list of models.Elements from the database
func (n *Neo4j) queryElements(instanceID, dimension, neoStmt string, neoArgs neoArgMap) ([]*models.Element, error) {
	ctx := context.Background()
	logData := log.Data{"db_statement": neoStmt, "db_args": neoArgs}
	log.Event(ctx, "QueryElements: executing get query", log.INFO, logData)

	res := &mapper.HierarchyElements{}
	if err := n.ReadWithParams(neoStmt, neoArgs, mapper.HierarchyElement(res), false); err != nil {
		log.Event(ctx, "QueryElements query", log.ERROR, logData, log.Error(err))
		return nil, err
	}

	return res.List, nil
}
