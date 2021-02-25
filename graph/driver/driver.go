package driver

import (
	"context"

	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/observation"
	health "github.com/ONSdigital/dp-healthcheck/healthcheck"
)

// Driver is the base interface any driver implementation must satisfy
type Driver interface {
	Close(ctx context.Context) error
	Healthcheck() (string, error)
	Checker(ctx context.Context, state *health.CheckState) error
}

// CodeList defines functions to retrieve code list and code nodes
type CodeList interface {
	GetCodeLists(ctx context.Context, filterBy string) (*models.CodeListResults, error)
	GetCodeList(ctx context.Context, codeListID string) (*models.CodeList, error)
	GetEditions(ctx context.Context, codeListID string) (*models.Editions, error)
	GetEdition(ctx context.Context, codeListID, edition string) (*models.Edition, error)
	CountCodes(ctx context.Context, codeListID string, edition string) (int64, error)
	GetCodes(ctx context.Context, codeListID, edition string) (*models.CodeResults, error)
	GetCode(ctx context.Context, codeListID, edition string, code string) (*models.Code, error)
	GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*models.Datasets, error)
}

// Hierarchy defines functions to create and retrieve generic and instance hierarchy nodes
type Hierarchy interface {
	// read
	GetHierarchyCodelist(ctx context.Context, instanceID, dimension string) (string, error)
	GetHierarchyRoot(ctx context.Context, instanceID, dimension string) (*models.HierarchyResponse, error)
	GetHierarchyElement(ctx context.Context, instanceID, dimension, code string) (*models.HierarchyResponse, error)
	GetCodesWithData(ctx context.Context, attempt int, instanceID, dimensionName string) (codes []string, err error)
	GetGenericHierarchyNodeIDs(ctx context.Context, attempt int, codeListID string, codes []string) (nodeIDs map[string]struct{}, err error)
	GetGenericHierarchyAncestriesIDs(ctx context.Context, attempt int, codeListID string, codes []string) (nodeIDs map[string]struct{}, err error)
	CountNodes(ctx context.Context, instanceID, dimensionName string) (count int64, err error)
	GetHierarchyNodeIDs(ctx context.Context, attempt int, instanceID, dimensionName string) (ids map[string]struct{}, err error)
	// write
	CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error
	CloneNodes(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error
	CloneNodesFromIDs(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string, ids map[string]struct{}, hasData bool) (err error)
	CloneRelationships(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error
	CloneRelationshipsFromIDs(ctx context.Context, attempt int, instanceID, dimensionName string, ids map[string]struct{}) error
	SetNumberOfChildren(ctx context.Context, attempt int, instanceID, dimensionName string) (err error)
	SetNumberOfChildrenFromIDs(ctx context.Context, attempt int, ids map[string]struct{}) (err error)
	RemoveCloneEdges(ctx context.Context, attempt int, instanceID, dimensionName string) (err error)
	RemoveCloneEdgesFromSourceIDs(ctx context.Context, attempt int, ids map[string]struct{}) (err error)
	SetHasData(ctx context.Context, attempt int, instanceID, dimensionName string) error
	MarkNodesToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error
	RemoveNodesNotMarkedToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error
	RemoveRemainMarker(ctx context.Context, attempt int, instanceID, dimensionName string) error
}

// Observation defines functions to create and retrieve observation nodes
type Observation interface {
	// StreamCSVRows returns a reader which the caller is ultimately responsible for closing
	// This allows for large volumes of data to be read from a stream without significant
	// memory overhead.
	StreamCSVRows(ctx context.Context, instanceID, filterID string, filters *observation.DimensionFilters, limit *int) (observation.StreamRowReader, error)
	InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionIDs map[string]string) error
}

// Instance defines functions to create, update and retrieve details about instances
type Instance interface {
	CreateInstanceConstraint(ctx context.Context, instanceID string) error
	CreateInstance(ctx context.Context, instanceID string, csvHeaders []string) error
	AddDimensions(ctx context.Context, instanceID string, dimensions []interface{}) error
	CreateCodeRelationship(ctx context.Context, instanceID, codeListID, code string) error
	InstanceExists(ctx context.Context, instanceID string) (bool, error)
	CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error)
	AddVersionDetailsToInstance(ctx context.Context, instanceID, datasetID, edition string, version int) error
	SetInstanceIsPublished(ctx context.Context, instanceID string) error
}

// Dimension defines functions to create dimension nodes
type Dimension interface {
	InsertDimension(ctx context.Context, cache map[string]string, instanceID string, d *models.Dimension) (*models.Dimension, error)
}
