package driver

import (
	"context"

	"github.com/ONSdigital/dp-graph/models"
	"github.com/ONSdigital/dp-graph/observation"
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
	GetCodes(ctx context.Context, codeListID, edition string) (*models.CodeResults, error)
	GetCode(ctx context.Context, codeListID, edition string, code string) (*models.Code, error)
	GetCodeDatasets(ctx context.Context, codeListID, edition string, code string) (*models.Datasets, error)
}

// Hierarchy defines functions to create and retrieve generic and instance hierarchy nodes
type Hierarchy interface {
	CreateInstanceHierarchyConstraints(ctx context.Context, attempt int, instanceID, dimensionName string) error
	CloneNodes(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error
	CountNodes(ctx context.Context, instanceID, dimensionName string) (count int64, err error)
	CloneRelationships(ctx context.Context, attempt int, instanceID, codeListID, dimensionName string) error
	SetNumberOfChildren(ctx context.Context, attempt int, instanceID, dimensionName string) error
	SetHasData(ctx context.Context, attempt int, instanceID, dimensionName string) error
	MarkNodesToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error
	RemoveNodesNotMarkedToRemain(ctx context.Context, attempt int, instanceID, dimensionName string) error
	RemoveRemainMarker(ctx context.Context, attempt int, instanceID, dimensionName string) error

	GetHierarchyCodelist(ctx context.Context, instanceID, dimension string) (string, error)
	GetHierarchyRoot(ctx context.Context, instanceID, dimension string) (*models.HierarchyResponse, error)
	GetHierarchyElement(ctx context.Context, instanceID, dimension, code string) (*models.HierarchyResponse, error)
}

// Observation defines functions to create and retrieve observation nodes
type Observation interface {
	// StreamCSVRows returns a reader which the caller is ultimately responsible for closing
	// This allows for large volumes of data to be read from a stream without significant
	// memory overhead.
	StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error)
	InsertObservationBatch(ctx context.Context, attempt int, instanceID string, observations []*models.Observation, dimensionIDs map[string]string) error
}

// Instance defines functions to create, update and retrieve details about instances
type Instance interface {
	CreateInstanceConstraint(ctx context.Context, i *models.Instance) error
	CreateInstance(ctx context.Context, i *models.Instance) error
	AddDimensions(ctx context.Context, i *models.Instance) error
	CreateCodeRelationship(ctx context.Context, i *models.Instance, codeListID, code string) error
	InstanceExists(ctx context.Context, i *models.Instance) (bool, error)
	CountInsertedObservations(ctx context.Context, instanceID string) (count int64, err error)
	AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error
	SetInstanceIsPublished(ctx context.Context, instanceID string) error
}

// Dimension defines functions to create dimension nodes
type Dimension interface {
	InsertDimension(ctx context.Context, cache map[string]string, i *models.Instance, d *models.Dimension) (*models.Dimension, error)
}
