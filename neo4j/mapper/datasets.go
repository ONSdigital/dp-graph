package mapper

import (
	dpbolt "github.com/ONSdigital/dp-bolt/bolt"
	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/graph"
)

//Datasets map of datasetID to dataset
type Datasets map[string]datasetData
type DatasetEditions map[string]Versions
type Versions []int

type datasetData struct {
	DimensionLabel string
	Editions       DatasetEditions
}

const (
	datasetsURI = "/code-lists/%s/editions/%s/codes/%s/datasets"
)

//CodesDatasets returns a dpbolt.ResultMapper which converts dpbolt.Result to Datasets
func (m *Mapper) CodesDatasets(datasets Datasets) dpbolt.ResultMapper {
	return func(r *dpbolt.Result) error {
		var err error

		var node graph.Node
		if node, err = getNode(r.Data[0]); err != nil {
			return err
		}

		var relationship graph.Relationship
		if relationship, err = getRelationship(r.Data[1]); err != nil {
			return err
		}

		var datasetID string
		if datasetID, err = getStringProperty("dataset_id", node.Properties); err != nil {
			return err
		}

		var datasetEdition string
		if datasetEdition, err = getStringProperty("edition", node.Properties); err != nil {
			return err
		}

		var version int64
		if version, err = getint64Property("version", node.Properties); err != nil {
			return err
		}

		var dimensionLabel string
		if dimensionLabel, err = getStringProperty("label", relationship.Properties); err != nil {
			return err
		}

		dataset, ok := datasets[datasetID]
		if !ok {
			dataset = datasetData{
				DimensionLabel: dimensionLabel,
				Editions:       make(DatasetEditions, 0),
			}
		}

		if dataset.Editions[datasetEdition] == nil {
			dataset.Editions[datasetEdition] = make(Versions, 0)
		}

		dataset.Editions[datasetEdition] = append(dataset.Editions[datasetEdition], int(version))

		datasets[datasetID] = dataset

		return nil
	}
}
