package models

const (
	fmtEditions                   = "/datasets/%s/editions/%s"
	fmtEditionsVersions           = "/datasets/%s/editions/%s/versions/%s"
	fmtEditionsVersionsDimensions = "/datasets/%s/editions/%s/versions/%s/dimensions/%s"
)

// Datasets represents the model returned from the api datasets
// endpoint
type Datasets struct {
	Items      []Dataset `json:"items"`
	Count      int       `json:"count"`
	Offset     int       `json:"offset"`
	Limit      int       `json:"limit"`
	TotalCount int       `json:"total_count"`
}

// Dataset represents an individual model dataset
type Dataset struct {
	Links          *DatasetLinks    `json:"links"`
	DimensionLabel string           `json:"dimension_label"`
	Editions       []DatasetEdition `json:"editions"`
}

// DatasetEdition represents an object containing dataset edition links
type DatasetEdition struct {
	Links *DatasetEditionLinks `json:"links"`
}

// DatasetEditionLinks represents a list of links related to the dataset edition
type DatasetEditionLinks struct {
	Self             *Link `json:"self"`
	DatasetDimension *Link `json:"dataset_dimension"`
	LatestVersion    *Link `json:"latest_version"`
}

// DatasetLinks represents the links returned specifically for a dataset
type DatasetLinks struct {
	Self *Link `json:"self"`
}
