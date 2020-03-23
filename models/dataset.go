package models

const (
	fmtEditions                   = "/datasets/%s/editions/%s"
	fmtEditionsVersions           = "/datasets/%s/editions/%s/versions/%s"
	fmtEditionsVersionsDimensions = "/datasets/%s/editions/%s/versions/%s/dimensions/%s"
)

// Datasets represents the model returned from the api datasets
// endpoint
type Datasets struct {
	Items []Dataset
}

// Dataset represents an individual model dataset
type Dataset struct {
	ID             string
	DimensionLabel string
	Editions       []DatasetEdition
}

// DatasetEdition represents an object containing dataset edition links
type DatasetEdition struct {
	ID            string
	CodeListID    string
	LatestVersion int
}
