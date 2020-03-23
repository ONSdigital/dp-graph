package models

// Datasets represents a list of dataset objects
type Datasets struct {
	Items []Dataset
}

// Dataset represents an individual dataset
type Dataset struct {
	ID             string
	DimensionLabel string
	Editions       []DatasetEdition
}

// DatasetEdition represents an object containing edition data
type DatasetEdition struct {
	ID            string
	CodeListID    string
	LatestVersion int
}
