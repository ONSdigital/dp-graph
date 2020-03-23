package models

// Editions represents the editions response model
type Editions struct {
	Items []Edition
}

// Edition represents a single edition response model
type Edition struct {
	Edition string
	Label   string
}
