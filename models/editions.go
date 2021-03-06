package models

// Editions represents a list of editions
type Editions struct {
	Items []Edition
}

// Edition represents a single edition object
type Edition struct {
	ID    string
	Label string
}
