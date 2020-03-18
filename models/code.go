package models

// CodeResults contains an array of codes which can be paginated
type CodeResults struct {
	Items []Code
}

// Code for a single dimensions type
type Code struct {
	ID    string
	Code  string
	Label string
}
