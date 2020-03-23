package models

// CodeListResults contains an array of code lists
type CodeListResults struct {
	Items []CodeList
}

// CodeList containing the ID of a codelist
type CodeList struct {
	ID string
}
