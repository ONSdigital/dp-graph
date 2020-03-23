package models

// CodeListResults contains an array of code lists which can be paginated
type CodeListResults struct {
	Items []CodeList
}

// CodeList containing links to all possible codes
type CodeList struct {
	ID string
}
