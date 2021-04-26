package models

// HierarchyResponse models a node in the hierarchy
type HierarchyResponse struct {
	ID           string
	Label        string
	Children     []*HierarchyElement
	NoOfChildren int64
	Order        *int64 // nil if order property not present
	HasData      bool
	Breadcrumbs  []*HierarchyElement
}

// HierarchyElement is a item in a list within a HierarchyResponse
type HierarchyElement struct {
	ID           string
	Label        string
	NoOfChildren int64
	Order        *int64 // nil if order property not present
	HasData      bool
}
