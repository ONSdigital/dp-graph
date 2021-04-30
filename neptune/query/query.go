package query

// Neptune implements a slight variance of Gremlin, so queries must be written with both specs in mind
// (https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-gremlin-differences.html)
// Important practices:
// 1) .property() function must contain 'single' where not a list, as the Neptune default is 'set'

const (
	// code lists
	GetCodeLists          = `g.V().hasLabel('_code_list')`
	GetCodeListsFiltered  = `g.V().hasLabel('_code_list').has('%s', true)`
	GetCodeList           = `g.V().hasLabel('_code_list').has('listID', '%s')`
	CodeListExists        = `g.V().hasLabel('_code_list').has('listID', '%s').count()`
	CodeListEditionExists = `g.V().hasLabel('_code_list').has('listID', '%s').has('edition', '%s').count()`
	CountCodes            = `g.V().has('_code_list','listID', '%s').has('edition', '%s')` +
		`.in('usedBy').count()`
	CountOrderedEdges      = `g.V().has('_code_list','listID', '%s').has('edition', '%s').inE('usedBy').has('order').count()`
	GetCodesAlphabetically = `g.V().has('_code_list','listID', '%s').has('edition', '%s')` +
		`.inE('usedBy').as('usedBy')` +
		`.outV().order().by('value',incr).as('code')` +
		`.select('usedBy', 'code').by('label').by('value')` +
		`.unfold().select(values)`
	GetCodesWithOrder = `g.V().has('_code_list', 'listID', '%s').has('edition', '%s')` +
		`.inE('usedBy').order().by('order',incr).as('usedBy')` +
		`.outV().as('code')` +
		`.select('usedBy', 'code').by('label').by('value')` +
		`.unfold().select(values)`
	CodeExists = `g.V().hasLabel('_code_list')` +
		`.has('listID', '%s').has('edition', '%s')` +
		`.in('usedBy').has('value', "%s").count()`
	GetUsedByEdgesFromNodeIDs = `g.V().hasLabel('_code_list').has('_code_list', 'listID', '%s')` +
		`.inE('usedBy').where(otherV().has('value', within(%s))).as('usedBy')` +
		`.outV().values('value').as('code').union(select('code', 'usedBy'))`

	/*
		This query harvests data from both edges and nodes, so we collapse
		the response to contain only strings - to make it parse-able with
		the graphson string-list method.

		%s Parameters: codeListID, codeListEdition, codeValue

		Naming:

			r: usedBy relation
			rl: usedBy.label
			c: code node
			d: dataset
			de: dataset.edition
			dv: dataset.version
	*/
	GetCodeDatasets = `g.V().hasLabel('_code_list').has('listID', '%s').
		has('edition','%s').
		inE('usedBy').as('r').values('label').as('rl').select('r').
		match(
			__.as('r').outV().has('value',"%s").as('c'),
			__.as('c').out('inDataset').as('d').
				select('d').values('edition').as('de').
				select('d').values('version').as('dv').
				select('d').values('dataset_id').as('did'),
			__.as('d').has('is_published',true)).
		union(select('rl', 'de', 'dv', 'did')).unfold().select(values)
	`

	// GetGenericHierarchyNodeIDs gets the IDs of the generic hierarchy nodes whose 'code' is in the provided list of codes, returning a list of {node_id=<node_id> node_code=<node_code>} maps
	GetGenericHierarchyNodeIDs = `g.V().hasLabel('_generic_hierarchy_node_%s').has('code',within(%s)).as('gh')` +
		`.id().as('node_id').select('gh').values('code').as('node_code').select('gh').select('node_id', 'node_code')`

	// GetGenericHierarchyAncestryIDs gets IDs of the ancestries (parents, grandparents, etc) of the generic hierarchy nodes
	// whose 'code' is in the provided list of codes.
	GetGenericHierarchyAncestryIDs = `g.V().hasLabel('_generic_hierarchy_node_%s').has('code',within(%s)).repeat(out('hasParent')).emit().as('gh')` +
		`.id().as('node_id').select('gh').values('code').as('node_code').select('gh').select('node_id', 'node_code')`

	// crete 'hasCode' edge from a generic hierarchy node to the provided code node, only if it does not exist already:
	// 1. get code node from code and codelist, this is uniquely determined by the usedBy edge between them
	// 2. create an 'hasCode' edge between the provided nodeID and the code
	CreateHasCodeEdge = `g.V().hasLabel('_code').has('value', '%s').where(out('usedBy').hasLabel('_code_list').has('listID','%s')).as('dest')` +
		`.V('%s').coalesce(__.outE('hasCode'), __.addE('hasCode').to(select('dest')))`

	// GetHierarchyNodeIDs gets the IDs of the cloned hierarchy nodes for a particular instanceID and dimensionName
	GetHierarchyNodeIDs = `g.V().hasLabel('_hierarchy_node_%s_%s').id()`

	// hierarchy write
	CloneHierarchyNodes = `g.V().hasLabel('_generic_hierarchy_node_%s').as('old')` +
		`.addV('_hierarchy_node_%s_%s')` +
		`.property(single,'code',select('old').values('code'))` +
		`.property(single,'label',select('old').values('label'))` +
		`.property(single,'hasData', false)` +
		`.property('code_list','%s').as('new')` +
		`.addE('clone_of').to('old')` +
		`.select('new')`

	// CloneHierarchyNodesFromIDs traverses the provided node IDs and creates a clone for each one, thus:
	// 1. get generic hierarchy nodes from IDs
	// 2. create a new hierarchy node for the provided 'instance' and 'dimensionName'
	// 3. copy 'code' property from the generic hierarchy node to the new node
	// 4. copy 'label' from the generic hierarchy node to the new node
	// 5. set 'hasData' to true or false, according to the provided value
	// 6. set 'code_list' property to the provided value
	// 7. create a 'clone_of' edge between the new node and the generic node
	CloneHierarchyNodesFromIDs = `g.V(%s).as('old')` +
		`.addV('_hierarchy_node_%s_%s')` +
		`.property(single,'code',select('old').values('code'))` +
		`.property(single,'label',select('old').values('label'))` +
		`.property(single,'hasData', %t)` +
		`.property('code_list','%s').as('new')` +
		`.addE('clone_of').to('old')`

	// CloneOrderFromIDs copies the order property from the code of a generic hierarchy node to its clone, thus:
	// 1. get generic hierarchy nodes from IDs
	// 2. traverse 'hasCode' edge to go to the corresponding code node
	//    (this edge exists so that the query can run quickly, up to x1000 quicker than nested queries)
	// 3. traverse 'usedBy' edge that points to the codeList that we are using
	// 4. select 'order' property from the 'usedBy' edge
	// 5. go back to the generic hierarchy node 'old', and traverse 'clone_of' edge to go to the cloned hierarchy node
	// 6. set 'order' property to the cloned hierarchy node
	CloneOrderFromIDs = `g.V(%s).as('old')` +
		`.out('hasCode')` +
		`.outE('usedBy').where(otherV().hasLabel('_code_list').has('_code_list', 'listID', '%s'))` +
		`.values('order').as('o')` +
		`.select('old').in('clone_of')` +
		`.property(single,'order', select('o'))`

	// CountHierarchyNodes returns the number of hierarchy nodes for the provided instanceID and dimensionName
	CountHierarchyNodes = `g.V().hasLabel('_hierarchy_node_%s_%s').count()`

	CloneHierarchyRelationships = `g.V().hasLabel('_generic_hierarchy_node_%s').as('oc')` +
		`.out('hasParent')` +
		`.in('clone_of').hasLabel('_hierarchy_node_%s_%s').as('p')` +
		`.select('oc').in('clone_of').hasLabel('_hierarchy_node_%s_%s')` +
		`.addE('hasParent').to('p')`

	// CloneHierarchyRelationshipsFromIDs clones the 'hasParent' edges from the generic hierarchy structure to the cloned structure,
	// for a provided set of generic hierarchy node IDs.
	CloneHierarchyRelationshipsFromIDs = `g.V(%s).as('oc')` +
		`.out('hasParent')` +
		`.in('clone_of').hasLabel('_hierarchy_node_%s_%s').as('p')` +
		`.select('oc').in('clone_of').hasLabel('_hierarchy_node_%s_%s')` +
		`.addE('hasParent').to('p')`

	// RemoveCloneMarkers drops the 'clone_of' outEdges from the provided nodes
	RemoveCloneMarkers              = `g.V().hasLabel('_hierarchy_node_%s_%s').outE('clone_of').drop()`
	RemoveCloneMarkersFromSourceIDs = `g.V(%s).outE('clone_of').drop()`

	// SetNumberOfChildren sets a property called 'numberOfChildren' to the value indegree of edges 'hasParent'
	SetNumberOfChildren        = `g.V().hasLabel('_hierarchy_node_%s_%s').property(single,'numberOfChildren',__.in('hasParent').count())`
	SetNumberOfChildrenFromIDs = `g.V(%s).property(single,'numberOfChildren',__.in('hasParent').count())`

	GetCodesWithData = `g.V().hasLabel('_%s_%s').values('value')`
	SetHasData       = `g.V().hasLabel('_hierarchy_node_%s_%s').as('v').has('code',within(%s)).property(single,'hasData',true)`

	MarkNodesToRemain = `g.V().hasLabel('_hierarchy_node_%s_%s').has('hasData', true).property(single,'remain',true)` +
		`.repeat(out('hasParent')).emit().property(single,'remain',true)`
	RemoveNodesNotMarkedToRemain = `g.V().hasLabel('_hierarchy_node_%s_%s').not(has('remain',true)).drop()`
	RemoveRemainMarker           = `g.V().hasLabel('_hierarchy_node_%s_%s').has('remain').properties('remain').drop()`

	// hierarchy read
	HierarchyExists           = `g.V().hasLabel('_hierarchy_node_%s_%s').limit(1)`
	GetHierarchyRoot          = `g.V().hasLabel('_hierarchy_node_%s_%s').not(outE('hasParent'))`
	GetHierarchyElement       = `g.V().hasLabel('_hierarchy_node_%s_%s').has('code','%s')`
	CountChildrenWithOrder    = `g.V().hasLabel('_hierarchy_node_%s_%s').has('code','%s').in('hasParent').has('order').count()`
	GetChildrenAlphabetically = `g.V().hasLabel('_hierarchy_node_%s_%s').has('code','%s').in('hasParent').order().by('label')`
	GetChildrenWithOrder      = `g.V().hasLabel('_hierarchy_node_%s_%s').has('code','%s').in('hasParent').order().by('order',incr)`
	// Note this query is recursive
	GetAncestry = `g.V().hasLabel('_hierarchy_node_%s_%s').has('code', '%s').repeat(out('hasParent')).emit()`

	// instance - import process
	CreateInstance = `g.addV('_%s_Instance').property(id, '_%s_Instance').property(single,'header',"%s")`
	CheckInstance  = `g.V('_%s_Instance').count()`

	GetCode                          = `g.V().hasLabel('_code').has('value',"%s").where(out('usedBy').hasLabel('_code_list').has('listID','%s')).id()`
	CreateInstanceToCodeRelationship = `g.V('_%s_Instance').as('i').V('%s').addE('inDataset').to('i')`
	AddVersionDetailsToInstance      = `g.V().hasId('_%s_Instance').property(single,'dataset_id','%s').` +
		`property(single,'edition','%s').property(single,'version','%d')`
	SetInstanceIsPublished = `g.V().hasId('_%s_Instance').property(single,'is_published',true)`
	CountObservations      = `g.V().hasLabel('_%s_observation').count()`

	//instance - parts
	AddInstanceDimensionsPart         = `g.V().hasId('_%s_Instance')`
	AddInstanceDimensionsPropertyPart = `.property('dimensions', "%s")`

	// dimension
	GetDimension               = `g.V('%s').id()`
	DropDimensionRelationships = `g.V('%s').bothE().drop().iterate();`
	DropDimension              = `g.V('%s').drop()`

	CreateDimension                       = `g.addV('_%s_%s').property(id, '%s').property('value',"%s")`
	CreateDimensionToInstanceRelationship = `g.V('_%s_Instance').as('inst')` +
		`.V('%s').addE('HAS_DIMENSION').to('inst')`

	// observation
	GetObservations      = `g.V(%s).id()`
	GetObservationsEdges = `g.V(%s).bothE().id()`
	DropObservationEdges = `g.E(%s).drop().iterate();`
	DropObservations     = `g.V(%s).drop()`

	CreateObservationPart  = `.addV('_%s_observation').property(id, '%s').property(single, 'value', '%s')`
	DimensionLookupPart    = `.V('%s').as('%s')`
	AddObservationEdgePart = `.V('%s').addE('isValueOf').to('%s')`

	GetInstanceHeaderPart       = `g.V().hasId('_%s_Instance').values('header')`
	GetAllObservationsPart      = `g.V().hasLabel('_%s_observation')`
	GetFirstDimensionPart       = `g.V().hasId(%s).in('isValueOf')`
	GetAdditionalDimensionsPart = `.where(out('isValueOf').hasId(%s).fold().count(local).is_(%d))`
	GetObservationValuesPart    = `.values('value')`
	LimitPart                   = `.limit(%d)`
)
