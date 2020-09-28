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
	GetCodes              = `g.V().has('_code_list','listID', '%s').has('edition', '%s')` +
		`.inE("usedBy").as('usedBy')` +
		`.outV().as('code')` +
		`.select('usedBy', 'code').by('label').by('value')` +
		`.unfold().select(values)`
	CodeExists = `g.V().hasLabel('_code_list')` +
		`.has('listID', '%s').has('edition', '%s')` +
		`.in('usedBy').has('value', "%s").count()`

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
				select('d').values('version').as('dv'),
				select('d').values('dataset_id').as('did').
			__.as('d').has('is_published',true)).
		union(select('rl', 'de', 'dv', 'did')).unfold().select(values)
	`

	// hierarchy write
	CloneHierarchyNodes = `g.V().hasLabel('_generic_hierarchy_node_%s').as('old')` +
		`.addV('_hierarchy_node_%s_%s')` +
		`.property(single,'code',select('old').values('code'))` +
		`.property(single,'label',select('old').values('label'))` +
		`.property(single,'hasData', false)` +
		`.property('code_list','%s').as('new')` +
		`.addE('clone_of').to('old')` +
		`.select('new')`
	CountHierarchyNodes         = `g.V().hasLabel('_hierarchy_node_%s_%s').count()`
	CloneHierarchyRelationships = `g.V().hasLabel('_generic_hierarchy_node_%s').as('oc')` +
		`.out('hasParent')` +
		`.in('clone_of').hasLabel('_hierarchy_node_%s_%s').as('p')` +
		`.select('oc').in('clone_of').hasLabel('_hierarchy_node_%s_%s')` +
		`.addE('hasParent').to('p')`
	RemoveCloneMarkers  = `g.V().hasLabel('_hierarchy_node_%s_%s').outE('clone_of').drop()`
	SetNumberOfChildren = `g.V().hasLabel('_hierarchy_node_%s_%s').property(single,'numberOfChildren',__.in('hasParent').count())`

	GetCodesWithData = `g.V().hasLabel('_%s_%s').values('value')`
	SetHasData       = `g.V().hasLabel('_hierarchy_node_%s_%s').as('v').has('code',within(%s)).property(single,'hasData',true)`

	MarkNodesToRemain = `g.V().hasLabel('_hierarchy_node_%s_%s').has('hasData', true).property(single,'remain',true)` +
		`.repeat(out('hasParent')).emit().property(single,'remain',true)`
	RemoveNodesNotMarkedToRemain = `g.V().hasLabel('_hierarchy_node_%s_%s').not(has('remain',true)).drop()`
	RemoveRemainMarker           = `g.V().hasLabel('_hierarchy_node_%s_%s').has('remain').properties('remain').drop()`

	// hierarchy read
	HierarchyExists     = `g.V().hasLabel('_hierarchy_node_%s_%s').limit(1)`
	GetHierarchyRoot    = `g.V().hasLabel('_hierarchy_node_%s_%s').not(outE('hasParent'))`
	GetHierarchyElement = `g.V().hasLabel('_hierarchy_node_%s_%s').has('code','%s')`
	GetChildren         = `g.V().hasLabel('_hierarchy_node_%s_%s').has('code','%s').in('hasParent').order().by('label')`
	// Note this query is recursive
	GetAncestry = `g.V().hasLabel('_hierarchy_node_%s_%s').has('code', '%s').repeat(out('hasParent')).emit()`

	// instance - import process
	CreateInstance                   = `g.addV('_%s_Instance').property(id, '_%s_Instance').property(single,'header',"%s")`
	CheckInstance                    = `g.V().hasId('_%s_Instance').count()`
	CreateInstanceToCodeRelationship = `g.V().hasId('_%s_Instance').as('i').` +
		`V().hasLabel('_code').has('value',"%s").where(out('usedBy').hasLabel('_code_list').has('listID','%s')).as('c')` +
		`.addE('inDataset').to('i')`
	AddVersionDetailsToInstance = `g.V().hasId('_%s_Instance').property(single,'dataset_id','%s').` +
		`property(single,'edition','%s').property(single,'version','%d')`
	SetInstanceIsPublished = `g.V().hasId('_%s_Instance').property(single,'is_published',true)`
	CountObservations      = `g.V().hasLabel('_%s_observation').count()`

	//instance - parts
	AddInstanceDimensionsPart         = `g.V().hasId('_%s_Instance')`
	AddInstanceDimensionsPropertyPart = `.property('dimensions', "%s")`

	// dimension
	DropDimensionRelationships            = `g.V().hasId('_%s_%s_%s').bothE().drop().iterate();`
	DropDimension                         = `g.V().hasId('_%s_%s_%s').drop().iterate();`
	CreateDimensionToInstanceRelationship = `g.V().hasId('_%s_Instance').as('inst')` +
		`.addV('_%s_%s').as('d').property(id, '_%s_%s_%s').property('value',"%s")` +
		`.addE('HAS_DIMENSION').to('inst').select('d')`

	// observation
	DropObservationRelationships   = `g.V().hasLabel('_%s_observation').has('value', '%s').bothE().drop().iterate();`
	DropObservation                = `g.V().hasLabel('_%s_observation').has('value', '%s').drop().iterate();`
	CreateObservationPart          = `g.addV('_%s_observation').as('o').property(single, 'value', '%s').property(single, 'rowIndex', '%d')`
	AddObservationRelationshipPart = `.V().hasId('%s').addE('isValueOf').from('o')`

	GetInstanceHeaderPart       = `g.V().hasId('_%s_Instance').values('header').aggregate('results')`
	GetAllObservationsPart      = `.V().hasLabel('_%s_observation')`
	GetFirstDimensionPart       = `.V().hasId(%s).in('isValueOf')`
	GetAdditionalDimensionsPart = `.where(out('isValueOf').hasId(%s).fold().count(local).is_(%d))`
	GetObservationValuesPart    = `.values('value').aggregate('results').cap('results').unfold()`
	LimitPart                   = `.limit(%d)`
)
