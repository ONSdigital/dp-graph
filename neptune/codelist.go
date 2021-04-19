/*
This module, when combined with codelistdataset.go, provides code that
satisfies the graph.driver.CodeList interface using Gremlin queries into
a Neptune database.
*/

package neptune

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	"github.com/ONSdigital/graphson"
)

// Type check to ensure that NeptuneDB implements the driver.CodeList interface
var _ driver.CodeList = (*NeptuneDB)(nil)

/*
GetCodeLists provides a list of either all Code Lists, or a list of only those
having a boolean property with the name <filterBy> which is set to true. E.g.
"geography": true. The caller is expected to
fully qualify the embedded Links field afterwards. It returns an error if:
- The Gremlin query failed to execute.
- A CodeList is encountered that does not have *listID* property.
*/
func (n *NeptuneDB) GetCodeLists(ctx context.Context, filterBy string) (*models.CodeListResults, error) {
	// Use differing Gremlin queries - depending on if a filterBy string is specified.
	var qry string
	if filterBy == "" {
		qry = fmt.Sprintf(query.GetCodeLists)
	} else {
		qry = fmt.Sprintf(query.GetCodeListsFiltered, filterBy)
	}
	codeListVertices, err := n.getVertices(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}

	results := &models.CodeListResults{}

	for _, codeListVertex := range codeListVertices {
		codeListID, err := codeListVertex.GetProperty("listID")
		if err != nil {
			return nil, errors.Wrapf(err, `Error reading "listID" property on Code List vertex`)
		}

		codeListMdl := models.CodeList{
			ID: codeListID,
		}
		results.Items = append(results.Items, codeListMdl)
	}
	return results, nil
}

// GetCodeList provides a CodeList for a given ID (e.g. "ashe-earnings"),
// having checked it exists
// in the database. Nb. The caller is expected to fully qualify the embedded
// Links field afterwards. It returns an error if:
// - The Gremlin query failed to execute.
// - The requested CodeList does not exist. (error is `ErrNotFound`)
// - Duplicate CodeLists exist with the given ID (error is `ErrMultipleFound`)
func (n *NeptuneDB) GetCodeList(ctx context.Context, codeListID string) (
	*models.CodeList, error) {
	existsQry := fmt.Sprintf(query.CodeListExists, codeListID)
	count, err := n.getNumber(existsQry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", existsQry)
	}
	if count == 0 {
		return nil, driver.ErrNotFound
	}
	if count > 1 {
		return nil, driver.ErrMultipleFound
	}

	return &models.CodeList{
		ID: codeListID,
	}, nil
}

/*
GetEditions provides a models.Editions structure populated based on the
the values in the Code List vertices in the database, that have the provided
codeListId.
It returns an error if:
- The Gremlin query failed to execute. (wrapped error)
- No CodeLists are found of the requested codeListID (error is ErrNotFound')
- A CodeList is found that does not have the "edition" property (error is 'ErrNoSuchProperty')
*/
func (n *NeptuneDB) GetEditions(ctx context.Context, codeListID string) (*models.Editions, error) {
	qry := fmt.Sprintf(query.GetCodeList, codeListID)
	codeLists, err := n.getVertices(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	if len(codeLists) == 0 {
		return nil, driver.ErrNotFound
	}
	editions := &models.Editions{
		Items: []models.Edition{},
	}
	for _, codeList := range codeLists {
		editionString, err := codeList.GetProperty("edition")
		if err != nil {
			return nil, errors.Wrapf(err, `Error reading "edition" property on Code List vertex`)
		}

		edition := models.Edition{
			ID: editionString,
		}
		editions.Items = append(editions.Items, edition)
	}
	return editions, nil
}

/*
GetEdition provides an Edition structure for the code list in the database that
has both the given codeListID (e.g. "ashed-earnings"), and the given edition string
(e.g. "one-off").
Nb. The caller is expected to fully qualify the embedded Links field
afterwards.
It returns an error if:
- The Gremlin query failed to execute. (wrapped error)
- No CodeLists exist with the requested codeListID (error is `ErrNotFound`)
- A CodeList is found that does not have the "edition" property (error is 'ErrNoSuchProperty')
- More than one CodeList exists with the requested ID AND edition (error is `ErrMultipleFound`)
*/
func (n *NeptuneDB) GetEdition(ctx context.Context, codeListID, edition string) (*models.Edition, error) {
	qry := fmt.Sprintf(query.CodeListEditionExists, codeListID, edition)
	nFound, err := n.getNumber(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	if nFound == 0 {
		return nil, driver.ErrNotFound
	}
	if nFound > 1 {
		return nil, driver.ErrMultipleFound
	}
	// What we return (having performed the checks above), is actually hard-coded, as a function of the
	// method parameters.
	return &models.Edition{ID: edition}, nil
}

/*
CountCodes counts the number of codes corresponding to the provided codeListID and edition.
*/
func (n *NeptuneDB) CountCodes(ctx context.Context, codeListID, edition string) (int64, error) {
	qry := fmt.Sprintf(query.CountCodes, codeListID, edition)
	totalCount, err := n.getNumber(qry)
	if err != nil {
		return 0, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	return totalCount, nil
}

/*
GetCodes provides a list of Code(s) packaged into a models.CodeResults structure that has been populated by
a database query that finds the Code List nodes of the required codeListID (e.g. "ashe-earnings"), and the
required edition (e.g.  "one-off"), and then harvests the Code nodes that are known to be "usedBy" that
Code List.  It raises a wrapped error if the database raises a non-transient error, (e.g.  malformed
query).  It raises driver.ErrNotFound if the graph traversal above produces an empty list of codes -
including the case of a short-circuit early termination of the query, because no such qualifying code
list exists. It returns a wrapped error if a Code is found that does not have a "value" property.
*/
func (n *NeptuneDB) GetCodes(ctx context.Context, codeListID, edition string) (*models.CodeResults, error) {

	// Check if order is defined
	qry := fmt.Sprintf(query.CountOrderedEdges, codeListID, edition)
	orderedCount, err := n.getNumber(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	hasOrder := orderedCount > 0

	// query depending on the presence of order in usedBy edges
	if hasOrder {
		qry = fmt.Sprintf(query.GetCodesWithOrder, codeListID, edition)
	} else {
		qry = fmt.Sprintf(query.GetCodesAlphabetically, codeListID, edition)
	}
	values, err := n.getStringList(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	if len(values) == 0 {
		return nil, driver.ErrNotFound
	}

	const valuesPerRecord = 2
	records, err := createRecords(values, valuesPerRecord)
	if err != nil {
		return nil, err
	}

	codes := createCodes(records)

	return codes, nil
}

func createCodes(records [][]string) *models.CodeResults {
	codes := &models.CodeResults{
		Items: []models.Code{},
	}

	for _, record := range records {
		code := models.Code{
			Label: record[0],
			Code:  record[1],
		}

		codes.Items = append(codes.Items, code)
	}

	return codes
}

/*
GetCode provides a Code struct to represent the requested code list, edition and code string.
E.g. ashe-earnings|one-off|hourly-pay-gross.
It doesn't need to access the database to form the response, but does so to validate the
query. Specifically it can return errors as follows:
- The Gremlin query failed to execute.
- The query parameter values do not successfully navigate to a Code node. (error is `ErrNotFound`)
- Duplicate Code(s) exist that satisfy the search criteria (error is `ErrMultipleFound`)
*/
func (n *NeptuneDB) GetCode(ctx context.Context, codeListID, edition string, code string) (*models.Code, error) {
	qry := fmt.Sprintf(query.CodeExists, codeListID, edition, code)
	nFound, err := n.getNumber(qry)
	if err != nil {
		return nil, errors.Wrapf(err, "Gremlin query failed: %q", qry)
	}
	if nFound == 0 {
		return nil, driver.ErrNotFound
	}
	if nFound > 1 {
		return nil, driver.ErrMultipleFound
	}

	// Missing ID and Label fields that exist in Neo4j response
	return &models.Code{
		Code: code,
	}, nil
}

// GetCodesOrder obtains the numerical order value defined in the 'usedBy' edge between the provided codes and codeListID nodes
func (n *NeptuneDB) GetCodesOrder(ctx context.Context, codeListID string, codes []string) (codeOrders map[string]*int, err error) {
	codeOrders = make(map[string]*int)

	// if no codes are provided, nothing needs to be done
	if len(codes) == 0 {
		return codeOrders, nil
	}

	// generate query with list of code node IDs
	codesString := `'` + strings.Join(codes, `','`) + `'`
	qry := fmt.Sprintf(query.GetUsedByEdgesFromNodeIDs, codeListID, codesString)

	// execute query
	res, err := n.exec(qry)
	if err != nil {
		return codeOrders, err
	}

	// responses are batched by gremgo library, hence we need to iterate them
	for _, result := range res {

		// get list of order to edge maps from the response
		orderEdgesMaps, err := graphson.DeserializeListFromBytes(result.Result.Data)
		if err != nil {
			return codeOrders, err
		}

		// each item is a map of {'code': <code>, 'usedBy': <usedBy edge>}, obtain the order from all the code edges
		for _, val := range orderEdgesMaps {
			codeEdgeMap, err := graphson.DeserializeMapFromBytes(val)
			if err != nil {
				return make(map[string]*int), err
			}

			code, order, err := getCodeOrderFromMap(codeEdgeMap)
			if err != nil {
				return make(map[string]*int), err
			}
			codeOrders[code] = order
		}

		// if not all 'usedBy' edges were found, we need to return ErrNotFound
		if len(orderEdgesMaps) < len(codes) {
			return codeOrders, driver.ErrNotFound
		}
	}

	return codeOrders, nil
}

// getCodeOrderFromMap obtains the code and order value from the provided map of {'code': <code>, 'usedBy': <usedBy edge>}
// order will be nil if not defined
func getCodeOrderFromMap(codeEdgeMap map[string]json.RawMessage) (code string, order *int, err error) {
	rawCode, ok := codeEdgeMap["code"]
	if !ok {
		return "", nil, driver.ErrNotFound
	}

	rawEdge, ok := codeEdgeMap["usedBy"]
	if !ok {
		return "", nil, driver.ErrNotFound
	}

	if err := json.Unmarshal(rawCode, &code); err != nil {
		return "", nil, err
	}

	var edge graphson.Edge
	if err := json.Unmarshal(rawEdge, &edge); err != nil {
		return "", nil, err
	}

	if edge.Value.Properties == nil {
		return "", nil, errors.New("unexpected nil Propertie for 'usedBy' edge")
	}

	// find order edge property
	o, ok := edge.Value.Properties["order"]
	if !ok {
		// edge exists without order property. Valid case for codes with no order defined
		return code, nil, nil
	}

	// unmarshal property of type int
	orderProperty, err := graphson.DeserializeInt32(o.Value.Value)
	if err != nil {
		return "", nil, err
	}
	orderPropertyInt := int(orderProperty)
	return code, &orderPropertyInt, nil
}

// convert a flat array of record values into  a 2d array of records
func createRecords(values []string, valuesPerRecord int) ([][]string, error) {
	var records [][]string
	if len(values)%valuesPerRecord != 0 {
		return nil, fmt.Errorf("list length is not divisible by %d", valuesPerRecord)
	}
	for i := 0; i < len(values); i += valuesPerRecord {

		var record []string

		for j := 0; j < valuesPerRecord; j++ {
			value := values[i+j]
			record = append(record, value)
		}

		records = append(records, record)
	}
	return records, nil
}
