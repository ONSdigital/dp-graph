/*
This module, when combined with codelistdataset.go, provides code that
satisfies the graph.driver.CodeList interface using Gremlin queries into
a Neptune database.
*/

package neptune

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/ONSdigital/dp-graph/v2/graph/driver"
	"github.com/ONSdigital/dp-graph/v2/models"
	"github.com/ONSdigital/dp-graph/v2/neptune/query"
	"github.com/ONSdigital/graphson"
	"github.com/ONSdigital/log.go/log"
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

// GremlinMap represents a map
type GremlinMap struct {
	Value []json.RawMessage `json:"@value"`
	// Value []interface{} `json:"@value"`
	Type string `json:"@type"`
}

type GMap struct {
	Type  string            `json:"@type"`
	Value []json.RawMessage `json:"@value"`
}

// type GremlinOrderResponse struct {
// 	Type  string        `json:"@type"`
// 	Value []interface{} `json:"@value"`
// }

// type GremlinMMM struct {
// Data []interface{} `json:"@value"`
// Type string        `json:"@type"`
// }

type GremlinString struct {
	Type  string `json:"@type"`
	Value string `json:"@value"`
}

// GetCodesOrder obtains the numerical order value defined in the 'usedBy' edge between the provided codes and codeListID nodes
func (n *NeptuneDB) GetCodesOrder(ctx context.Context, codeListID string, codes []string) (codeOrders map[string]*int, err error) {

	if len(codes) == 0 {
		return make(map[string]*int), nil
	}

	codeNodeIDs := make([]string, len(codes))
	for i, code := range codes {
		codeNodeIDs[i] = fmt.Sprintf("_code_%s_%s", codeListID, code)
	}

	codesString := `'` + strings.Join(codeNodeIDs, `','`) + `'`

	// codes := `'_code_mmm_mar','_code_mmm_apr','_code_mmm_jun'`
	qry := fmt.Sprintf(query.GetUsedByEdges, codeListID, codesString)
	fmt.Sprintf("query: %s", qry)

	// res, err := n.getStringList(qry)
	res, err := n.exec(qry)
	if err != nil {
		return make(map[string]*int), err
	}

	if len(res) == 0 || res[0].Status.Code != 200 {
		return make(map[string]*int), errors.New("failed to run")
	}

	results := res[0].Result.Data

	// resMap := map[string]interface{}{}
	resMap := GremlinMap{}

	err = json.Unmarshal(results, &resMap)
	if err != nil {
		return make(map[string]*int), err
	}

	// codeOrders = make(map[string]*int)
	usedByEdgesByCode := map[string]graphson.Edge{}
	for _, val := range resMap.Value {
		rrr, err := DeserializeMapFromBytes(val)
		if err != nil {
			return make(map[string]*int), err
		}
		log.Event(ctx, "result", log.Data{"rrr": rrr})

		for k, v := range rrr {
			usedByEdgesByCode[k] = v
		}
	}

	log.Event(ctx, "result", log.Data{"res": res})

	codeOrders = make(map[string]*int, len(usedByEdgesByCode))
	for k, v := range usedByEdgesByCode {

		// get order property from edge
		o, ok := v.Value.Properties["order"]
		if !ok {
			// valid edge, with no order defined (valid case)
			codeOrders[k] = nil
			continue
		}

		// unmarshal property of type int
		var orderProperty PropertyValueInt
		if err = json.Unmarshal(o.Value.Value, &orderProperty); err != nil {
			return nil, err
		}

		codeOrders[k] = &orderProperty.Value
	}

	return codeOrders, nil
}

func isEmptyResponse(rawResponse []byte) bool {
	return len(rawResponse) == 0 || isNullResponse(rawResponse)
}

func isNullResponse(rawResponse []byte) bool {
	return len(rawResponse) == 4 && string(rawResponse) == "null"
}

// DeserializeMapFromBytes TODO move this method to graphson, after generalising
func DeserializeMapFromBytes(rawResponse []byte) (edgesMap map[string]graphson.Edge, err error) {

	edgesMap = make(map[string]graphson.Edge)

	if isEmptyResponse(rawResponse) {
		return edgesMap, nil
	}

	// var metaResponse graphson.GList
	var metaResponse GMap

	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&metaResponse); err != nil {
		return nil, err
	}

	if metaResponse.Type != "g:Map" {
		return edgesMap, fmt.Errorf("DeserializeMapFromBytes: Expected `g:Map` type, but got %q", metaResponse.Type)
	}

	// populate map
	if len(metaResponse.Value) != 4 {
		return edgesMap, errors.New("wrong respose size for edges map")
	}

	var k0 string
	if err := json.Unmarshal(metaResponse.Value[0], &k0); err != nil {
		return edgesMap, err
	}

	var k2 string
	if err := json.Unmarshal(metaResponse.Value[2], &k2); err != nil {
		return edgesMap, err
	}

	var code string
	if k0 == "code" {
		if err := json.Unmarshal(metaResponse.Value[1], &code); err != nil {
			return edgesMap, err
		}
	} else if k2 == "code" {
		if err := json.Unmarshal(metaResponse.Value[3], &code); err != nil {
			return edgesMap, err
		}
	} else {
		return edgesMap, errors.New("code key not found in response")
	}

	var edge graphson.Edge
	if k0 == "usedBy" {
		if err := json.Unmarshal(metaResponse.Value[1], &edge); err != nil {
			return edgesMap, err
		}
	} else if k2 == "usedBy" {
		if err := json.Unmarshal(metaResponse.Value[3], &edge); err != nil {
			return edgesMap, err
		}
	}

	edgesMap[code] = edge

	// for i, val := range metaResponse.Value {
	// 	if i%2 == 0 {
	// 		switch val.(type) {
	// 		case string:
	// 			lastKey = val.(string)
	// 		default:
	// 			return edgesMap, errors.New("Wrong type for key")
	// 		}
	// 	} else {
	// 		if lastKey == "" {
	// 			return edgesMap, errors.New("Empty key")
	// 		}
	// 		edgesMap[lastKey] = nil
	// 		lastKey = ""
	// 	}
	// }

	// rrr := GMap{}

	// if err = json.Unmarshal(metaResponse.Value, &metaResponse); err != nil {
	// return resMap, err
	// }

	return edgesMap, nil
}

// func getEdgeArray(val json.RawMessage) (res []graphson.Edge, err error) {
// 	for _, item := range resp {
// 		var resN []graphson.Edge
// 		if resN, err = graphson.DeserializeListOfEdgesFromBytes(item.Result.Data); err != nil {
// 			return
// 		}
// 		res = append(res, resN...)
// 	}

// }

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
