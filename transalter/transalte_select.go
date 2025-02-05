package transalter

import (
	"errors"
	"fmt"

	"github.com/ollionorg/cassandra-to-spanner-proxy/tableConfig"
)

// func (t *Translator) ToSpannerSelect(originalQuery string) (*SelectQueryMap, error) {
// 	lowerQuery := strings.ToLower(originalQuery)
// 	//Create copy of cassandra query where literals are substituted with a suffix
// 	// query := renameLiterals(originalQuery)
// 	// // special handling for writetime
// 	// query = replaceWriteTimePatterns(query)

// 	p, err := NewCqlParser(query, t.Debug)
// 	if err != nil {
// 		return nil, err
// 	}
// 	selectObj := p.Select_()
// 	if selectObj == nil {
// 		return nil, errors.New("could not parse select object")
// 	}

// 	kwSelectObj := selectObj.KwSelect()
// 	if kwSelectObj == nil {
// 		return nil, errors.New("could not parse select object")
// 	}

// 	if selectObj.KwJson() != nil {
// 		return nil, errors.New(errJSONSupport)
// 	}

// 	if selectObj.DistinctSpec() != nil {
// 		return nil, errors.New(errDistinctSupport)
// 	}

// 	queryType := kwSelectObj.GetText()
// 	columns, err := parseColumnsFromSelect(selectObj.SelectElements())
// 	if err != nil {
// 		return nil, err
// 	}

// 	tableSpec, err := parseTableFromSelect(selectObj.FromSpec())
// 	if err != nil {
// 		return nil, err
// 	} else if tableSpec.TableName == "" || tableSpec.KeyspaceName == "" {
// 		return nil, errors.New("no table or keyspace name found in the query")
// 	}
// 	tableSpec.TableName = utilities.FlattenTableName(t.KeyspaceFlatter, tableSpec.KeyspaceName, tableSpec.TableName)

// 	var clauseResponse ClauseResponse

// 	if hasWhere(lowerQuery) {
// 		resp, err := parseWhereByClause(selectObj.WhereSpec(), tableSpec.TableName, t.TableConfig)
// 		if err != nil {
// 			return nil, err
// 		}
// 		clauseResponse = *resp
// 	}

// 	var orderBy OrderBy
// 	if hasOrderBy(lowerQuery) {
// 		orderBy = parseOrderByFromSelect(selectObj.OrderSpec())
// 	} else {
// 		orderBy.isOrderBy = false
// 	}

// 	var limit Limit
// 	if hasLimit(lowerQuery) {
// 		limit = parseLimitFromSelect(selectObj.LimitSpec())
// 		if limit.Count == questionMark {
// 			clauseResponse.ParamKeys = append(clauseResponse.ParamKeys, limitPlaceholder)
// 		}
// 	} else {
// 		limit.isLimit = false
// 	}

// 	selectQueryData := &SelectQueryMap{
// 		CassandraQuery: query,
// 		SpannerQuery:   "",
// 		QueryType:      queryType,
// 		Table:          tableSpec.TableName,
// 		Keyspace:       tableSpec.KeyspaceName,
// 		ColumnMeta:     columns,
// 		Clauses:        clauseResponse.Clauses,
// 		Limit:          limit,
// 		OrderBy:        orderBy,
// 		Params:         clauseResponse.Params,
// 		ParamKeys:      clauseResponse.ParamKeys,
// 	}

// 	translatedResult, err := getSpannerSelectQuery(t, selectQueryData)
// 	if err != nil {
// 		return nil, err
// 	}

// 	selectQueryData.SpannerQuery = translatedResult
// 	return selectQueryData, nil
// }

type SelectQueryMap struct {
	PartiQLQuery string // Original query string
	SpannerQuery string // Translated query string suitable for Spanner
	QueryType    string // Type of the query (e.g., SELECT)
	Table        string // Table involved in the query
	Keyspace     string // Keyspace to which the table belongs
	//ColumnMeta      ColumnMeta                           // Translator generated Metadata about the columns involved
	//Clauses         []Clause                             // List of clauses in the query
	//	Limit           Limit                                // Limit clause details
	//OrderBy         OrderBy                              // Order by clause details
	Params          map[string]interface{}               // Parameters for the query
	ParamKeys       []string                             // column_name of the parameters
	AliasMap        map[string]tableConfig.AsKeywordMeta // Aliases used in the query
	PrimaryKeys     []string                             // Primary keys of the table
	ColumnsWithInOp []string                             // Columns involved in IN operations
}

func PocSelectQuery(originalQuery string) (*SelectQueryMap, error) {
	// lowerQuery := strings.ToLower(originalQuery)
	selectQueryMap := &SelectQueryMap{}
	p, err := NewPartiQLParser(originalQuery, false)
	if err != nil {
		return selectQueryMap, err
	}
	tree := p.Root()
	p.SelectClause()
	tableName := tree.Statement()
	if tableName == nil {
		return nil, errors.New("could not parse select object")
	}
	tableName1 := p.WhereClauseSelect()
	// if tableName1 == nil {
	// 	return nil, errors.New("could not parse select object")
	// }
	query := tableName
	fmt.Println("originalQuery======>", originalQuery)
	// fmt.Println("lowerQuery======>", lowerQuery)
	fmt.Println("query======>", query.GetText())
	// query1 := tableName1.GetAllText()
	fmt.Println("query1======>", tableName1.GetText())
	// fmt.Println("query1======>", tableName1)
	return selectQueryMap, err
}
