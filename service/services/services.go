// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package services implements services for getting data from Spanner
// and streaming data into pubsub
package services

import (
	"context"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/ahmetb/go-linq"
	"github.com/cloudspannerecosystem/dynamodb-adapter/config"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/errors"
	"github.com/cloudspannerecosystem/dynamodb-adapter/pkg/logger"
	"github.com/cloudspannerecosystem/dynamodb-adapter/storage"
	translator "github.com/cloudspannerecosystem/dynamodb-adapter/translator/utils"
	"github.com/cloudspannerecosystem/dynamodb-adapter/utils"
)

// getSpannerProjections makes a projection array of columns
func getSpannerProjections(projectionExpression, table string, expressionAttributeNames map[string]string) []string {
	if projectionExpression == "" {
		return nil
	}
	expressionAttributes := expressionAttributeNames
	projections := strings.Split(projectionExpression, ",")
	projectionCols := []string{}
	for _, pro := range projections {
		pro = strings.TrimSpace(pro)
		if val, ok := expressionAttributes[pro]; ok {
			projectionCols = append(projectionCols, val)
		} else {
			projectionCols = append(projectionCols, pro)
		}
	}

	linq.From(projectionCols).IntersectByT(linq.From(models.TableColumnMap[utils.ChangeTableNameForSpanner(table)]), func(str string) string {
		return str
	}).ToSlice(&projectionCols)
	return projectionCols
}

// Put writes an object to Spanner
func Put(ctx context.Context, tableName string, putObj map[string]interface{}, expr *models.UpdateExpressionCondition, conditionExp string, expressionAttr, oldRes map[string]interface{}) (map[string]interface{}, error) {
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return nil, err
	}

	tableName = tableConf.ActualTable
	e, err := utils.CreateConditionExpression(conditionExp, expressionAttr)
	if err != nil {
		return nil, err
	}
	newResp, err := storage.GetStorageInstance().SpannerPut(ctx, tableName, putObj, e, expr)
	if err != nil {
		return nil, err
	}

	if oldRes == nil {
		return oldRes, nil
	}
	updateResp := map[string]interface{}{}
	for k, v := range oldRes {
		updateResp[k] = v
	}
	for k, v := range newResp {
		updateResp[k] = v
	}

	return updateResp, nil
}

// Add checks the expression for converting the data
func Add(ctx context.Context, tableName string, attrMap map[string]interface{}, condExpression string, m, expressionAttr map[string]interface{}, expr *models.UpdateExpressionCondition, oldRes map[string]interface{}) (map[string]interface{}, error) {
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return nil, err
	}
	tableName = tableConf.ActualTable

	e, err := utils.CreateConditionExpression(condExpression, expressionAttr)
	if err != nil {
		return nil, err
	}

	newResp, err := storage.GetStorageInstance().SpannerAdd(ctx, tableName, m, e, expr)
	if err != nil {
		return nil, err
	}
	if oldRes == nil {
		return newResp, nil
	}
	updateResp := map[string]interface{}{}
	for k, v := range oldRes {
		updateResp[k] = v
	}
	for k, v := range newResp {
		updateResp[k] = v
	}

	return updateResp, nil
}

// Del checks the expression for saving the data
func Del(ctx context.Context, tableName string, attrMap map[string]interface{}, condExpression string, expressionAttr map[string]interface{}, expr *models.UpdateExpressionCondition) (map[string]interface{}, error) {
	logger.LogDebug(expressionAttr)
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return nil, err
	}

	tableName = tableConf.ActualTable

	e, err := utils.CreateConditionExpression(condExpression, expressionAttr)
	if err != nil {
		return nil, err
	}

	err = storage.GetStorageInstance().SpannerDel(ctx, tableName, expressionAttr, e, expr)
	if err != nil {
		return nil, err
	}
	sKey := tableConf.SortKey
	pKey := tableConf.PartitionKey
	res, err := storage.GetStorageInstance().SpannerGet(ctx, tableName, attrMap[pKey], attrMap[sKey], nil)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// BatchGet for batch operation for getting data
func BatchGet(ctx context.Context, tableName string, keyMapArray []map[string]interface{}) ([]map[string]interface{}, error) {
	if len(keyMapArray) == 0 {
		var resp = make([]map[string]interface{}, 0)
		return resp, nil
	}
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return nil, err
	}
	tableName = tableConf.ActualTable

	var pValues []interface{}
	var sValues []interface{}
	for i := 0; i < len(keyMapArray); i++ {
		pValue := keyMapArray[i][tableConf.PartitionKey]
		if tableConf.SortKey != "" {
			sValue := keyMapArray[i][tableConf.SortKey]
			sValues = append(sValues, sValue)
		}
		pValues = append(pValues, pValue)
	}
	return storage.GetStorageInstance().SpannerBatchGet(ctx, tableName, pValues, sValues, nil)
}

// BatchPut writes bulk records to Spanner
func BatchPut(ctx context.Context, tableName string, arrAttrMap []map[string]interface{}) error {
	if len(arrAttrMap) <= 0 {
		return errors.New("ValidationException")
	}

	oldRes, err := BatchGet(ctx, tableName, arrAttrMap)
	if err != nil {
		return err
	}
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return err
	}
	tableName = tableConf.ActualTable
	err = storage.GetStorageInstance().SpannerBatchPut(ctx, tableName, arrAttrMap)
	if err != nil {
		return err
	}
	go func() {
		if len(oldRes) == len(arrAttrMap) {
			for i := 0; i < len(arrAttrMap); i++ {
				go StreamDataToThirdParty(oldRes[i], arrAttrMap[i], tableName)
			}
		} else {
			for i := 0; i < len(arrAttrMap); i++ {
				go StreamDataToThirdParty(nil, arrAttrMap[i], tableName)
			}

		}
	}()
	return nil
}

// GetWithProjection get table data with projection
func GetWithProjection(ctx context.Context, tableName string, primaryKeyMap map[string]interface{}, projectionExpression string, expressionAttributeNames map[string]string) (map[string]interface{}, error) {
	if primaryKeyMap == nil {
		return nil, errors.New("ValidationException")
	}
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return nil, err
	}

	tableName = tableConf.ActualTable

	projectionCols := getSpannerProjections(projectionExpression, tableName, expressionAttributeNames)
	pValue := primaryKeyMap[tableConf.PartitionKey]
	var sValue interface{}
	if tableConf.SortKey != "" {
		sValue = primaryKeyMap[tableConf.SortKey]
	}
	return storage.GetStorageInstance().SpannerGet(ctx, tableName, pValue, sValue, projectionCols)
}

// QueryAttributes from Spanner
func QueryAttributes(ctx context.Context, query models.Query) (map[string]interface{}, string, error) {
	tableConf, err := config.GetTableConf(query.TableName)
	if err != nil {
		return nil, "", err
	}
	var sKey string
	var pKey string
	tPKey := tableConf.PartitionKey
	tSKey := tableConf.SortKey
	if query.IndexName != "" {
		conf := tableConf.Indices[query.IndexName]
		query.IndexName = strings.Replace(query.IndexName, "-", "_", -1)

		if tableConf.ActualTable != query.TableName {
			query.TableName = tableConf.ActualTable
		}

		sKey = conf.SortKey
		pKey = conf.PartitionKey
	} else {
		sKey = tableConf.SortKey
		pKey = tableConf.PartitionKey
	}
	if pKey == "" {
		pKey = tPKey
		sKey = tSKey
	}

	originalLimit := query.Limit
	query.Limit = originalLimit + 1

	stmt, cols, isCountQuery, offset, hash, err := createSpannerQuery(&query, tPKey, pKey, sKey)
	if err != nil {
		return nil, hash, err
	}
	logger.LogDebug(stmt)
	resp, err := storage.GetStorageInstance().ExecuteSpannerQuery(ctx, query.TableName, cols, isCountQuery, stmt)
	if err != nil {
		return nil, hash, err
	}
	if isCountQuery {
		return resp[0], hash, nil
	}
	finalResp := make(map[string]interface{})
	length := len(resp)
	if length == 0 {
		finalResp["Count"] = 0
		finalResp["Items"] = []map[string]interface{}{}
		finalResp["LastEvaluatedKey"] = nil
		return finalResp, hash, nil
	}
	if int64(length) > originalLimit {
		finalResp["Count"] = length - 1
		last := resp[length-2]
		if sKey != "" {
			finalResp["LastEvaluatedKey"] = map[string]interface{}{"offset": originalLimit + offset, pKey: last[pKey], tPKey: last[tPKey], sKey: last[sKey], tSKey: last[tSKey]}
		} else {
			finalResp["LastEvaluatedKey"] = map[string]interface{}{"offset": originalLimit + offset, pKey: last[pKey], tPKey: last[tPKey]}
		}
		finalResp["Items"] = resp[:length-1]
	} else {
		if query.StartFrom != nil && length-1 == 1 {
			finalResp["Items"] = resp
		} else {
			finalResp["Items"] = resp
		}
		finalResp["Count"] = length
		finalResp["Items"] = resp
		finalResp["LastEvaluatedKey"] = nil
	}
	return finalResp, hash, nil
}

func createSpannerQuery(query *models.Query, tPkey, pKey, sKey string) (spanner.Statement, []string, bool, int64, string, error) {
	stmt := spanner.Statement{}
	cols, colstr, isCountQuery, err := parseSpannerColumns(query, tPkey, pKey, sKey)
	if err != nil {
		return stmt, cols, isCountQuery, 0, "", err
	}
	tableName := parseSpannerTableName(query)
	whereCondition, m := parseSpannerCondition(query, pKey, sKey)
	offsetString, offset := parseOffset(query)
	orderBy := parseSpannerSorting(query, isCountQuery, pKey, sKey)
	limitClause := parseLimit(query, isCountQuery)
	finalQuery := "SELECT " + colstr + " FROM " + tableName + " " + whereCondition + orderBy + limitClause + offsetString
	stmt.SQL = finalQuery
	h := fnv.New64a()
	h.Write([]byte(finalQuery))
	val := h.Sum64()
	rs := strconv.FormatUint(val, 10)
	stmt.Params = m
	return stmt, cols, isCountQuery, offset, rs, nil
}

func parseSpannerColumns(query *models.Query, tPkey, pKey, sKey string) ([]string, string, bool, error) {
	if query == nil {
		return []string{}, "", false, errors.New("Query is not present")
	}
	colStr := ""
	if query.OnlyCount {
		return []string{"count"}, "COUNT(" + pKey + ") AS count", true, nil
	}
	table := utils.ChangeTableNameForSpanner(query.TableName)
	var cols []string
	if query.ProjectionExpression != "" {
		cols = getSpannerProjections(query.ProjectionExpression, query.TableName, query.ExpressionAttributeNames)
		insertPKey := true
		for i := 0; i < len(cols); i++ {
			if cols[i] == pKey {
				insertPKey = false
				break
			}
		}
		if insertPKey {
			cols = append(cols, pKey)
		}
		if sKey != "" {
			insertSKey := true
			for i := 0; i < len(cols); i++ {
				if cols[i] == sKey {
					insertSKey = false
					break
				}
			}
			if insertSKey {
				cols = append(cols, sKey)
			}
		}
		if tPkey != pKey {
			insertSKey := true
			for i := 0; i < len(cols); i++ {
				if cols[i] == tPkey {
					insertSKey = false
					break
				}
			}
			if insertSKey {
				cols = append(cols, tPkey)
			}
		}

	} else {
		cols = models.TableColumnMap[table]
	}
	for i := 0; i < len(cols); i++ {
		if cols[i] == "commit_timestamp" {
			continue
		}
		colStr += table + ".`" + cols[i] + "`,"
	}
	colStr = strings.Trim(colStr, ",")
	return cols, colStr, false, nil
}

func parseSpannerTableName(query *models.Query) string {
	tableName := utils.ChangeTableNameForSpanner(query.TableName)
	if query.IndexName != "" {
		tableName += "@{FORCE_INDEX=" + query.IndexName + "}"
	}
	return tableName
}

func parseSpannerCondition(query *models.Query, pKey, sKey string) (string, map[string]interface{}) {
	params := make(map[string]interface{})
	whereClause := "WHERE "

	if sKey != "" {
		whereClause += sKey + " is not null "
	}

	if query.RangeExp != "" {
		whereClause, query.RangeExp = createWhereClause(whereClause, query.RangeExp, "rangeExp", query.RangeValMap, params)
	}

	if query.FilterExp != "" {
		whereClause, query.FilterExp = createWhereClause(whereClause, query.FilterExp, "filterExp", query.RangeValMap, params)
	}

	if whereClause == "WHERE " {
		whereClause = " "
	}
	return whereClause, params
}

func createWhereClause(whereClause string, expression string, queryVar string, RangeValueMap map[string]interface{}, params map[string]interface{}) (string, string) {
	_, _, expression = utils.ParseBeginsWith(expression)
	expression = strings.ReplaceAll(expression, "begins_with", "STARTS_WITH")

	if whereClause != "WHERE " {
		whereClause += " AND "
	}
	count := 1
	for k, v := range RangeValueMap {
		if strings.Contains(expression, k) {
			str := queryVar + strconv.Itoa(count)
			expression = strings.ReplaceAll(expression, k, "@"+str)
			params[str] = v
			count++
		}
	}
	whereClause += expression
	return whereClause, expression
}

func parseOffset(query *models.Query) (string, int64) {
	logger.LogDebug(query)
	if query.StartFrom != nil {
		offset, ok := query.StartFrom["offset"].(float64)
		if ok {
			return " OFFSET " + strconv.FormatInt(int64(offset), 10), int64(offset)
		}
	}
	return "", 0
}

func parseSpannerSorting(query *models.Query, isCountQuery bool, pKey, sKey string) string {
	if isCountQuery {
		return " "
	}
	if sKey == "" {
		return " "
	}

	if query.SortAscending {
		return " ORDER BY " + sKey + " ASC "
	}
	return " ORDER BY " + sKey + " DESC "
}

func parseLimit(query *models.Query, isCountQuery bool) string {
	if isCountQuery {
		return ""
	}
	if query.Limit == 0 {
		return " LIMIT 5000 "
	}
	return " LIMIT " + strconv.FormatInt(query.Limit, 10)
}

// BatchGetWithProjection from Spanner
func BatchGetWithProjection(ctx context.Context, tableName string, keyMapArray []map[string]interface{}, projectionExpression string, expressionAttributeNames map[string]string) ([]map[string]interface{}, error) {
	if len(keyMapArray) == 0 {
		var resp = make([]map[string]interface{}, 0)
		return resp, nil
	}
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return nil, err
	}
	tableName = tableConf.ActualTable

	projectionCols := getSpannerProjections(projectionExpression, tableName, expressionAttributeNames)
	var pValues []interface{}
	var sValues []interface{}
	for i := 0; i < len(keyMapArray); i++ {
		pValue := keyMapArray[i][tableConf.PartitionKey]
		if tableConf.SortKey != "" {
			sValue := keyMapArray[i][tableConf.SortKey]
			sValues = append(sValues, sValue)
		}
		pValues = append(pValues, pValue)
	}
	return storage.GetStorageInstance().SpannerBatchGet(ctx, tableName, pValues, sValues, projectionCols)
}

// Delete service
func Delete(ctx context.Context, tableName string, primaryKeyMap map[string]interface{}, condExpression string, attrMap map[string]interface{}, expr *models.UpdateExpressionCondition) error {
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return err
	}
	tableName = tableConf.ActualTable
	e, err := utils.CreateConditionExpression(condExpression, attrMap)
	if err != nil {
		return err
	}
	return storage.GetStorageInstance().SpannerDelete(ctx, tableName, primaryKeyMap, e, expr)
}

// BatchDelete service
func BatchDelete(ctx context.Context, tableName string, keyMapArray []map[string]interface{}) error {
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return err
	}
	oldRes, _ := BatchGet(ctx, tableName, keyMapArray)

	tableName = tableConf.ActualTable
	err = storage.GetStorageInstance().SpannerBatchDelete(ctx, tableName, keyMapArray)
	if err != nil {
		return err
	}
	go func() {
		if len(oldRes) == len(keyMapArray) {
			for i := 0; i < len(keyMapArray); i++ {
				go StreamDataToThirdParty(oldRes[i], keyMapArray[i], tableName)
			}
		} else {
			for i := 0; i < len(keyMapArray); i++ {
				go StreamDataToThirdParty(nil, keyMapArray[i], tableName)
			}

		}
	}()
	return nil
}

// Scan service
func Scan(ctx context.Context, scanData models.ScanMeta) (map[string]interface{}, error) {
	query := models.Query{}
	query.TableName = scanData.TableName
	query.Limit = scanData.Limit
	if query.Limit == 0 {
		query.Limit = models.GlobalConfig.Spanner.QueryLimit
	}
	query.StartFrom = scanData.StartFrom
	query.RangeValMap = scanData.ExpressionAttributeMap
	query.IndexName = scanData.IndexName
	query.FilterExp = scanData.FilterExpression
	query.ExpressionAttributeNames = scanData.ExpressionAttributeNames
	query.OnlyCount = scanData.OnlyCount
	query.ProjectionExpression = scanData.ProjectionExpression

	for k, v := range query.ExpressionAttributeNames {
		query.FilterExp = strings.ReplaceAll(query.FilterExp, k, v)
	}

	rs, _, err := QueryAttributes(ctx, query)
	return rs, err
}

// Remove for remove operation in update
func Remove(ctx context.Context, tableName string, updateAttr models.UpdateAttr, actionValue string, expr *models.UpdateExpressionCondition, oldRes map[string]interface{}) (map[string]interface{}, error) {
	actionValue = strings.ReplaceAll(actionValue, " ", "")
	colsToRemove := strings.Split(actionValue, ",")
	tableConf, err := config.GetTableConf(tableName)
	if err != nil {
		return nil, err
	}
	tableName = tableConf.ActualTable
	e, err := utils.CreateConditionExpression(updateAttr.ConditionExpression, updateAttr.ExpressionAttributeMap)
	if err != nil {
		return nil, err
	}
	err = storage.GetStorageInstance().SpannerRemove(ctx, tableName, updateAttr.PrimaryKeyMap, e, expr, colsToRemove)
	if err != nil {
		return nil, err
	}
	if oldRes == nil {
		return oldRes, nil
	}
	updateResp := map[string]interface{}{}
	for k, v := range oldRes {
		updateResp[k] = v
	}

	for i := 0; i < len(colsToRemove); i++ {
		delete(updateResp, colsToRemove[i])
	}
	return updateResp, nil
}

// ExecuteStatement service
func ExecuteStatement(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, error) {

	query := strings.TrimSpace(executeStatement.Statement) // Remove any leading or trailing whitespace
	queryUpper := strings.ToUpper(query)

	// Regular expressions to match the beginning of the query
	selectRegex := regexp.MustCompile(`(?i)^\s*SELECT`)
	insertRegex := regexp.MustCompile(`(?i)^\s*INSERT`)
	updateRegex := regexp.MustCompile(`(?i)^\s*UPDATE`)
	deleteRegex := regexp.MustCompile(`(?i)^\s*DELETE`)

	switch {
	case selectRegex.MatchString(queryUpper):
		return ExecuteStatementForSelect(ctx, executeStatement)
	case insertRegex.MatchString(queryUpper):
		return ExecuteStatementForInsert(ctx, executeStatement)
	case updateRegex.MatchString(queryUpper):
		return ExecuteStatementForUpdate(ctx, executeStatement)
	case deleteRegex.MatchString(queryUpper):
		return ExecuteStatementForDelete(ctx, executeStatement)
	default:
		return nil, nil
	}

}

func parsePartiQlToSpannerforSelect(ctx context.Context, executeStatement models.ExecuteStatement) (spanner.Statement, error) {
	stmt := spanner.Statement{}
	var err error

	translatorObj := translator.Translator{}
	queryStmt := executeStatement.Statement

	for i, val := range executeStatement.Parameters {
		if val.S != nil {
			stmt.Params[fmt.Sprintf("val%d", i)] = val.S
			queryStmt = strings.Replace(queryStmt, "?", "@val"+strconv.Itoa(i), 1)
		} else if val.N != nil {
			stmt.Params[fmt.Sprintf("val%d", i)] = val.N
			queryStmt = strings.Replace(queryStmt, "?", "@val"+strconv.Itoa(i), 1)
		}
	}

	queryMap, err := translatorObj.ToSpannerSelect(executeStatement.Statement)
	if err != nil {
		return stmt, err
	}
	stmt.SQL = queryMap.SpannerQuery
	return stmt, nil
}

func ExecuteStatementForSelect(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, error) {
	res, err := parsePartiQlToSpannerforSelect(ctx, executeStatement)
	if err != nil {
		return nil, err

	}
	resp, err := storage.GetStorageInstance().ExecuteSpannerQuery(ctx, executeStatement.TableName, []string{}, false, res)
	if err != nil {
		return nil, err
	}
	finalResp := make(map[string]interface{})
	finalResp["Items"] = resp
	return nil, nil
}

// func convertParamterisedQuery(executeStatement models.ExecuteStatement) (string, error) {
// 	newMap := make(map[string]*dynamodb.AttributeValue)
// 	columnNames, err := extractColumnNames(executeStatement.Statement)
// 	if err != nil {
// 		return "", err
// 	}

// 	if len(columnNames) != len(executeStatement.Parameters) {
// 		return "", fmt.Errorf("invalid params")
// 	}
// 	for i, val := range executeStatement.Parameters {
// 		newMap[columnNames[i]] = val

// 	}
// 	configs, err := config.GetTableConf(executeStatement.TableName)
// 	a, _ := json.Marshal(configs)
// 	fmt.Println("tableConf-->", string(a))
// 	if err != nil {
// 		return "", err
// 	}

// 	finalMapParameters, err := convertTypeValueAttribute(newMap, configs.AttributeTypes, "default")
// 	if err != nil {
// 		return "", err
// 	}
// 	for _, val := range finalMapParameters {
// 		executeStatement.Statement = strings.Replace(executeStatement.Statement, "?", fmt.Sprintf("%v", val), 1)
// 	}
// 	return executeStatement.Statement, err
// }
// func parsePartQlToSpannerInsert(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, error) {
// 	var result map[string]interface{}
// 	resultParam := make(map[string]*dynamodb.AttributeValue)

// 	valueSplitString := strings.Split(executeStatement.Statement, "VALUE")
// 	if len(valueSplitString) < 2 {
// 		return result, fmt.Errorf("VALUES part missing for insert")
// 	}

// 	// Extract the part after "VALUE"
// 	rawJSON := strings.TrimSpace(valueSplitString[1])
// 	// Replace single quotes with double quotes to make it valid JSON
// 	normalizeJSON := strings.ReplaceAll(rawJSON, `'`, `"`)
// 	normalizeJSON = strings.ReplaceAll(normalizeJSON, `?`, `"?"`)

// 	// Parse JSON string into a map
// 	if err := json.Unmarshal([]byte(normalizeJSON), &result); err != nil {
// 		return nil, fmt.Errorf("error parsing JSON: %v", err)
// 	}
// 	if executeStatement.Parameters != nil && strings.Contains(rawJSON, "?") {
// 		// Parse JSON string into a map
// 		if err := json.Unmarshal([]byte(normalizeJSON), &result); err != nil {
// 			return nil, fmt.Errorf("error parsing JSON: %v", err)
// 		}

// 		i := 0
// 		for key, _ := range result {
// 			resultParam[key] = executeStatement.Parameters[i]
// 			i++

// 		}
// 		configs, err := config.GetTableConf(executeStatement.TableName)
// 		a, _ := json.Marshal(configs)
// 		logger.LogInfo("tableConf-->", string(a))
// 		if err != nil {
// 			return nil, err
// 		}

// 		result, err = convertTypeValueAttribute(resultParam, configs.AttributeTypes, "insert")
// 		if err != nil {
// 			return nil, err
// 		}
// 		colDLL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(executeStatement.TableName)]
// 		if !ok {
// 			return nil, errors.New("ResourceNotFoundException", executeStatement.TableName)
// 		}
// 		result, err = convertType(result, colDLL)
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	return result, nil
// }

func parsePartQlToSpannerUpdate(ctx context.Context, executeStmnt models.ExecuteStatement) (map[string]interface{}, map[string]interface{}, error) {
	return nil, nil, nil
}

// func parsePartQlToSpannerUpdate(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, map[string]interface{}, error) {
// 	primaryKeyMap := make(map[string]interface{})
// 	setValueMap := make(map[string]interface{})

// 	setStatemenWithtAndWhereClause := strings.Split(executeStatement.Statement, "SET")
// 	if len(setStatemenWithtAndWhereClause) < 2 {
// 		return primaryKeyMap, setValueMap, fmt.Errorf("SET part missing for update")
// 	}

// 	setAndWhereClause := strings.Split(strings.ReplaceAll(setStatemenWithtAndWhereClause[1], `'`, ""), "WHERE")
// 	if len(setAndWhereClause) < 2 {
// 		return primaryKeyMap, setValueMap, fmt.Errorf("WHERE part missing for update")
// 	}

// 	setValueMapAttr := make(map[string]*dynamodb.AttributeValue)
// 	// Extract the part after "SET Clause"
// 	setClauseString := strings.TrimSpace(setAndWhereClause[0])
// 	extractKeyValue := strings.Split(setClauseString, ",")
// 	i := 0
// 	paramLen := len(executeStatement.Parameters)
// 	configAttr, err := config.GetTableConf(executeStatement.TableName)
// 	if err != nil {
// 		return nil, setValueMap, err
// 	}
// 	for _, value := range extractKeyValue {
// 		extractKeyValueArray := strings.Split(value, "=")
// 		setValueMap[strings.TrimSpace(extractKeyValueArray[0])] = strings.TrimSpace(extractKeyValueArray[1])
// 		if paramLen > 0 && strings.Contains(extractKeyValueArray[1], "?") {

// 			setValueMapAttr[strings.TrimSpace(extractKeyValueArray[0])] = executeStatement.Parameters[i]
// 			setValueMap, err = convertTypeValueAttribute(setValueMapAttr, configAttr.AttributeTypes, "update")
// 			if err != nil {
// 				return nil, setValueMap, err
// 			}
// 		}
// 		i++
// 	}

// 	// Extract the part after "WHERE Clause"
// 	whereClauseString := strings.TrimSpace(setAndWhereClause[1])

// 	// Replace single quotes with double quotes to make it valid String
// 	KeyValueSplit := strings.Split(whereClauseString, "=")
// 	if len(KeyValueSplit) != 2 {
// 		return primaryKeyMap, setValueMap, fmt.Errorf("invalid where clause")
// 	}

// 	primaryKeyMapMapAttr := make(map[string]*dynamodb.AttributeValue)
// 	primaryKeyMap[strings.TrimSpace(KeyValueSplit[0])] = strings.ReplaceAll(strings.TrimSpace(KeyValueSplit[1]), `"`, "")
// 	if paramLen > 0 && strings.Contains(KeyValueSplit[1], "?") {
// 		primaryKeyMapMapAttr[strings.TrimSpace(KeyValueSplit[0])] = executeStatement.Parameters[i]
// 		primaryKeyMap, err = convertTypeValueAttribute(primaryKeyMapMapAttr, configAttr.AttributeTypes, "update")
// 		if err != nil {
// 			return primaryKeyMap, setValueMap, err
// 		}
// 	}

// 	colDLL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(executeStatement.TableName)]
// 	if !ok {
// 		return primaryKeyMap, setValueMap, errors.New("ResourceNotFoundException", executeStatement.TableName)
// 	}
// 	primaryKeyMap, err = convertType(primaryKeyMap, colDLL)
// 	if err != nil {
// 		return nil, setValueMap, err
// 	}

// 	setValueMap, err = convertType(setValueMap, colDLL)
// 	if err != nil {
// 		return primaryKeyMap, setValueMap, err
// 	}

// 	return primaryKeyMap, setValueMap, nil
// }

// func parsePartQlToSpannerDelete(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, error) {
// 	primaryKeyMap := make(map[string]interface{})

// 	whereSplitString := strings.Split(executeStatement.Statement, "WHERE")
// 	if len(whereSplitString) < 2 {
// 		return primaryKeyMap, fmt.Errorf("WHERE part missing for insert")
// 	}

// 	// Extract the part after "VALUE"
// 	rawJSON := strings.TrimSpace(whereSplitString[1])

// 	// Replace single quotes with double quotes to make it valid String
// 	KeyValueSplit := strings.Split(rawJSON, "=")
// 	if len(whereSplitString) != 2 {
// 		return primaryKeyMap, fmt.Errorf("invalid where clause")
// 	}

// 	paramLen := len(executeStatement.Parameters)
// 	configAttr, err := config.GetTableConf(executeStatement.TableName)
// 	if err != nil {
// 		return primaryKeyMap, err
// 	}
// 	primaryKeyMapMapAttr := make(map[string]*dynamodb.AttributeValue)
// 	primaryKeyMap[strings.TrimSpace(KeyValueSplit[0])] = strings.ReplaceAll(strings.TrimSpace(KeyValueSplit[1]), `"`, "")
// 	if paramLen > 0 && strings.Contains(KeyValueSplit[1], "?") {
// 		primaryKeyMapMapAttr[strings.TrimSpace(KeyValueSplit[0])] = executeStatement.Parameters[0]
// 		primaryKeyMap, err = convertTypeValueAttribute(primaryKeyMapMapAttr, configAttr.AttributeTypes, "delete")
// 		if err != nil {
// 			return primaryKeyMap, err
// 		}
// 	}
// 	colDLL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(executeStatement.TableName)]
// 	if !ok {
// 		return nil, errors.New("ResourceNotFoundException", executeStatement.TableName)
// 	}
// 	primaryKeyMap, _ = convertType(primaryKeyMap, colDLL)
// 	return primaryKeyMap, nil
// }

func convertType(columnName string, val interface{}, columntype string) (interface{}, error) {
	switch columntype {
	case "STRING(MAX)":
		// Ensure the value is a string
		return fmt.Sprintf("%v", val), nil

	case "INT64":
		// Convert to int64
		intValue, err := strconv.ParseInt(fmt.Sprintf("%v", val), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting to int64: %v", err)
		}
		return intValue, nil

	case "FLOAT64":
		// Convert to float64
		floatValue, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
		if err != nil {
			return nil, fmt.Errorf("error converting to float64: %v", err)
		}
		return floatValue, nil

	case "NUMERIC":
		// Treat same as FLOAT64 here or use a specific library for decimal types
		numValue, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
		if err != nil {
			return nil, fmt.Errorf("error converting to numeric: %v", err)
		}
		return numValue, nil

	case "BOOL":
		// Convert to boolean
		boolValue, err := strconv.ParseBool(fmt.Sprintf("%v", val))
		if err != nil {
			return nil, fmt.Errorf("error converting to bool: %v", err)
		}
		return boolValue, nil

	default:
		return nil, fmt.Errorf("unsupported data type: %s", columntype)
	}

}

// func convertType(pkeyMap map[string]interface{}, colDLL map[string]string) (map[string]interface{}, error) {
// 	for k, val := range pkeyMap {
// 		v, ok := colDLL[k]
// 		if !ok {
// 			return nil, fmt.Errorf("ResourceNotFoundException: %s", k)
// 		}

// 		switch v {
// 		case "STRING(MAX)":
// 			// Ensure the value is a string
// 			pkeyMap[k] = fmt.Sprintf("%v", val)

// 		case "INT64":
// 			// Convert to int64
// 			intValue, err := strconv.ParseInt(fmt.Sprintf("%v", val), 10, 64)
// 			if err != nil {
// 				return nil, fmt.Errorf("error converting to int64: %v", err)
// 			}
// 			pkeyMap[k] = intValue

// 		case "FLOAT64":
// 			// Convert to float64
// 			floatValue, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
// 			if err != nil {
// 				return nil, fmt.Errorf("error converting to float64: %v", err)
// 			}
// 			pkeyMap[k] = floatValue

// 		case "NUMERIC":
// 			// Treat same as FLOAT64 here or use a specific library for decimal types
// 			numValue, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
// 			if err != nil {
// 				return nil, fmt.Errorf("error converting to numeric: %v", err)
// 			}
// 			pkeyMap[k] = numValue

// 		case "BOOL":
// 			// Convert to boolean
// 			boolValue, err := strconv.ParseBool(fmt.Sprintf("%v", val))
// 			if err != nil {
// 				return nil, fmt.Errorf("error converting to bool: %v", err)
// 			}
// 			pkeyMap[k] = boolValue

// 		default:
// 			return nil, fmt.Errorf("unsupported data type: %s", v)
// 		}
// 	}
// 	return pkeyMap, nil
// }

// // contains function checks if a slice contains a specific string
// func contains(slice []string, item string) bool {
// 	for _, v := range slice {
// 		if v == item {
// 			return true
// 		}
// 	}
// 	return false
// }

// func convertTypeValueAttribute(m map[string]*dynamodb.AttributeValue, attrMap map[string]string, queryType string) (map[string]interface{}, error) {
// 	newMap := make(map[string]interface{})
// 	for k, val := range m {
// 		v, ok := attrMap[k]
// 		if !ok {
// 			return nil, fmt.Errorf("ResourceNotFoundException: %s", k)
// 		}
// 		supportedQueries := []string{"insert", "update", "delete"}
// 		switch v {
// 		case "S":
// 			newMap[k] = fmt.Sprintf(`"%v"`, *val.S)

//				// Check if queryType is one of the supported types
//				if contains(supportedQueries, queryType) {
//					newMap[k] = *val.S // Assign value to the map
//				}
//			case "N":
//				newMap[k] = *val.N
//			case "BOOL":
//				newMap[k] = *val.BOOL
//			case "NULL":
//				newMap[k] = *val.NULL
//			case "B":
//				newMap[k] = &val.B
//			case "L":
//				newMap[k] = &val.L
//			default:
//				return nil, fmt.Errorf("unsupported data type: %s", v)
//			}
//		}
//		return newMap, nil
//	}

func ExecuteStatementForInsert(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, error) {
	// res, err := parsePartQlToSpannerInsert(ctx, executeStatement)
	// if err != nil {
	// 	return nil, err
	// }
	newMap := make(map[string]interface{})
	translatorObj := translator.Translator{}
	parsedQueryObj, err := translatorObj.ToSpannerInsert(executeStatement.Statement)
	if err != nil {
		return nil, err
	}
	colDLL, ok := models.TableDDL[utils.ChangeTableNameForSpanner(executeStatement.TableName)]
	if !ok {
		return nil, errors.New("ResourceNotFoundException", executeStatement.TableName)
	}
	for i, col := range parsedQueryObj.Columns {
		columntype := colDLL[col]
		newMap[col], err = convertType(col, parsedQueryObj.Values[i], columntype)
		return nil, err

	}
	result, err := Put(ctx, executeStatement.TableName, newMap, nil, "", nil, nil)
	if err != nil {
		return result, err
	}
	return result, nil
}

func ExecuteStatementForUpdate(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, error) {
	// primaryKeyMap, setValueMap, err := parsePartQlToSpannerUpdate(ctx, executeStatement)
	// if err != nil {
	// 	return nil, err
	// }
	translatorObj := translator.Translator{}
	parsedQueryObj, err := translatorObj.ToSpannerUpdate(executeStatement.Statement)
	if err != nil {
		return nil, err
	}
	res, err := storage.GetStorageInstance().InsertUpdateOrDeleteStatement(ctx, parsedQueryObj)
	if err != nil {
		return res, err
	}
	// fetch old row to modify the column value
	// oldRes, err := GetWithProjection(ctx, executeStatement.TableName, primaryKeyMap, "", nil)
	// if err != nil {
	// 	return nil, err
	// }
	// for key, val := range setValueMap {
	// 	oldRes[key] = val
	// }

	// _, err = Put(ctx, executeStatement.TableName, oldRes, nil, "", nil, nil)
	// if err != nil {
	// 	return nil, err
	// }
	return nil, err
}

func ExecuteStatementForDelete(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, error) {

	translatorObj := translator.Translator{}
	parsedQueryObj, err := translatorObj.ToSpannerDelete(executeStatement.Statement)
	if err != nil {
		return nil, err
	}

	res, err := storage.GetStorageInstance().InsertUpdateOrDeleteStatement(ctx, parsedQueryObj)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// func ExecuteStatementForDelete(ctx context.Context, executeStatement models.ExecuteStatement) (map[string]interface{}, error) {
// 	primaryKeyMap, err := parsePartQlToSpannerDelete(ctx, executeStatement)
// 	if err != nil {
// 		return nil, err
// 	}
// 	err = Delete(ctx, executeStatement.TableName, primaryKeyMap, "", nil, nil)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return nil, err
// }

// // QueryType defines the type of SQL query
// type QueryType string

// const (
// 	SelectType  QueryType = "SELECT"
// 	InsertType  QueryType = "INSERT"
// 	UpdateType  QueryType = "UPDATE"
// 	DeletTypee  QueryType = "DELETE"
// 	UnknownType QueryType = "UNKNOWN"
// )

// // splitAndAppendOperators function takes a query string, splits it by logical operators,
// // and appends any comparison operators found in each part.
// func splitAndAppendOperators(query string) ([]string, error) {
// 	// Define regex patterns for logical and comparison operators
// 	logicalOperatorPattern := `\s*(AND|OR)\s*`
// 	comparisonOperatorPattern := `(?<![<>=!])\s*(=|<>|!=|>=|<=|>|<)\s*`

// 	// Split by logical operators
// 	logicalParts := regexp.MustCompile(logicalOperatorPattern).Split(query, -1)

// 	// Result slices
// 	var result []string

// 	for _, part := range logicalParts {
// 		part = strings.TrimSpace(part) // Trim whitespace

// 		// Find comparison operators within the current part
// 		comparisonOperators := regexp.MustCompile(comparisonOperatorPattern).FindAllString(part, -1)

// 		// If there are any comparison operators, append them to the part
// 		if len(comparisonOperators) > 0 {
// 			for _, operator := range comparisonOperators {
// 				part = strings.Replace(part, operator, operator+" ", 1) // Append space
// 			}
// 		}

// 		result = append(result, part)
// 	}

// 	return result, nil
// }

// // extractColumnNames function takes a query string and extracts column names while removing spaces.
// func extractColumnNames(query string) ([]string, error) {
// 	// Define regex to capture column names from the query
// 	// This regex matches anything before a comparison operator in a condition
// 	// It assumes that the column name will always be left of an operator
// 	columnPattern := `(\w+)\s*(=|<>|!=|>=|<=|>|<)`

// 	// Find all matches for column names in the query
// 	matches := regexp.MustCompile(columnPattern).FindAllStringSubmatch(query, -1)

// 	// Result slice to hold unique column names
// 	uniqueColumns := make(map[string]struct{})
// 	for _, match := range matches {
// 		if len(match) > 1 {
// 			columnName := strings.TrimSpace(match[1]) // Get column name and trim spaces
// 			uniqueColumns[columnName] = struct{}{}    // Use a map to ensure uniqueness
// 		}
// 	}

// 	// Convert keys of the map to a slice
// 	var columnNames []string
// 	for column := range uniqueColumns {
// 		columnNames = append(columnNames, column)
// 	}

// 	return columnNames, nil
// }
