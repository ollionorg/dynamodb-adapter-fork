package translator

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/PartiQLParser/parser"
)

func (l *SelectQueryListener) EnterExprAnd(ctx *parser.ExprAndContext) {
	fmt.Println("Entering AND Logical Context")
	l.CurrentLogic = "AND"
	l.LogicStack = append(l.LogicStack, LogicalGroup{Operator: "AND"})
}

func (l *SelectQueryListener) EnterExprOr(ctx *parser.ExprOrContext) {
	fmt.Println("Entering OR Logical Context")
	l.CurrentLogic = "OR"
	l.LogicStack = append(l.LogicStack, LogicalGroup{Operator: "OR"})
}

func (l *SelectQueryListener) EnterProjectionItems(ctx *parser.ProjectionItemsContext) {
	for _, proj := range ctx.AllProjectionItem() {
		l.Columns = append(l.Columns, proj.GetText())
	}
}

func (l *SelectQueryListener) EnterFromClause(ctx *parser.FromClauseContext) {
	l.Tables = append(l.Tables, ctx.TableReference().GetText())
}

func (l *SelectQueryListener) EnterOrderByClause(ctx *parser.OrderByClauseContext) {
	for _, orderSpec := range ctx.AllOrderSortSpec() {
		l.OrderBy = append(l.OrderBy, orderSpec.GetText())
	}
}

func (l *SelectQueryListener) EnterLimitClause(ctx *parser.LimitClauseContext) {
	l.Limit = ctx.GetText()
}

func (l *SelectQueryListener) EnterOffsetByClause(ctx *parser.OffsetByClauseContext) {
	l.Offset = ctx.GetText()
}

// Extracts WHERE conditions for SELECT
func (l *SelectQueryListener) EnterPredicateComparison(ctx *parser.PredicateComparisonContext) {
	fmt.Println("EnterPredicateComparison", ctx.GetText())
	column := ctx.GetLhs().GetText()
	operator := ctx.GetOp().GetText()
	value := ctx.GetRhs().GetText()

	condition := Condition{
		Column:   strings.ReplaceAll(column, `'`, ""),
		Operator: operator,
		Value:    strings.ReplaceAll(value, `'`, `"`),
	}

	if len(l.LogicStack) > 0 {
		lastGroup := &l.LogicStack[len(l.LogicStack)-1]
		lastGroup.Conditions = append(lastGroup.Conditions, condition)
	} else {
		// Avoid adding logic operators among the base conditions themselves
		l.Where = append(l.Where, condition)
	}

	fmt.Printf("Added condition: %s %s %s under logic: %s\n",
		column, operator, value, l.CurrentLogic)
}

func (l *SelectQueryListener) ExitExprAnd(ctx *parser.ExprAndContext) {
	fmt.Println("ExitExprAnd-->", ctx.GetText())
	if len(l.LogicStack) > 0 {
		lastGroup := l.LogicStack[len(l.LogicStack)-1]
		l.LogicStack = l.LogicStack[:len(l.LogicStack)-1] // Pop from stack
		for i, cond := range lastGroup.Conditions {
			// Ensure all conditions except the first get AND
			if i > 0 {
				cond.ANDOpr = "AND"
			}
			l.Where = append(l.Where, cond)
		}
	}
}

func (l *SelectQueryListener) ExitExprOr(ctx *parser.ExprOrContext) {
	fmt.Println("ExitExprOr-->", ctx.GetText())
	if len(l.LogicStack) > 0 {
		lastGroup := l.LogicStack[len(l.LogicStack)-1]
		l.LogicStack = l.LogicStack[:len(l.LogicStack)-1] // Pop from stack
		for i, cond := range lastGroup.Conditions {
			// Ensure all conditions except the first get OR
			if i > 0 {
				cond.OROpr = "OR"
			}
			l.Where = append(l.Where, cond)
		}
	}
}

func (t *Translator) ToSpannerSelect(query string) (*SelectQueryMap, error) {
	var err error
	var whereConditions []Condition // Local variable to store WHERE conditions temporarily

	// Lexer and parser setup
	lexer := parser.NewPartiQLLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewPartiQLParser(stream)

	selectListener := &SelectQueryListener{}
	antlr.ParseTreeWalkerDefault.Walk(selectListener, p.Root())

	// Capture WHERE conditions
	whereConditions = append(whereConditions, selectListener.Where...)

	// Build the SelectQueryMap
	selectQueryMap := &SelectQueryMap{
		PartiQLQuery:      query,
		SpannerQuery:      "",       // TODO: Assign translated Spanner SQL
		QueryType:         "SELECT", // Assuming SELECT by context
		Table:             selectListener.Tables[0],
		ParamKeys:         []string{}, // Populate if params are used
		ProjectionColumns: selectListener.Columns,
		Limit:             selectListener.Limit,
		OrderBy:           selectListener.OrderBy,
		Offset:            selectListener.Offset,
	}
	// Generate Spanner query string
	selectQueryMap.SpannerQuery, err = formSpannerSelectQuery(selectQueryMap, whereConditions)
	if err != nil {
		return nil, err
	}

	return selectQueryMap, nil
}

func formSpannerSelectQuery(selectQueryMap *SelectQueryMap, whereConditions []Condition) (string, error) {
	spannerQuery := "SELECT "

	// Construct projection columns or use * if empty
	if len(selectQueryMap.ProjectionColumns) == 0 {
		spannerQuery += "* "
	} else {
		spannerQuery += strings.Join(selectQueryMap.ProjectionColumns, ", ") + " "
	}

	spannerQuery += "FROM " + selectQueryMap.Table

	// Construct WHERE clause
	if len(whereConditions) > 0 {
		var whereClauses []string
		for i, cond := range whereConditions {
			clause := fmt.Sprintf("%s %s %s", cond.Column, cond.Operator, cond.Value)

			// Add logical operators if it's not the first condition
			if i > 0 {
				if cond.ANDOpr != "" {
					clause = cond.ANDOpr + " " + clause
				} else if cond.OROpr != "" {
					clause = cond.OROpr + " " + clause
				}
			}
			whereClauses = append(whereClauses, clause)
		}
		// Join the WHERE clauses using the appropriate spacing
		spannerQuery += " WHERE " + strings.Join(whereClauses, " ")
	}

	// Append ORDER BY clause if present
	if len(selectQueryMap.OrderBy) > 0 {
		spannerQuery += " ORDER BY " + strings.Join(selectQueryMap.OrderBy, ", ")
	}

	// Append LIMIT clause if present
	if selectQueryMap.Limit != "" {
		spannerQuery += " LIMIT " + strings.Trim(selectQueryMap.Limit, "LIMIT")
	}

	// Append OFFSET clause if present
	if selectQueryMap.Offset != "" {
		spannerQuery += " OFFSET " + strings.Trim(selectQueryMap.Offset, "OFFSET")
	}

	return spannerQuery, nil
}

// func convertType(tablename string, col string, val string) (string, interface{}) {
// 	colDLL, ok := models.TableDDL[tablename]
// 	if !ok {
// 		return "", ""
// 	}
// 	v, ok := colDLL[col]
// 	if !ok {
// 		return "", ""
// 	}
// 	switch v {
// 	case "STRING(MAX)":
// 		return col, fmt.Sprintf("%v", val)

// 	case "INT64":
// 		// Convert to int64
// 		intValue, err := strconv.ParseInt(fmt.Sprintf("%v", val), 10, 64)
// 		if err != nil {
// 			return nil, fmt.Errorf("error converting to int64: %v", err)
// 		}
// 		pkeyMap[k] = intValue

// 	case "FLOAT64":
// 		// Convert to float64
// 		floatValue, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
// 		if err != nil {
// 			return nil, fmt.Errorf("error converting to float64: %v", err)
// 		}
// 		pkeyMap[k] = floatValue

// 	case "NUMERIC":
// 		// Treat same as FLOAT64 here or use a specific library for decimal types
// 		numValue, err := strconv.ParseFloat(fmt.Sprintf("%v", val), 64)
// 		if err != nil {
// 			return nil, fmt.Errorf("error converting to numeric: %v", err)
// 		}
// 		pkeyMap[k] = numValue

// 	case "BOOL":
// 		// Convert to boolean
// 		boolValue, err := strconv.ParseBool(fmt.Sprintf("%v", val))
// 		if err != nil {
// 			return nil, fmt.Errorf("error converting to bool: %v", err)
// 		}
// 		pkeyMap[k] = boolValue

// 	default:
// 		return nil, fmt.Errorf("unsupported data type: %s", v)
// 	}

// 	return "", ""
// }
