package main

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/transalter/parser"
)

type Condition struct {
	Column   string
	Operator string
	Value    string
	ANDOpr   string
	OROpr    string
	LogicOp  string
}

// Listener for SELECT queries.
type SelectQueryListener struct {
	*parser.BasePartiQLParserListener
	Columns []string
	Tables  []string
	Where   []Condition
	OrderBy []string
	Limit   string
	Offset  string
}

type InsertStatement struct {
	Table         string
	Columns       []string
	Values        []string
	OnConflict    string
	AdditionalMap map[string]interface{} //
}

// Listener for INSERT queries.
type InsertQueryListener struct {
	*parser.BasePartiQLParserListener
	InsertData InsertStatement
}

type UpdateStatement struct {
	Table      string
	SetColumns []string
	Where      []Condition
	Returning  []string
}

type SetClause struct {
	Column   string
	Operator string
	Value    string
}

// Listener for UPDATE queries.
type UpdateQueryListener struct {
	*parser.BasePartiQLParserListener
	Table      string
	SetColumns []string // Storage for column-specific updates.
	Where      []Condition
	SetClauses []SetClause
}

// Listener for DELETE queries.
type DeleteQueryListener struct {
	*parser.BasePartiQLParserListener
	Table string
	Where []Condition
}

// Methods for SELECT Listener
func (l *SelectQueryListener) EnterSelectClause(ctx *parser.SelectClauseContext) {
	fmt.Println("Parsing SELECT clause:", ctx.GetText())
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

func (l *SelectQueryListener) EnterExprAnd(ctx *parser.ExprAndContext) {
	fmt.Printf("Entering AND Logical Context")
	val := ctx.GetText()
	fmt.Println(val)
}

// func (l *SelectQueryListener) EnterExprOr(ctx *parser.SelectClauseContext) {
// 	val := ctx.GetText()
// 	fmt.Println("Entering OR Logical Context")
// 	fmt.Println(val)
// }

// Extracts WHERE conditions for SELECT
func (l *SelectQueryListener) EnterPredicateComparison(ctx *parser.PredicateComparisonContext) {
	column := ctx.GetLhs().GetText()
	operator := ctx.GetOp().GetText()
	value := ctx.GetRhs().GetText()

	condition := Condition{
		Column:   column,
		Operator: operator,
		Value:    value,
	}

	// Navigate through ancestors to find logical connectors
	parent := ctx.GetParent()
	for parent != nil {
		switch parent.(type) {
		case *parser.ExprAndContext:
			fmt.Println("Found AND logic")
			condition.ANDOpr = "AND"
			parent = nil
		case *parser.ExprOrContext:
			fmt.Println("Found OR logic")
			condition.OROpr = "OR"
			parent = nil
		default:
			parent = parent.GetParent() // Go up in the hierarchy
		}
	}

	l.Where = append(l.Where, condition)
	fmt.Printf("Added condition: %s %s %s with logic: %s%s\n",
		column, operator, value, condition.ANDOpr, condition.OROpr)
}

// Methods for INSERT Listener
func (l *InsertQueryListener) EnterInsertStatement(ctx *parser.InsertStatementContext) {
	l.InsertData.Table = ctx.SymbolPrimitive().GetText()
}

func (l *InsertQueryListener) EnterInsertCommandReturning(ctx *parser.InsertCommandReturningContext) {
	l.InsertData.Table = ctx.PathSimple().GetText()
}

func (l *InsertQueryListener) EnterInsertStatementLegacy(ctx *parser.InsertStatementLegacyContext) {
	l.InsertData.Table = ctx.PathSimple().GetText()
}

func (l *InsertQueryListener) EnterProjectionItems(ctx *parser.ProjectionItemsContext) {
	for _, proj := range ctx.AllProjectionItem() {
		l.InsertData.Columns = append(l.InsertData.Columns, proj.GetText())
	}
}

func (l *InsertQueryListener) EnterValueList(ctx *parser.ValueListContext) {
	for _, expr := range ctx.AllExpr() {
		l.InsertData.Values = append(l.InsertData.Values, expr.GetText())
	}
}

func (l *InsertQueryListener) EnterValueExpr(ctx *parser.ValueExprContext) {
	jsonText := ctx.GetText()
	if strings.HasPrefix(jsonText, "{") {
		l.parseJsonObject(jsonText)
	}
}

func (l *InsertQueryListener) parseJsonObject(jsonString string) {
	jsonString = strings.Trim(jsonString, "{}")
	pairs := strings.Split(jsonString, ",")

	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) == 2 {
			key := strings.TrimSpace(strings.Trim(parts[0], "'")) // Remove surrounding quotes
			value := strings.TrimSpace(parts[1])

			if key == "additional_info" {
				// Logic to parse nested JSON as a map
				mapValue := make(map[string]interface{})
				l.additionalMapParser(value, mapValue)
				l.InsertData.AdditionalMap = mapValue
			} else {
				l.InsertData.Columns = append(l.InsertData.Columns, key)
				l.InsertData.Values = append(l.InsertData.Values, value)
			}
		}
	}
}

// Function to parse additional map information recursively
func (l *InsertQueryListener) additionalMapParser(jsonString string, m map[string]interface{}) {
	// Implement your logic to parse the map string into a Go map
	// Note that parsing logic will depend on your expected structure
	jsonString = strings.Trim(jsonString, "{}")
	pairs := strings.Split(jsonString, ",")

	for _, pair := range pairs {
		parts := strings.Split(pair, ":")
		if len(parts) == 2 {
			key := strings.TrimSpace(strings.Trim(parts[0], "'"))
			value := strings.TrimSpace(parts[1])

			// If the value is another JSON object or array, you might need to parse again or handle lists.
			m[key] = value // For simplicity, putting value directly
		}
	}
}
func (l *InsertQueryListener) EnterOnConflict(ctx *parser.OnConflictContext) {
	l.InsertData.OnConflict = ctx.GetText()
}

// Methods for UPDATE Listener
func (l *UpdateQueryListener) EnterUpdateClause(ctx *parser.UpdateClauseContext) {
	l.Table = ctx.TableBaseReference().GetText()
}

func (l *UpdateQueryListener) EnterSetCommand(ctx *parser.SetCommandContext) {
	for _, setAssign := range ctx.AllSetAssignment() {
		column := setAssign.PathSimple().GetText()
		value := setAssign.Expr().GetText()

		l.SetClauses = append(l.SetClauses, SetClause{
			Column:   column,
			Operator: "=",
			Value:    value,
		})
	}
}

func (l *UpdateQueryListener) EnterPredicateComparison(ctx *parser.PredicateComparisonContext) {
	column := ctx.GetLhs().GetText()
	operator := ctx.GetOp().GetText()
	value := ctx.GetRhs().GetText()

	condition := Condition{
		Column:   column,
		Operator: operator,
		Value:    value,
	}

	// Check logical operator from parent context node
	if parent, ok := ctx.GetParent().(antlr.ParserRuleContext); ok {
		switch parent.(type) {
		case *parser.ExprAndContext:
			fmt.Println("Detected AND condition in UPDATE")
			condition.LogicOp = "AND"
		case *parser.ExprOrContext:
			fmt.Println("Detected OR condition in UPDATE")
			condition.LogicOp = "OR"
		}
	}

	l.Where = append(l.Where, condition)
	fmt.Printf("Added condition: %s %s %s with logic: %s\n",
		column, operator, value, condition.LogicOp)
}

func (l *UpdateQueryListener) EnterExprAnd(ctx *parser.ExprAndContext) {
	fmt.Println("Entering AND context in UPDATE")
}

func (l *UpdateQueryListener) EnterExprOr(ctx *parser.ExprOrContext) {
	fmt.Println("Entering OR context in UPDATE")
}

// Methods for DELETE Listener
func (l *DeleteQueryListener) EnterDeleteCommand(ctx *parser.DeleteCommandContext) {
	if fromCtx, ok := ctx.FromClauseSimple().(*parser.FromClauseSimpleExplicitContext); ok {
		l.Table = fromCtx.PathSimple().GetText()
	}
}

// Extracts WHERE conditions for DELETE
func (l *DeleteQueryListener) EnterPredicateComparison(ctx *parser.PredicateComparisonContext) {
	column := ctx.GetLhs().GetText()
	operator := ctx.GetOp().GetText()
	value := ctx.GetRhs().GetText()

	l.Where = append(l.Where, Condition{
		Column:   column,
		Operator: operator,
		Value:    value,
	})
}

func main() {
	queries := []string{
		"SELECT age, address FROM employee WHERE age > 30 AND address = 'abc' ORDER BY age DESC LIMIT 10 OFFSET 5;",
		"INSERT INTO employee VALUE {'emp_id': 10, 'first_name': 'Marc', 'last_name': 'Richards1', 'age': 10, 'address': 'Shamli'};",
		"INSERT INTO employee VALUE {'emp_id': 10, 'first_name': 'Marc', 'last_name': 'Richards1', 'age': 10, 'address': 'Shamli', 'additional_info': {'hobbies': ['reading', 'traveling'], 'skills': {'programming': true, 'communication': true}}};",
		"UPDATE employee SET age = 11, address = 'New address' WHERE emp_id = 11 AND age > 10;",
		"DELETE FROM employee WHERE emp_id = 11 AND age = 20;",
	}

	for _, query := range queries {
		fmt.Println("")
		fmt.Println("Executing query:", query)

		lexer := parser.NewPartiQLLexer(antlr.NewInputStream(query))
		stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
		p := parser.NewPartiQLParser(stream)

		if strings.HasPrefix(query, "SELECT") {
			selectListener := &SelectQueryListener{}
			antlr.ParseTreeWalkerDefault.Walk(selectListener, p.Root())

			fmt.Println("Extracted Query Components:")
			fmt.Println("Columns:", strings.Join(selectListener.Columns, ", "))
			fmt.Println("Tables:", strings.Join(selectListener.Tables, ", "))
			if len(selectListener.Where) > 0 {
				for i := range selectListener.Where {
					fmt.Printf("WHERE Clause %d: %s %s %s, %s, %s\n", i, selectListener.Where[i].Column, selectListener.Where[i].Operator, selectListener.Where[i].Value, selectListener.Where[i].ANDOpr, selectListener.Where[i].OROpr)
				}
			}
			fmt.Println("ORDER BY:", strings.Join(selectListener.OrderBy, ", "))
			fmt.Println("LIMIT:", selectListener.Limit)
			fmt.Println("OFFSET:", selectListener.Offset)

		} else if strings.HasPrefix(query, "INSERT") {
			insertListener := &InsertQueryListener{}
			antlr.ParseTreeWalkerDefault.Walk(insertListener, p.Root())

			fmt.Println("\nExtracted INSERT Query Components:")
			fmt.Println("Table:", insertListener.InsertData.Table)
			fmt.Println("Columns:", strings.Join(insertListener.InsertData.Columns, ", "))
			fmt.Println("Values:", strings.Join(insertListener.InsertData.Values, ", "))
			fmt.Println("AdditionalMap:", insertListener.InsertData.AdditionalMap)
			fmt.Println("ON CONFLICT:", insertListener.InsertData.OnConflict)

		} else if strings.HasPrefix(query, "UPDATE") {
			updateListener := &UpdateQueryListener{}
			antlr.ParseTreeWalkerDefault.Walk(updateListener, p.Root())

			fmt.Println("UPDATE table:", updateListener.Table)
			for _, clause := range updateListener.SetClauses {
				fmt.Printf("SET Clause: %s %s %s\n", clause.Column, clause.Operator, clause.Value)
			}
			if len(updateListener.Where) > 0 {
				for i := range updateListener.Where {
					fmt.Printf("WHERE Clause %d: %s %s %s %s\n", i, updateListener.Where[i].Column, updateListener.Where[i].Operator, updateListener.Where[i].Value, updateListener.Where[i].LogicOp)
				}
			}

		} else if strings.HasPrefix(query, "DELETE") {
			deleteListener := &DeleteQueryListener{}
			antlr.ParseTreeWalkerDefault.Walk(deleteListener, p.Root())

			fmt.Println("DELETE FROM table:", deleteListener.Table)
			if len(deleteListener.Where) > 0 {
				for i := range deleteListener.Where {
					fmt.Printf("WHERE Clause %d: %s %s %s\n", i, deleteListener.Where[i].Column, deleteListener.Where[i].Operator, deleteListener.Where[i].Value)
				}
			}
		}
	}
}
