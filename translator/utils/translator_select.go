package translator

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/PartiQLParser/parser"
)

type Condition struct {
	Column   string
	Operator string
	Value    string
	ANDOpr   string
	OROpr    string
	LogicOp  string
}

type SelectQueryListener struct {
	*parser.BasePartiQLParserListener
	Columns []string
	Tables  []string
	Where   []Condition
	OrderBy []string
	Limit   string
	Offset  string
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

func (t *Translator) ToSpannerSelect(query string) (*SelectQueryMap, error) {

	lexer := parser.NewPartiQLLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewPartiQLParser(stream)
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
	return nil, nil
}
