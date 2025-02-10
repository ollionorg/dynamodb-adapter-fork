package translator

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/PartiQLParser/parser"
)

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

func (t *Translator) ToSpannerUpdate(query string) (*UpdateQueryMap, error) {
	updateQueryMap := &UpdateQueryMap{}

	// Lexer and parser setup
	lexer := parser.NewPartiQLLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewPartiQLParser(stream)

	updateListener := &UpdateQueryListener{}
	antlr.ParseTreeWalkerDefault.Walk(updateListener, p.Root())

	fmt.Println("UPDATE table:", updateListener.Table)
	updateQueryMap.Table = updateListener.Table
	updateQueryMap.PartiQLQuery = query
	updateQueryMap.QueryType = "UPDATE"
	for _, clause := range updateListener.SetClauses {
		fmt.Printf("SET Clause: %s %s %s\n", clause.Column, clause.Operator, clause.Value)
		updateQueryMap.UpdateSetValues = append(updateQueryMap.UpdateSetValues, UpdateSetValue{
			Column:   clause.Column,
			Operator: clause.Operator,
			Value:    trimSingleQuotes(clause.Value),
		})
	}
	if len(updateListener.Where) > 0 {
		for i := range updateListener.Where {
			fmt.Printf("WHERE Clause %d: %s %s %s %s\n", i, updateListener.Where[i].Column, updateListener.Where[i].Operator, updateListener.Where[i].Value, updateListener.Where[i].LogicOp)
			updateQueryMap.Clauses = append(updateQueryMap.Clauses, Clause{
				Column:   updateListener.Where[i].Column,
				Operator: updateListener.Where[i].Operator,
				Value:    updateListener.Where[i].Value,
			})
		}
	}
	return updateQueryMap, nil
}
