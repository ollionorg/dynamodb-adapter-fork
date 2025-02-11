package translator

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/PartiQLParser/parser"
)

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
			condition.LogicOp = "AND"
		case *parser.ExprOrContext:
			condition.LogicOp = "OR"
		}
	}

	l.Where = append(l.Where, condition)
}

func (l *UpdateQueryListener) EnterExprAnd(ctx *parser.ExprAndContext) {
}

func (l *UpdateQueryListener) EnterExprOr(ctx *parser.ExprOrContext) {
}

func (t *Translator) ToSpannerUpdate(query string) (*UpdateQueryMap, error) {
	updateQueryMap := &UpdateQueryMap{}

	// Lexer and parser setup
	lexer := parser.NewPartiQLLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewPartiQLParser(stream)

	updateListener := &UpdateQueryListener{}
	antlr.ParseTreeWalkerDefault.Walk(updateListener, p.Root())

	updateQueryMap.Table = updateListener.Table
	updateQueryMap.PartiQLQuery = query
	updateQueryMap.QueryType = "UPDATE"
	for _, clause := range updateListener.SetClauses {
		updateQueryMap.UpdateSetValues = append(updateQueryMap.UpdateSetValues, UpdateSetValue{
			Column:   clause.Column,
			Operator: clause.Operator,
			Value:    trimSingleQuotes(clause.Value),
		})
	}
	if len(updateListener.Where) > 0 {
		for i := range updateListener.Where {
			updateQueryMap.Clauses = append(updateQueryMap.Clauses, Clause{
				Column:   updateListener.Where[i].Column,
				Operator: updateListener.Where[i].Operator,
				Value:    trimSingleQuotes(updateListener.Where[i].Value),
			})
		}
	}
	return updateQueryMap, nil
}
