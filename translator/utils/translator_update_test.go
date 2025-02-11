package translator

import (
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/PartiQLParser/parser"
	"github.com/stretchr/testify/assert"
)

func TestToSpannerUpdate(t *testing.T) {
	query := "UPDATE employee SET status = 'active', address = 'new address', age = 31 WHERE emp_id = 'eqi';"

	translator := &Translator{}

	// Set up the lexer and parser
	lexer := parser.NewPartiQLLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewPartiQLParser(stream)

	// Prepare the listener for capturing parsed data
	updateListener := &UpdateQueryListener{}
	antlr.ParseTreeWalkerDefault.Walk(updateListener, p.Root())

	// Call ToSpannerUpdate after parsing
	updateQueryMap, err := translator.ToSpannerUpdate(query)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Prepare expected results
	expectedTable := "employee"
	expectedSetClauses := []SetClause{
		{Column: "status", Operator: "=", Value: "active"},
		{Column: "address", Operator: "=", Value: "new address"},
		{Column: "age", Operator: "=", Value: "31"},
	}
	expectedWhereConditions := []Condition{
		{Column: "emp_id", Operator: "=", Value: "eqi"},
	}

	// Assertions for Table
	assert.Equal(t, expectedTable, updateQueryMap.Table)

	// Assertions for SET Clauses
	assert.Equal(t, len(expectedSetClauses), len(updateQueryMap.UpdateSetValues))
	for i, setClause := range updateQueryMap.UpdateSetValues {
		assert.Equal(t, expectedSetClauses[i].Column, setClause.Column)
		assert.Equal(t, expectedSetClauses[i].Operator, setClause.Operator)
		assert.Equal(t, expectedSetClauses[i].Value, setClause.Value)
	}

	// Assertions for WHERE conditions
	assert.Equal(t, len(expectedWhereConditions), len(updateQueryMap.Clauses))
	for i, whereCondition := range updateQueryMap.Clauses {
		assert.Equal(t, expectedWhereConditions[i].Column, whereCondition.Column)
		assert.Equal(t, expectedWhereConditions[i].Operator, whereCondition.Operator)
		assert.Equal(t, expectedWhereConditions[i].Value, whereCondition.Value)
	}
}
