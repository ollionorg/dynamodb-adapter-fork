package translator

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/PartiQLParser/parser"
)

func (t *Translator) ToSpannerDelete(query string) (*DeleteQueryMap, error) {
	deleteQueryMap := &DeleteQueryMap{}
	deleteListener := &DeleteQueryListener{}

	// Lexer and parser setup
	lexer := parser.NewPartiQLLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewPartiQLParser(stream)
	antlr.ParseTreeWalkerDefault.Walk(deleteListener, p.Root())

	fmt.Println("DELETE FROM table:", deleteListener.Table)
	deleteQueryMap.Table = deleteListener.Table
	if len(deleteListener.Where) > 0 {
		for i := range deleteListener.Where {
			fmt.Printf("WHERE Clause %d: %s %s %s\n", i, deleteListener.Where[i].Column, deleteListener.Where[i].Operator, deleteListener.Where[i].Value)
			deleteQueryMap.Clauses = append(deleteQueryMap.Clauses, Clause{
				Column:   deleteListener.Where[i].Column,
				Operator: deleteListener.Where[i].Operator,
				Value:    deleteListener.Where[i].Value,
			})
		}
	}
	return deleteQueryMap, nil
}
