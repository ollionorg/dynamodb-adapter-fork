package translator

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/partiqlparser/parser"
)

// Funtion to create lexer and parser object for the partiql query
func NewPartiQLParser(partiQL string, isDebug bool) (*parser.PartiQLParser, error) {
	if partiQL == "" {
		return nil, fmt.Errorf("invalid input string")
	}

	lexer := parser.NewPartiQLLexer(antlr.NewInputStream(partiQL))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewPartiQLParser(stream)
	if p == nil {
		return nil, fmt.Errorf("error while creating parser object")
	}
	if !isDebug {
		p.RemoveErrorListeners()
	}
	return p, nil
}
