package transalter

import (
	"fmt"

	"github.com/cloudspannerecosystem/dynamodb-adapter/transalter/parser"
	partiql "github.com/cloudspannerecosystem/dynamodb-adapter/transalter/parser"

	"github.com/antlr4-go/antlr/v4"
)

// Funtion to create lexer and parser object for the partiql query
func NewPartiQLParser(partiQL string, isDebug bool) (*partiql.PartiQLParser, error) {
	if partiQL == "" {
		return nil, fmt.Errorf("invalid input string")
	}

	lexer := partiql.NewPartiQLLexer(antlr.NewInputStream(partiQL))
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
