package translator

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/PartiQLParser/parser"
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

func trimSingleQuotes(s string) string {
	// Check if the string starts and ends with single quotes
	if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") {
		// Remove the quotes from the beginning and end
		s = s[1 : len(s)-1]
	}
	return s
}
