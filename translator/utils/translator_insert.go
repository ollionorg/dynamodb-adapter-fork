package translator

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cloudspannerecosystem/dynamodb-adapter/translator/PartiQLParser/parser"
)

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

func (t *Translator) ToSpannerInsert(query string) (*InsertStatement, error) {
	insertListener := &InsertQueryListener{}
	insertStatement := &InsertStatement{}
	// Lexer and parser setup
	lexer := parser.NewPartiQLLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := parser.NewPartiQLParser(stream)
	antlr.ParseTreeWalkerDefault.Walk(insertListener, p.Root())

	insertStatement.Table = insertListener.InsertData.Table
	for _, column := range insertListener.InsertData.Columns {
		insertStatement.Columns = append(insertStatement.Columns, trimSingleQuotes(column))
	}
	for _, val := range insertListener.InsertData.Values {
		insertStatement.Values = append(insertStatement.Values, trimSingleQuotes(val))
	}
	insertStatement.AdditionalMap = insertListener.InsertData.AdditionalMap
	insertStatement.OnConflict = insertListener.InsertData.OnConflict
	return insertStatement, nil
}
