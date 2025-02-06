package translator

import (
	"github.com/ollionorg/cassandra-to-spanner-proxy/tableConfig"
	"go.uber.org/zap"
)

type Translator struct {
	Logger *zap.Logger
	// TableConfig     *tableConfig.TableConfig
	KeyspaceFlatter bool
	UseRowTimestamp bool
	UseRowTTL       bool
	Debug           bool
}

// SelectQueryMap represents the mapping of a select query along with its translation details.
type SelectQueryMap struct {
	PartiQLQuery string // Original query string
	SpannerQuery string // Translated query string suitable for Spanner
	QueryType    string // Type of the query (e.g., SELECT)
	Table        string // Table involved in the query
	// ColumnMeta      ColumnMeta                           // Translator generated Metadata about the columns involved
	// Clauses         []Clause                             // List of clauses in the query
	// Limit           Limit                                // Limit clause details
	// OrderBy         OrderBy                              // Order by clause details
	Params          map[string]interface{}               // Parameters for the query
	ParamKeys       []string                             // column_name of the parameters
	AliasMap        map[string]tableConfig.AsKeywordMeta // Aliases used in the query
	PrimaryKeys     []string                             // Primary keys of the table
	ColumnsWithInOp []string                             // Columns involved in IN operations
}
