package translator

import (
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
	PartiQLQuery      string
	SpannerQuery      string
	QueryType         string
	Table             string
	ParamKeys         []string
	ProjectionColumns []string
	OrderBy           []string // Ensure OrderBy is part of this struct
	Limit             string   // Ensure Limit is part of this struct
	Offset            string   // Ensure Offset is part of this struct
}
