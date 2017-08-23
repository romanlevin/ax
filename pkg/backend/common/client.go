package common

import (
	"time"
)

type JsonObject map[string]interface{}
type JsonList []interface{}

type Client interface {
	Query(query Query, eachFunc func(object JsonObject))
}

type QueryFilter struct {
	FieldName string
	Value     string
}

type Query struct {
	QueryString  string
	After        *time.Time
	Before       *time.Time
	SelectFields []string
	Filters      []QueryFilter
	MaxResults   int
	QueryAsc     bool
	ResultsDesc  bool
	Follow       bool
}
