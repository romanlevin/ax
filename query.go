package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

type QueryResult struct {
	Responses []struct {
		Hits struct {
			Hits []Hit `json:"hits"`
		} `json:"hits"`
	} `json:"responses"`
}

type Hit struct {
	Id     string     `json:"_id"`
	Source JsonObject `json:"_source"`
}

type QueryFilter struct {
	FieldName string
	Value     string
}

type Query struct {
	QueryString string
	After       time.Time
	Before      time.Time
	Filters     []QueryFilter
	MaxResults  int
	QueryAsc    bool
	ResultsAsc  bool
}

type HitsByAscDate []Hit

func (a HitsByAscDate) Len() int      { return len(a) }
func (a HitsByAscDate) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a HitsByAscDate) Less(i, j int) bool {
	return a[i].Source["@timestamp"].(string) < a[j].Source["@timestamp"].(string)
}

func queryMessages(index string, query Query) ([]Hit, error) {
	orderDirection := "desc"
	if query.QueryAsc {
		orderDirection = "asc"
	}
	filterList := JsonList{
		JsonObject{
			"query_string": JsonObject{
				"analyze_wildcard": true,
				"query":            query.QueryString,
			},
		},
		JsonObject{
			"range": JsonObject{
				"@timestamp": JsonObject{
					"gte":    unixMillis(query.After),
					"lte":    unixMillis(query.Before),
					"format": "epoch_millis",
				},
			},
		},
	}
	for _, filter := range query.Filters {
		m := JsonObject{}
		m[filter.FieldName] = JsonObject{
			"query": filter.Value,
			"type":  "phrase",
		}
		filterList = append(filterList, JsonObject{
			"match": m,
		})
	}
	body, err := createMultiSearch(
		JsonObject{
			"index":              JsonList{index},
			"ignore_unavailable": true,
		},
		JsonObject{
			"size": query.MaxResults,
			"sort": JsonList{
				JsonObject{
					"@timestamp": JsonObject{
						"order":         orderDirection,
						"unmapped_type": "boolean",
					},
				},
			},
			"query": JsonObject{
				"bool": JsonObject{
					"must": filterList,
				},
			},
		})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/_msearch", *EsUrl), body)
	if err != nil {
		return nil, err
	}
	addHeaders(req)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data QueryResult
	err = decoder.Decode(&data)
	if err != nil {
		return nil, err
	}
	hits := data.Responses[0].Hits.Hits
	if query.ResultsAsc {
		sort.Sort(HitsByAscDate(hits))
	}
	return hits, nil
}
