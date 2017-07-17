package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

type Indexes struct {
	Indices map[string]struct {
		Fields map[string]FieldType `json:"fields"`
	} `json:"indices"`
}

type FieldType struct {
	MinValue int64 `json:"min_value"`
	MaxValue int64 `json:"max_value"`
}

type IndexMeta struct {
	Name  string
	MinTs int64
	MaxTs int64
}

type ByReverseDate []IndexMeta

func (a ByReverseDate) Len() int           { return len(a) }
func (a ByReverseDate) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByReverseDate) Less(i, j int) bool { return a[i].Name > a[j].Name }

func queryIndexes(indexPrefix string, after, before time.Time) ([]IndexMeta, error) {
	body, err := createMultiSearch(
		JsonObject{
			"fields": JsonList{"@timestamp"},
			"index_constraints": JsonObject{
				"@timestamp": JsonObject{
					"max_value": JsonObject{
						"gte":    unixMillis(after),
						"format": "epoch_millis",
					},
					"min_value": JsonObject{
						"lte":    unixMillis(before),
						"format": "epoch_millis",
					},
				},
			},
		},
	)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/%s/_field_stats?level=indices", *EsUrl, *IndexName), body)
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
	var data Indexes
	err = decoder.Decode(&data)
	if err != nil {
		return nil, err
	}

	// Build result
	results := make([]IndexMeta, 0, 10)
	for indexName, indexData := range data.Indices {
		results = append(results, IndexMeta{
			Name:  indexName,
			MinTs: indexData.Fields["@timestamp"].MinValue,
			MaxTs: indexData.Fields["@timestamp"].MaxValue,
		})
	}

	sort.Sort(ByReverseDate(results))

	return results, nil

}
