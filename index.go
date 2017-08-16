package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

type IndexList struct {
	Hits struct {
		Hits []IndexListHit `json:"hits"`
	} `json:"hits"`
}

type IndexListHit struct {
	Id string `json:"_id"`
}

func ListIndices(rc RuntimeConfig) ([]string, error) {
	body, err := createMultiSearch(
		JsonObject{
			"query": JsonObject{
				"match_all": JsonObject{},
			},
			"size": 10000,
		},
	)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/es_admin/.kibana/index-pattern/_search?stored_fields=", rc.Url), body)
	if err != nil {
		return nil, err
	}
	addHeaders(rc, req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data IndexList
	err = decoder.Decode(&data)
	if err != nil {
		return nil, err
	}
	// Build list
	indexNames := make([]string, 0, len(data.Hits.Hits))
	for _, indexInfo := range data.Hits.Hits {
		indexNames = append(indexNames, indexInfo.Id)
	}
	return indexNames, nil
}

var (
	listIndexCommand = kingpin.Command("list-index", "List all indices")
)

func listIndicesMain(rc RuntimeConfig) {
	indices, err := ListIndices(rc)
	if err != nil {
		panic(err)
	}
	for _, indexName := range indices {
		fmt.Println(indexName)
	}
}

type Indices struct {
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

func CacheIndices() ([]IndexMeta, error) {
	rc := BuildConfig()
	return queryIndices(rc)
}

func queryIndices(rc RuntimeConfig) ([]IndexMeta, error) {
	// Query indices for the last 3 months
	before := time.Now()
	after := before.Add(-90 * 24 * time.Hour)
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
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/elasticsearch/%s/_field_stats?level=indices", rc.Url, rc.Index), body)
	if err != nil {
		return nil, err
	}
	addHeaders(rc, req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data Indices
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
