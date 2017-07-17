package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

func createMultiSearch(objs ...interface{}) (io.Reader, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, obj := range objs {
		err := encoder.Encode(obj)
		if err != nil {
			return nil, err
		}
	}
	fmt.Println(buf.String())
	return &buf, nil
}

type JsonObject map[string]interface{}
type JsonList []interface{}

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

func addHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Basic emhlbWVsOnR6UURmYzJqV3ZOWFU1Rm4=")
	req.Header.Set("Kbn-Version", "5.2.2")
	req.Header.Set("Content-Type", "application/x-ldjson")
	req.Header.Set("Accept", "application/json, text/plain, */*")
}

type Indexes struct {
	Indices map[string]struct {
		Fields map[string]FieldType `json:"fields"`
	} `json:"indices"`
}

type FieldType struct {
	// all we care about
	MinValue int64 `json:"min_value"`
	MaxValue int64 `json:"max_value"`
}

type IndexMeta struct {
	Name  string
	MinTs int64
	MaxTs int64
}

func queryIndexes(indexPrefix string, minTs, maxTs int64) ([]IndexMeta, error) {
	body, err := createMultiSearch(
		JsonObject{
			"fields": JsonList{"@timestamp"},
			"index_constraints": JsonObject{
				"@timestamp": JsonObject{
					"max_value": JsonObject{
						"gte":    minTs,
						"format": "epoch_millis",
					},
					"min_value": JsonObject{
						"lte":    maxTs,
						"format": "epoch_millis",
					},
				},
			},
		},
	)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", "https://kibana-prod.egnyte-internal.com/elasticsearch/turbo-*/_field_stats?level=indices", body)
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

	return results, nil

}

func queryMessages(index string, after, before time.Time, query string) {
	body, err := createMultiSearch(
		JsonObject{
			"index":              JsonList{index},
			"ignore_unavailable": true,
		},
		JsonObject{
			"size": 500,
			"sort": JsonList{
				JsonObject{
					"@timestamp": JsonObject{
						"order":         "desc",
						"unmapped_type": "boolean",
					},
				},
			},
			"query": JsonObject{
				"bool": JsonObject{
					"must": JsonList{
						JsonObject{
							"query_string": JsonObject{
								"analyze_wildcard": true,
								"query":            query,
							},
						},
						JsonObject{
							"range": JsonObject{
								"@timestamp": JsonObject{
									"gte":    unixMillis(after),
									"lte":    unixMillis(before),
									"format": "epoch_millis",
								},
							},
						},
					},
				},
			},
		})
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", "https://kibana-prod.egnyte-internal.com/elasticsearch/_msearch", body)
	if err != nil {
		panic(err)
	}
	addHeaders(req)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error", err)
		return
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var data QueryResult
	err = decoder.Decode(&data)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", data)
}

func unixMillis(t time.Time) int64 {
	return t.Unix() * 1000
}

func main() {
	now := time.Now()
	threeDaysAgo := now.Add(-3 * 24 * time.Hour)
	/*
		now := time.Now()
		indices, err := queryIndexes("turbo-*", unixMillis(now.Add(-3*24*time.Hour)), unixMillis(now))
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", indices)
	*/
	fmt.Println(now, threeDaysAgo)
	fmt.Println(unixMillis(now), unixMillis(threeDaysAgo))
	queryMessages("turbo-2017.07.15", threeDaysAgo, now, "*")
}
