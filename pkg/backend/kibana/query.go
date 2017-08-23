package kibana

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/zefhemel/ax/pkg/backend/common"

	"os"

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
	ID     string            `json:"_id"`
	Source common.JsonObject `json:"_source"`
}

type hitsByAscDate []Hit

func (a hitsByAscDate) Len() int      { return len(a) }
func (a hitsByAscDate) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a hitsByAscDate) Less(i, j int) bool {
	return a[i].Source["@timestamp"].(string) < a[j].Source["@timestamp"].(string)
}

func (client *Client) queryMessages(subIndex string, query common.Query) ([]Hit, error) {
	orderDirection := "desc"
	if query.QueryAsc {
		orderDirection = "asc"
	}
	filterList := common.JsonList{
		common.JsonObject{
			"query_string": common.JsonObject{
				"analyze_wildcard": true,
				"query":            query.QueryString,
			},
		},
	}

	if query.After != nil || query.Before != nil {
		rangeObj := common.JsonObject{
			"range": common.JsonObject{
				"@timestamp": common.JsonObject{
					"format": "epoch_millis",
				},
			},
		}
		if query.After != nil {
			rangeObj["range"].(common.JsonObject)["@timestamp"].(common.JsonObject)["gt"] = unixMillis(*query.After)
		}
		if query.Before != nil {
			rangeObj["range"].(common.JsonObject)["@timestamp"].(common.JsonObject)["lt"] = unixMillis(*query.Before)
		}
		filterList = append(filterList, rangeObj)
	}
	for _, filter := range query.Filters {
		m := common.JsonObject{}
		m[filter.FieldName] = common.JsonObject{
			"query": filter.Value,
			"type":  "phrase",
		}
		filterList = append(filterList, common.JsonObject{
			"match": m,
		})
	}
	body, err := createMultiSearch(
		common.JsonObject{
			"index":              common.JsonList{subIndex},
			"ignore_unavailable": true,
		},
		common.JsonObject{
			"size": query.MaxResults,
			"sort": common.JsonList{
				common.JsonObject{
					"@timestamp": common.JsonObject{
						"order":         orderDirection,
						"unmapped_type": "boolean",
					},
				},
			},
			"query": common.JsonObject{
				"bool": common.JsonObject{
					"must": filterList,
				},
			},
		})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/elasticsearch/_msearch", client.URL), body)
	if err != nil {
		return nil, err
	}
	client.addHeaders(req)

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
	if !query.ResultsDesc {
		sort.Sort(hitsByAscDate(hits))
	}
	return hits, nil
}

func project(m map[string]interface{}, fields []string) map[string]interface{} {
	if len(fields) == 0 {
		return m
	}
	projected := map[string]interface{}{}
	for _, field := range fields {
		projected[field] = m[field]
	}
	return projected
}

func (client *Client) queryFollow(q common.Query, eachFunc func(common.JsonObject)) {
	var after *time.Time
	retries := 0
	for {
		// fmt.Fprintf(os.Stderr, "Querying index %s\n", idxName)
		q.After = after
		allMessages, err := client.querySubIndex(client.Index, q)
		if err != nil {
			retries++
			if retries < 10 {
				fmt.Fprintf(os.Stderr, "Could not connect to Kibana: %v retrying in 5s\n", err)
				time.Sleep(5 * time.Second)
				continue
			} else {
				fmt.Fprintf(os.Stderr, "Could not connect to Kibana: %v\nExceeded total number of retries, exiting.\n", err)
				os.Exit(1)
			}
		}
		// Request succesful, so reset retry count
		retries = 0
		for _, message := range allMessages {
			eachFunc(message)
			afterDate, err := time.Parse(time.RFC3339, message["@timestamp"].(string))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could not parse timestamp: %s", message["@timestamp"])
				continue
			}
			after = &afterDate
		}
		if after == nil {
			fmt.Fprintf(os.Stderr, "Could determine latest hit, defaulting to now")
			afterDate := time.Now()
			after = &afterDate
		}
		time.Sleep(5 * time.Second)
	}
	// Will never end
}

func (client *Client) Query(q common.Query, eachFunc func(common.JsonObject)) {
	if q.Follow {
		client.queryFollow(q, eachFunc)
		return
	}
	printedResultsCount := 0
	fmt.Fprintf(os.Stderr, "Querying index %s\n", client.Index)
	allMessages, err := client.querySubIndex(client.Index, q)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to Kibana: %v", err)
		os.Exit(2)
	}
	for _, message := range allMessages {
		eachFunc(message)
		printedResultsCount++
		if printedResultsCount >= q.MaxResults {
			break
		}
	}
}

func (client *Client) querySubIndex(subIndex string, q common.Query) ([]common.JsonObject, error) {
	hits, err := client.queryMessages(subIndex, q)
	if err != nil {
		return nil, err
	}

	allMessages := make([]common.JsonObject, 0, 200)
	for _, hit := range hits {
		var message map[string]interface{}
		if len(q.SelectFields) > 0 {
			message = project(hit.Source, q.SelectFields)
			message["@timestamp"] = hit.Source["@timestamp"]
		} else {
			message = hit.Source
		}
		// Note: only kept in slice for aggregation in countDistinct later + counting total number of results
		allMessages = append(allMessages, message)
	}
	return allMessages, nil
}
