package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"

	"github.com/araddon/dateparse"
	"github.com/fatih/color"
)

type QueryResult struct {
	Responses []struct {
		Hits struct {
			Hits []Hit `json:"hits"`
		} `json:"hits"`
	} `json:"responses"`
}

type Hit struct {
	ID     string     `json:"_id"`
	Source JsonObject `json:"_source"`
}

type QueryFilter struct {
	FieldName string
	Value     string
}

type Query struct {
	QueryString string
	After       *time.Time
	Before      *time.Time
	Filters     []QueryFilter
	MaxResults  int
	QueryAsc    bool
	ResultsDesc bool
}

type hitsByAscDate []Hit

func (a hitsByAscDate) Len() int      { return len(a) }
func (a hitsByAscDate) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a hitsByAscDate) Less(i, j int) bool {
	return a[i].Source["@timestamp"].(string) < a[j].Source["@timestamp"].(string)
}

func queryMessages(rc RuntimeConfig, index string, query Query) ([]Hit, error) {
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
	}

	if query.After != nil || query.Before != nil {
		rangeObj := JsonObject{
			"range": JsonObject{
				"@timestamp": JsonObject{
					"format": "epoch_millis",
				},
			},
		}
		if query.After != nil {
			rangeObj["range"].(JsonObject)["@timestamp"].(JsonObject)["gt"] = unixMillis(*query.After)
		}
		if query.Before != nil {
			rangeObj["range"].(JsonObject)["@timestamp"].(JsonObject)["lt"] = unixMillis(*query.Before)
		}
		filterList = append(filterList, rangeObj)
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
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/elasticsearch/_msearch", rc.URL), body)
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

var (
	queryCommand      = kingpin.Command("query", "Query Kibana")
	queryBefore       = queryCommand.Flag("before", "Results from before").String()
	queryAfter        = queryCommand.Flag("after", "Results from after").String()
	queryMaxResults   = queryCommand.Flag("results", "Maximum number of results").Short('n').Default("200").Int()
	querySelect       = queryCommand.Flag("select", "Fields to select").Short('s').Strings()
	queryWhere        = queryCommand.Flag("where", "Add a filter").Short('w').Strings()
	querySortDesc     = queryCommand.Flag("desc", "Sort results reverse-chronologically").Default("false").Bool()
	queryOutputFormat = queryCommand.Flag("output", "Output format: text|json|yaml").Short('o').Default("text").String()
	queryFollow       = queryCommand.Flag("follow", "Follow log in quasi-realtime, similar to tail -f").Short('f').Default("false").Bool()
	queryString       = queryCommand.Arg("query", "Query string").Default("*").Strings()
)

func project(m map[string]interface{}, fields []string) map[string]interface{} {
	if len(fields) == 0 {
		return m
	}
	projected := map[string]interface{}{}
	for _, field := range fields {
		// fieldParts := strings.Split(field, ".")
		// current := projected
		// for _, fieldPart := range fieldParts {

		}
		projected[field] = m[field]
	}
	return projected
}

func buildFilters(wheres []string) []QueryFilter {
	filters := make([]QueryFilter, 0, len(wheres))
	for _, whereClause := range *queryWhere {
		pieces := strings.SplitN(whereClause, ":", 2)
		if len(pieces) != 2 {
			fmt.Println("Invalid where clause", whereClause)
			os.Exit(1)
		}
		filters = append(filters, QueryFilter{pieces[0], pieces[1]})
	}
	return filters
}

func printMessage(message JsonObject, queryOutputFormat string) {
	switch queryOutputFormat {
	case "text":
		ts := message["@timestamp"]
		fmt.Printf("[%s] ", color.MagentaString(ts.(string)))
		msg, hasMessage := message["message"]
		if hasMessage {
			messageColor := color.New(color.Bold)
			fmt.Printf("%s ", messageColor.Sprint(msg))
		}
		for key, value := range message {
			if key == "@timestamp" || key == "message" {
				continue
			}
			fmt.Printf("%s=%s ", color.CyanString(key), jsonThis(value))
		}
		fmt.Println()
	case "json":
		encoder := json.NewEncoder(os.Stdout)
		err := encoder.Encode(message)
		if err != nil {
			fmt.Println("Error JSON encoding")
		}
	case "json-pretty":
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		err := encoder.Encode(message)
		if err != nil {
			fmt.Println("Error JSON encoding")
		}
	case "yaml":
		buf, err := yaml.Marshal(message)
		if err != nil {
			fmt.Println("Error YAML encoding")
		}
		fmt.Printf("---\n%s", string(buf))
	}
}

func queryIndex(rc RuntimeConfig, idxName string, q Query) ([]JsonObject, error) {
	hits, err := queryMessages(rc, idxName, q)
	if err != nil {
		return nil, err
	}

	allMessages := make([]JsonObject, 0, 200)
	for _, hit := range hits {
		var message map[string]interface{}
		if len(*querySelect) > 0 {
			message = project(hit.Source, *querySelect)
			message["@timestamp"] = hit.Source["@timestamp"]
		} else {
			message = hit.Source
		}
		// Note: only kept in slice for aggregation in countDistinct later + counting total number of results
		allMessages = append(allMessages, message)
	}
	return allMessages, nil
}

func queryFollowMain(rc RuntimeConfig) {
	cache := NewIndexCache(rc, CacheIndices)
	var after *time.Time
	retries := 0
	for {
		indices, err := cache.Indices()
		if err != nil {
			panic(err)
		}
		// We will only ever read from the most recent index (limitation?)
		idxName := indices[0].Name
		// fmt.Fprintf(os.Stderr, "Querying index %s\n", idxName)
		allMessages, err := queryIndex(rc, idxName, Query{
			QueryString: strings.Join(*queryString, " "),
			After:       after,
			Filters:     buildFilters(*queryWhere),
			MaxResults:  *queryMaxResults,
			ResultsDesc: *querySortDesc,
		})
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
			printMessage(message, *queryOutputFormat)
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

func queryMain(rc RuntimeConfig) {
	var before *time.Time
	var after *time.Time
	if *queryAfter != "" {
		var err error
		afterTime, err := dateparse.ParseAny(*queryAfter)
		if err != nil {
			fmt.Println("Could parse after date:", *queryAfter)
			os.Exit(1)
		}
		after = &afterTime
	}
	if *queryBefore != "" {
		var err error
		beforeTime, err := dateparse.ParseAny(*queryBefore)
		if err != nil {
			fmt.Println("Could parse before date:", *queryBefore)
			os.Exit(1)
		}
		before = &beforeTime
	}

	if *queryFollow {
		queryFollowMain(rc)
		return // Actually a NOOP, because queryFollowMain never returns
	}
	cache := NewIndexCache(rc, CacheIndices)
	indices, err := cache.Indices()
	if err != nil {
		panic(err)
	}
	printedResultsCount := 0
	for i := 0; i < len(indices) && printedResultsCount < *queryMaxResults; i++ {
		idxName := indices[i].Name
		fmt.Fprintf(os.Stderr, "Querying index %s\n", idxName)
		allMessages, err := queryIndex(rc, idxName, Query{
			QueryString: strings.Join(*queryString, " "),
			After:       after,
			Before:      before,
			Filters:     buildFilters(*queryWhere),
			MaxResults:  *queryMaxResults,
			ResultsDesc: *querySortDesc,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to Kibana: %v", err)
			os.Exit(2)
		}
		for _, message := range allMessages {
			printMessage(message, *queryOutputFormat)
			printedResultsCount++
			if printedResultsCount >= *queryMaxResults {
				break
			}
		}

	}
}
