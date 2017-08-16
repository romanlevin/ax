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
	"github.com/olekukonko/tablewriter"
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
	After       *time.Time
	Before      *time.Time
	Filters     []QueryFilter
	MaxResults  int
	QueryAsc    bool
	ResultsDesc bool
}

type HitsByAscDate []Hit

func (a HitsByAscDate) Len() int      { return len(a) }
func (a HitsByAscDate) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a HitsByAscDate) Less(i, j int) bool {
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
			rangeObj["range"].(JsonObject)["@timestamp"].(JsonObject)["gte"] = unixMillis(*query.After)
		}
		if query.Before != nil {
			rangeObj["range"].(JsonObject)["@timestamp"].(JsonObject)["lte"] = unixMillis(*query.Before)
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
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/elasticsearch/_msearch", rc.Url), body)
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
		sort.Sort(HitsByAscDate(hits))
	}
	return hits, nil
}

var (
	queryCommand       = kingpin.Command("query", "Query Kibana")
	queryBefore        = queryCommand.Flag("before", "Results from before").String()
	queryAfter         = queryCommand.Flag("after", "Results from after").String()
	queryMaxResults    = queryCommand.Flag("results", "Maximum number of results").Short('n').Default("200").Int()
	querySelect        = queryCommand.Flag("select", "Fields to select").Short('s').Strings()
	queryWhere         = queryCommand.Flag("where", "Add a filter").Short('w').Strings()
	querySortDesc      = queryCommand.Flag("desc", "Sort results reverse-chronologically").Default("false").Bool()
	queryOutputFormat  = queryCommand.Flag("output", "Output format: text|json|yaml").Short('o').Default("text").String()
	queryCountDistinct = queryCommand.Flag("count-distinct", "Tally up number of matching log based on selected fields").Default("false").Bool()
	queryString        = queryCommand.Arg("query", "Query string").Default("*").Strings()
)

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

func jsonThis(obj interface{}) string {
	buf, err := json.Marshal(obj)
	if err != nil {
		return "<<JSON ENCODE ERROR>>"
	}
	return string(buf)
}

func unJsonThis(s string) JsonObject {
	var obj JsonObject
	json.Unmarshal([]byte(s), &obj)
	return obj
}

type messagesByCount []messageCount

type messageCount struct {
	message string
	count   int
}

func (a messagesByCount) Len() int      { return len(a) }
func (a messagesByCount) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a messagesByCount) Less(i, j int) bool {
	return a[i].count > a[j].count
}

func countDistinct(messages []JsonObject) {
	counts := make(map[string]int)
	for _, message := range messages {
		delete(message, "@timestamp")
		counts[jsonThis(message)] += 1
	}
	messageCounts := make([]messageCount, 0, len(messages))
	for jsonS, count := range counts {
		messageCounts = append(messageCounts, messageCount{jsonS, count})
	}
	sort.Sort(messagesByCount(messageCounts))

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Message", "Count"})
	for _, messageCount := range messageCounts {
		table.Append([]string{messageCount.message, fmt.Sprintf("%d", messageCount.count)})
	}
	table.Render()
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

	cache := NewIndexCache(rc.Index, CacheIndices)
	indices, err := cache.Indices()
	if err != nil {
		panic(err)
	}
	allMessages := make([]JsonObject, 0, *queryMaxResults)
	for i := 0; i < len(indices) && len(allMessages) < *queryMaxResults; i++ {
		idxName := indices[i].Name
		fmt.Fprintf(os.Stderr, "Querying index %s\n", idxName)
		hits, err := queryMessages(rc, idxName, Query{
			QueryString: strings.Join(*queryString, " "),
			After:       after,
			Before:      before,
			Filters:     buildFilters(*queryWhere),
			MaxResults:  *queryMaxResults,
			ResultsDesc: *querySortDesc,
		})
		if err != nil {
			panic(err)
		}

		for _, hit := range hits {
			var message map[string]interface{}
			if len(*querySelect) > 0 {
				message = project(hit.Source, *querySelect)
				message["@timestamp"] = hit.Source["@timestamp"]
			} else {
				message = hit.Source
			}
			allMessages = append(allMessages, message)
		}
	}
	if *queryCountDistinct {
		countDistinct(allMessages)
		return
	}
	for _, message := range allMessages {
		switch *queryOutputFormat {
		case "text":
			ts := message["@timestamp"]
			fmt.Printf("[%s] ", color.MagentaString(ts.(string)))
			delete(message, "@timestamp")
			msg, hasMessage := message["message"]
			if hasMessage {
				messageColor := color.New(color.Bold)
				fmt.Printf("%s ", messageColor.Sprint(msg))
				delete(message, "message")
			}
			for key, value := range message {
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
				continue
			}
		case "yaml":
			buf, err := yaml.Marshal(message)
			if err != nil {
				fmt.Println("Error YAML encoding")
				continue
			}
			fmt.Printf("---\n%s", string(buf))
		}
	}
}
