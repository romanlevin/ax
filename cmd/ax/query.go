package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/fatih/color"
	"github.com/zefhemel/ax/pkg/backend/common"
	"github.com/zefhemel/ax/pkg/backend/kibana"
	yaml "gopkg.in/yaml.v2"
)

var (
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

func buildFilters(wheres []string) []common.QueryFilter {
	filters := make([]common.QueryFilter, 0, len(wheres))
	for _, whereClause := range wheres {
		pieces := strings.SplitN(whereClause, ":", 2)
		if len(pieces) != 2 {
			fmt.Println("Invalid where clause", whereClause)
			os.Exit(1)
		}
		filters = append(filters, common.QueryFilter{
			FieldName: pieces[0],
			Value:     pieces[1],
		})
	}
	return filters
}
func queryMain(client *kibana.Client) {
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

	//cache := NewIndexCache(rc, CacheIndices)
	client.Query(common.Query{
		QueryString:  strings.Join(*queryString, " "),
		Before:       before,
		After:        after,
		Filters:      buildFilters(*queryWhere),
		MaxResults:   *queryMaxResults,
		ResultsDesc:  *querySortDesc,
		SelectFields: *querySelect,
		Follow:       *queryFollow,
	}, func(message common.JsonObject) {
		printMessage(message, *queryOutputFormat)
	})

}

func printMessage(message common.JsonObject, queryOutputFormat string) {
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

func jsonThis(obj interface{}) string {
	buf, err := json.Marshal(obj)
	if err != nil {
		return "<<JSON ENCODE ERROR>>"
	}
	return string(buf)
}
