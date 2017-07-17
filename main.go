package main

import (
	"fmt"
	"os"
	"time"

	"github.com/araddon/dateparse"
	"github.com/spf13/viper"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	EsUrl           = kingpin.Flag("url", "Kibana/ElasticSearch URL to connect to").Default("https://kibana-prod.egnyte-internal.com/elasticsearch").String()
	IndexName       = kingpin.Flag("index", "Index prefix to query").Default("turbo-*").String()
	query           = kingpin.Command("query", "Query Kibana")
	queryFilters    = query.Flag("filter", "Filter the result").Strings()
	queryBefore     = query.Flag("before", "Results from before").String()
	queryAfter      = query.Flag("after", "Results from after").String()
	queryMaxResults = query.Flag("results", "Maximum number of results").Default("200").Int()
	queryString     = query.Arg("query", "Query string").Default("*").String()
	querySortAsc    = query.Flag("asc", "Sort results chronologically").Default("true").Bool()
	AuthHeader      string
)

func readConfig() {
	viper.SetConfigName("terbana") // name of config file (without extension)
	viper.AddConfigPath("$HOME")   // call multiple times to add many search paths
	viper.AddConfigPath(".")       // optionally look for config in the working directory
	err := viper.ReadInConfig()    // Find and read the config file
	if err != nil {                // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	if !viper.IsSet("auth-header") {
		panic("auth-header not set in terbana.json")
	}
	AuthHeader = viper.GetString("auth-header")
}

func main() {
	readConfig()
	switch kingpin.Parse() {
	case "query":
		before := time.Now()
		after := before.Add(-3 * 24 * time.Hour)
		if *queryAfter != "" {
			var err error
			after, err = dateparse.ParseAny(*queryAfter)
			if err != nil {
				fmt.Println("Could parse after date:", *queryAfter)
				os.Exit(1)
			}
		}
		if *queryBefore != "" {
			var err error
			before, err = dateparse.ParseAny(*queryBefore)
			if err != nil {
				fmt.Println("Could parse before date:", *queryBefore)
				os.Exit(1)
			}
		}

		indices, err := queryIndexes(*IndexName, after, before)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", indices)
		hits, err := queryMessages(indices[0].Name, Query{
			QueryString: *queryString,
			After:       after,
			Before:      before,
			MaxResults:  *queryMaxResults,
			ResultsAsc:  *querySortAsc,
		})
		if err != nil {
			panic(err)
		}
		for _, hit := range hits {
			fmt.Printf("[%s] %s\n", hit.Source["@timestamp"], hit.Source["message"])
			// fmt.Println(hit.Source)
		}
	}

}
