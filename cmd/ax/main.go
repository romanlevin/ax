package main

import (
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/zefhemel/ax/pkg/backend/kibana"
)

var (
	queryCommand = kingpin.Command("query", "Query Kibana")
	//listIndexCommand = kingpin.Command("list-index", "List all indices")
)

func main() {
	rc := BuildConfig()
	client := kibana.New(rc.URL, rc.AuthHeader, rc.Index)
	switch rc.Command {
	case "query":
		queryMain(client)
		//case "list-index":
		//	listIndicesMain(client, rc)

	}

}

/*
func listIndicesMain(client *kibana.Client, rc RuntimeConfig) {
	indices, err := client.listIndices()
	if err != nil {
		panic(err)
	}
	for _, indexName := range indices {
		fmt.Println(indexName)
	}
}
*/
