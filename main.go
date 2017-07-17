package main

import (
	"fmt"
	"time"
)

func main() {
	now := time.Now()
	threeDaysAgo := now.Add(-3 * 24 * time.Hour)
	indices, err := queryIndexes("turbo-*", threeDaysAgo, now)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", indices)
	hits, err := queryMessages(indices[0].Name, Query{
		QueryString: "*",
		Filters: []QueryFilter{
			{
				FieldName: "egnyte_domain",
				Value:     "weigandconstruction",
			},
		},
		After:      threeDaysAgo,
		Before:     now,
		MaxResults: 20,
	})
	if err != nil {
		panic(err)
	}
	for _, hit := range hits {
		fmt.Printf("[%s] %s\n", hit.Source["@timestamp"], hit.Source["message"])
		// fmt.Println(hit.Source)
	}
}
