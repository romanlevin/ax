package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"time"

	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const IndexCacheExpiry time.Duration = 4 * time.Hour

var DataDir string

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
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/es_admin/.kibana/index-pattern/_search?stored_fields=", rc.URL), body)
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
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/elasticsearch/%s/_field_stats?level=indices", rc.URL, rc.Index), body)
	if err != nil {
		return nil, err
	}
	addHeaders(rc, req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		buf, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.New(string(buf))
	}
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

type FetchIndicesFunc func() ([]IndexMeta, error)

type IndexCache struct {
	path             string
	fetchIndicesFunc FetchIndicesFunc
	CacheDate        time.Time
	IndexData        []IndexMeta
}

func indexCachePathFilename(rc RuntimeConfig) string {
	return fmt.Sprintf("%s/%s.index_cache.json", DataDir, safeFilename(fmt.Sprintf("%s_%s", rc.URL, rc.Index)))
}

func NewIndexCache(rc RuntimeConfig, fetchIndicesFunc FetchIndicesFunc) *IndexCache {
	cache := &IndexCache{
		path:             indexCachePathFilename(rc),
		fetchIndicesFunc: fetchIndicesFunc,
	}
	file, err := os.Open(cache.path)
	if err == nil {
		defer file.Close()
		decoder := json.NewDecoder(file)
		err = decoder.Decode(cache)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding: %v\n", err)
		}
	}
	return cache
}

func (cache *IndexCache) HasCache() bool {
	if cache.IndexData == nil {
		return false
	}
	return cache.CacheDate.Add(IndexCacheExpiry).After(time.Now())
}

func (cache *IndexCache) Indices() ([]IndexMeta, error) {
	if cache.HasCache() {
		return cache.IndexData, nil
	}
	fmt.Fprint(os.Stderr, "Fetching indices...\n")
	indices, err := cache.fetchIndicesFunc()
	if err != nil {
		return nil, err
	}
	cache.CacheDate = time.Now()
	cache.IndexData = indices
	err = cache.flush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Caching indices failed: %v\n", err)
	}
	return indices, nil
}

func (cache *IndexCache) flush() error {
	file, err := os.OpenFile(cache.path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.Encode(cache)
	return nil
}

func init() {
	DataDir = fmt.Sprintf("%s/.config/clibana", os.Getenv("HOME"))
	err := os.MkdirAll(DataDir, 0700)
	if err != nil {
		fmt.Println("Could not create", DataDir)
		os.Exit(1)
	}
}
