package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type FetchIndicesFunc func() ([]IndexMeta, error)

type IndexCache struct {
	path             string
	fetchIndicesFunc FetchIndicesFunc
	CacheDate        time.Time
	IndexData        []IndexMeta
}

const IndexCacheExpiry time.Duration = 4 * time.Hour

func NewIndexCache(indexName string, fetchIndicesFunc FetchIndicesFunc) *IndexCache {
	cache := &IndexCache{
		path:             fmt.Sprintf("%s/%s.index_cache.json", DataDir, safeFilename(indexName)),
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
	if cache.Indices == nil {
		return false
	}
	if cache.CacheDate.Add(IndexCacheExpiry).After(time.Now()) {
		return true
	} else {
		return false
	}
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
	err = cache.Flush()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Caching indices failed: %v\n", err)
	}
	return indices, nil
}

func (cache *IndexCache) Flush() error {
	file, err := os.OpenFile(cache.path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.Encode(cache)
	return nil
}

var DataDir string

func init() {
	DataDir = fmt.Sprintf("%s/.config/clibana", os.Getenv("HOME"))
	err := os.MkdirAll(DataDir, 0700)
	if err != nil {
		fmt.Println("Could not create", DataDir)
		os.Exit(1)
	}
}
