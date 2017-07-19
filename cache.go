package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

type FetchIndexesFunc func() []IndexMeta

type EnvironmentCache struct {
	path             string
	expiry           time.Duration
	fetchIndexesFunc FetchIndexesFunc
	CacheDate        time.Time
	IndexData        []IndexMeta
}

func NewEnvironmentCache(name string, fetchIndexesFunc FetchIndexesFunc) (cache *EnvironmentCache, err error) {
	cache = &EnvironmentCache{
		path:             fmt.Sprintf("%s/%s.cache", DataDir, name),
		expiry:           time.Hour * 24,
		fetchIndexesFunc: fetchIndexesFunc,
	}
	file, err := os.Open(cache.path)
	if err != nil && !os.IsNotExist(err) {
		return
	} else if err == nil {
		defer file.Close()
		decoder := gob.NewDecoder(file)
		err = decoder.Decode(cache)
		if err != nil {
			return
		}
		return
	} else { // No such file
		err = nil
		return
	}
}

func (cache *EnvironmentCache) HasCachedIndexes() bool {
	if cache.Indexes == nil {
		return false
	}
	if cache.CacheDate.Add(cache.expiry).After(time.Now()) {
		return true
	} else {
		return false
	}
}

func (cache *EnvironmentCache) Indexes() []IndexMeta {
	if cache.HasCachedIndexes() {
		return cache.IndexData
	}
	indexes := cache.fetchIndexesFunc()
	if indexes == nil {
		return nil
	}
	cache.CacheDate = time.Now()
	cache.IndexData = indexes
	return indexes
}

func (cache *EnvironmentCache) Flush() error {
	file, err := os.OpenFile(cache.path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := gob.NewEncoder(file)
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
