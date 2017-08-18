package main

import (
	"fmt"
	"os"
	"testing"
)

func dummyLookup() ([]IndexMeta, error) {
	return []IndexMeta{
		{Name: "index1"},
		{Name: "index2"},
	}, nil
}
func TestCache(t *testing.T) {
	rc := RuntimeConfig{
		EnvironmentConfig: EnvironmentConfig{
			URL:   "http://localhost",
			Index: "test-*",
		},
	}
	os.Remove(indexCachePathFilename(rc))
	cache := NewIndexCache(rc, dummyLookup)
	if cache.HasCache() {
		t.Fatal("Already has cached indices")
	}
	indices, err := cache.Indices()
	if err != nil {
		t.Fatal("Fetching indices failed")
	}
	if indices == nil {
		t.Fatal("Should return env cache")
	}
	err = cache.flush()
	if err != nil {
		t.Fatal("Could not flush cache successfully")
	}

	cache2 := NewIndexCache(rc, dummyLookup)
	if err != nil {
		fmt.Println(err)
		t.Fatal("Could not open cache second time")
	}
	if !cache2.HasCache() {
		t.Fatal("Did not persist all indices")
	}
	indices, err = cache2.Indices()
	if err != nil {
		t.Fatal("Fetching indices failed 2")
	}
	if indices[0].Name != "index1" {
		t.Fatal("Did not persist index name properly")
	}
}
