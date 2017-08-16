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
	envName := "test-*"
	os.Remove(fmt.Sprintf("%s/%s.index_cache.json", DataDir, safeFilename(envName)))
	cache := NewIndexCache(envName, dummyLookup)
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
	err = cache.Flush()
	if err != nil {
		t.Fatal("Could not flush cache successfully")
	}

	cache2 := NewIndexCache(envName, dummyLookup)
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
