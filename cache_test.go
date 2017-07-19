package main

import (
	"fmt"
	"os"
	"testing"
)

func dummyLookup() []IndexMeta {
	return []IndexMeta{
		{Name: "index1"},
		{Name: "index2"},
	}
}
func TestCache(t *testing.T) {
	envName := "testenv"
	os.Remove(fmt.Sprintf("%s/%s.cache", DataDir, envName))
	cache, err := NewEnvironmentCache(envName, dummyLookup)
	if err != nil {
		t.Fatal("Could not open cache")
	}
	if cache.HasCachedIndexes() {
		t.Fatal("Already has cached indexes")
	}
	indexes := cache.Indexes()
	if indexes == nil {
		t.Fatal("Should return env cache")
	}
	err = cache.Flush()
	if err != nil {
		t.Fatal("Could not flush cache successfully")
	}

	cache2, err := NewEnvironmentCache(envName, dummyLookup)
	if err != nil {
		fmt.Println(err)
		t.Fatal("Could not open cache second time")
	}
	if !cache2.HasCachedIndexes() {
		t.Fatal("Did not persist all indexes")
	}
	if cache2.Indexes()[0].Name != "index1" {
		t.Fatal("Did not persist index name properly")
	}
}
