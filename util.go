package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"time"
)

func createMultiSearch(objs ...interface{}) (io.Reader, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, obj := range objs {
		err := encoder.Encode(obj)
		if err != nil {
			return nil, err
		}
	}
	//fmt.Println(buf.String())
	return &buf, nil
}

func safeFilename(name string) string {
	re := regexp.MustCompile(`[^\w\-]`)
	return re.ReplaceAllString(name, "_")
}

type JsonObject map[string]interface{}
type JsonList []interface{}

func addHeaders(rc RuntimeConfig, req *http.Request) {
	req.Header.Set("Authorization", rc.AuthHeader)
	req.Header.Set("Kbn-Version", rc.KbnVersion)
	req.Header.Set("Content-Type", "application/x-ldjson")
	req.Header.Set("Accept", "application/json, text/plain, */*")
}

func unixMillis(t time.Time) int64 {
	return t.Unix() * 1000
}

func jsonThis(obj interface{}) string {
	buf, err := json.Marshal(obj)
	if err != nil {
		return "<<JSON ENCODE ERROR>>"
	}
	return string(buf)
}

func unJsonThis(s string) JsonObject {
	var obj JsonObject
	json.Unmarshal([]byte(s), &obj)
	return obj
}
