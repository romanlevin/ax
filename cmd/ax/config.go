package main

import (
	"fmt"
	"net/http"
	"os"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/BurntSushi/toml"
)

var DataDir string

type Config struct {
	ActiveEnv    string                       `toml:"activeenv"`
	URL          string                       `toml:"url"`
	AuthHeader   string                       `toml:"authheader"`
	Environments map[string]EnvironmentConfig `toml:"env"`
}

type EnvironmentConfig struct {
	URL        string `toml:"url"`
	AuthHeader string `toml:"authheader"`
	Index      string `toml:"index"`
}

type RuntimeConfig struct {
	EnvironmentConfig
	Command    string
	KbnVersion string
}

func BuildConfig() RuntimeConfig {
	var config Config
	_, err := toml.DecodeFile(fmt.Sprintf("%s/ax.toml", DataDir), &config)
	if err != nil {
		panic(err)
	}
	rc := RuntimeConfig{}
	var (
		activeEnv = kingpin.Flag("env", "Environment to connect to").Short('e').String()
		url       = kingpin.Flag("url", "Kibana/ElasticSearch URL to connect to").String()
		indexName = kingpin.Flag("index", "Index prefix to query").String()
	)
	rc.Command = kingpin.Parse()
	var ok bool
	if config.ActiveEnv != "" {
		rc.EnvironmentConfig, ok = config.Environments[config.ActiveEnv]
		if !ok {
			fmt.Println("Undefined active environment:", config.ActiveEnv)
			os.Exit(1)
		}
	}
	if *activeEnv != "" {
		rc.EnvironmentConfig, ok = config.Environments[*activeEnv]
		if !ok {
			fmt.Println("Undefined active environment:", *activeEnv)
			os.Exit(1)
		}
	}

	// Inherit Url and AuthHeader if necessary
	if rc.URL == "" {
		rc.URL = config.URL
	}
	if rc.AuthHeader == "" {
		rc.AuthHeader = config.AuthHeader
	}

	// Arg overrides
	if *url != "" {
		rc.URL = *url
	}
	if *indexName != "" {
		rc.Index = *indexName
	}

	res, err := http.Head(fmt.Sprintf("%s/app/kibana", rc.URL))
	res.Body.Close()
	rc.KbnVersion = res.Header.Get("kbn-version")

	//fmt.Printf("RuntimeConfig: %+v\n", rc)

	return rc
}

func init() {
	DataDir = fmt.Sprintf("%s/.config/ax", os.Getenv("HOME"))
	err := os.MkdirAll(DataDir, 0700)
	if err != nil {
		fmt.Println("Could not create", DataDir)
		os.Exit(1)
	}
}
