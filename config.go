package main

import (
	"fmt"
	"os"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/BurntSushi/toml"
)

type Config struct {
	ActiveEnv    string                       `toml:"activeenv"`
	Url          string                       `toml:"url"`
	AuthHeader   string                       `toml:"authheader"`
	Environments map[string]EnvironmentConfig `toml:"env"`
}

type EnvironmentConfig struct {
	Url        string `toml:"url"`
	AuthHeader string `toml:"authheader"`
	Index      string `toml:"index"`
}

type RuntimeConfig struct {
	EnvironmentConfig
	Command string
}

func LoadConfig() Config {
	var config Config
	_, err := toml.DecodeFile(fmt.Sprintf("%s/clibana.toml", DataDir), &config)
	if err != nil {
		panic(err)
	}
	return config
}

func BuildConfig() RuntimeConfig {
	config := LoadConfig()
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
	if rc.Url == "" {
		rc.Url = config.Url
	}
	if rc.AuthHeader == "" {
		rc.AuthHeader = config.AuthHeader
	}

	// Arg overrides
	if *url != "" {
		rc.Url = *url
	}
	if *indexName != "" {
		rc.Index = *indexName
	}

	//fmt.Printf("RuntimeConfig: %+v\n", rc)

	return rc
}
