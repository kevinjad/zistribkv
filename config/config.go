package config

import (
	"io/ioutil"
	"log"

	"github.com/BurntSushi/toml"
)

// Shard describes a shard that holds the appropriate set of keys.
// Each shard has unique set of keys.
type Shard struct {
	Name    string
	Index   int
	Address string
}

// Config describes the sharding config.
type Config struct {
	Shards []Shard
}

func CreateContext(configFile string, shard string) *Context {
	data, err := ioutil.ReadFile(configFile)
	log.Println(string(data))
	if err != nil {
		log.Fatalf("error reading config file (%q) %v", configFile, err.Error())
	}
	var shards Config
	err = toml.Unmarshal(data, &shards)
	if err != nil {
		log.Fatalf("error parsing config file (%q) %v", configFile, err.Error())
	}
	log.Println(shards)

	var shardIdx = -1
	shardCount := len(shards.Shards)
	log.Printf("shard count is %d\n", shardCount)
	for _, s := range shards.Shards {
		if s.Name == shard {
			shardIdx = s.Index
		}
	}
	if shardIdx < 0 {
		log.Fatalf("could not find shard %q", shard)
	}
	log.Printf("The shard count is: %d and shardIdx is: %d", shardCount, shardIdx)
	return &Context{ShardIdx: shardIdx, ShardCount: shardCount, Conf: shards}
}
