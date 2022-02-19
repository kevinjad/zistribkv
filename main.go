package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/kevinjad/zistribkv/config"
	"github.com/kevinjad/zistribkv/db"
	"github.com/kevinjad/zistribkv/web"
)

var (
	dbLocation    = flag.String("db-location", "", "filesystem path for the database to sit")
	serverAddress = flag.String("server-address", "localhost:8080", "server address")
	configFile    = flag.String("config-file", "shards.toml", "config file for static sharding")
	shard         = flag.String("shard", "", "shard name for data storage")
)

func parseFlags() {
	flag.Parse()
	if *dbLocation == "" {
		log.Fatalf("invalid path for database\n")
	}
	if *serverAddress == "" {
		log.Fatal("invalid server address\n")
	}
	if *shard == "" {
		log.Fatal("invalid shard name\n")
	}
}

func main() {
	parseFlags()
	ctx := config.CreateContext(*configFile, *shard)
	database, err := db.NewDatabase(*dbLocation)
	if err != nil {
		log.Fatalf("could not instatiate database %v", err)
	}
	defer db.Close(*database)
	handler := web.NewHandler(database, ctx)

	http.HandleFunc("/set", handler.Set)

	http.HandleFunc("/get", handler.Get)

	log.Fatal(http.ListenAndServe(*serverAddress, nil))
}
