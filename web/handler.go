package web

import (
	"fmt"
	"github.com/kevinjad/zistribkv/config"
	"hash/fnv"
	"io"
	"net/http"

	"github.com/kevinjad/zistribkv/db"
)

type Handler struct {
	database *db.Database
	ctx      *config.Context
}

func NewHandler(database *db.Database, ctx *config.Context) *Handler {
	return &Handler{database: database, ctx: ctx}
}

func (h *Handler) reroute(rw http.ResponseWriter, r *http.Request, shard int) {
	url := "http://" + h.ctx.Conf.Shards[shard].Address + r.RequestURI
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(rw, "set unsuccessful")
	}
	io.Copy(rw, resp.Body)
	resp.Body.Close()
}

func (h *Handler) Set(rw http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")
	shard := h.findShard(key)
	if shard == h.ctx.ShardIdx {
		err := h.database.Set(key, []byte(value))
		if err != nil {
			fmt.Fprintf(rw, "Oops! Error: %v", err.Error())
		}
		fmt.Fprintf(rw, "set successful")
	} else {
		//fmt.Fprintf(rw, "Not my shard, sits in: %d", shard)
		h.reroute(rw, r, shard)
	}
}

func (h *Handler) Get(rw http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	shard := h.findShard(key)
	if shard == h.ctx.ShardIdx {
		value, err := h.database.Get(key)
		if err != nil {
			fmt.Fprintf(rw, "Oops! Error: %v", err.Error())
		}
		fmt.Fprintf(rw, "get successful value: %s", string(value))
	} else {
		//fmt.Fprintf(rw, "Not my shard, sits in: %d", shard)
		h.reroute(rw, r, shard)
	}
}

func (h *Handler) findShard(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	num := hash.Sum32()
	return int(num % uint32(h.ctx.ShardCount))
}
