package main

import (
	"flag"
	"raft/part3/server/ConsensusModule"
)

var address = flag.Int("id", 0, "input peer Id")

func main() {
	flag.Parse()
	server := ConsensusModule.NewServer()
	server.Serve(*address)
}
