package main

import "github.com/docker/libnetwork/networkdb/gossipdb"

func main() {
	s := &gossipdb.Server{}
	s.Init()
	s.StartServer()
}
