package main

import (
	"github.com/docker/libnetwork/networkdb/gossipdb"
	"github.com/sirupsen/logrus"
)

func main() {
	s := &gossipdb.Server{}
	logrus.SetLevel(logrus.DebugLevel)
	s.Init()
	s.StartServer()
}
