package main

import (
	"github.com/Sirupsen/logrus"
	"github.com/docker/libnetwork/networkdb/gossipdb"
)

func main() {
	s := &gossipdb.Server{}
	logrus.SetLevel(logrus.DebugLevel)
	s.Init()
	s.StartServer()
}
