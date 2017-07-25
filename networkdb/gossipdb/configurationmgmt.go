package gossipdb

import (
	context "golang.org/x/net/context"

	api "github.com/docker/libnetwork/components/api/networkdb"
	"github.com/docker/libnetwork/networkdb"
)

func (s *Server) Initialize(ctx context.Context, config *api.Configuration) (*api.Result, error) {
	netDBConf := networkdb.DefaultConfig()
	netDBConf.NodeName = config.GetNodeName()
	netDBConf.BindAddr = config.GetBindAddr()
	netDBConf.Keys = config.GetKeys()

	nDB, err := networkdb.New(netDBConf)
	s.Database = nDB
	if err == nil {
		api.RegisterClusterManagementServer(s.Srv, s)
		api.RegisterGroupManagementServer(s.Srv, s)
		api.RegisterEntryManagementServer(s.Srv, s)
	}

	return &api.Result{}, err
}
