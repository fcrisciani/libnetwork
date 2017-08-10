package gossipdb

import (
	context "golang.org/x/net/context"

	api "github.com/docker/libnetwork/components/api/networkdb"
	"github.com/docker/libnetwork/networkdb"
	"github.com/sirupsen/logrus"
)

// Initialize rpc impl
func (s *Server) Initialize(ctx context.Context, config *api.Configuration) (*api.Result, error) {
	logrus.Infof("Received the Initialize call")
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
		api.RegisterDiagnoseManagementServer(s.Srv, s)
	}
	logrus.Infof("Ending the Initialize call err:%s", err)
	return &api.Result{Status: api.OperationResult_SUCCESS}, err
}
