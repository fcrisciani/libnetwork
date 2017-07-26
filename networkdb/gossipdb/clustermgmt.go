package gossipdb

import (
	context "golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	api "github.com/docker/libnetwork/components/api/networkdb"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
)

func (s *Server) JoinCluster(ctx context.Context, req *api.JoinClusterReq) (*api.Result, error) {
	logrus.Infof("Received the JoinCluster with %v", req.GetMembers())
	err := s.Database.Join(req.GetMembers())
	return &api.Result{}, err
}

func (s *Server) PeersCluster(ctx context.Context, null *google_protobuf.Empty) (*api.PeerList, error) {
	peers := s.Database.ClusterPeers()
	res := &api.PeerList{}
	for _, peer := range peers {
		res.Peers = append(res.Peers, &api.Peer{Name: peer.Name, Ip: peer.IP})
	}
	return res, nil
}
