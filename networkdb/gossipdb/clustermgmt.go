package gossipdb

import (
	context "golang.org/x/net/context"

	api "github.com/docker/libnetwork/components/api/networkdb"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
)

// StateCluster rpc impl
func (s *Server) StateCluster(ctx context.Context, null *google_protobuf.Empty) (*api.ClusterState, error) {
	// TODO implement it
	return &api.ClusterState{Status: api.State_HEALTHY}, nil
}

// JoinCluster rpc impl
func (s *Server) JoinCluster(ctx context.Context, req *api.PeerList) (*api.Result, error) {
	logrus.Infof("Received the JoinCluster with %v", req.GetPeers())
	members := make([]string, 0, len(req.GetPeers()))
	for _, peer := range req.GetPeers() {
		members = append(members, peer.Ip)
	}
	ret := &api.Result{Status: api.OperationResult_SUCCESS}
	err := s.Database.Join(members)
	if err != nil {
		ret.Status = api.OperationResult_FAIL
	}

	return ret, err
}

// LeaveCluster rpc impl
func (s *Server) LeaveCluster(ctx context.Context, null *google_protobuf.Empty) (*api.Result, error) {
	s.Database.Close()
	return &api.Result{Status: api.OperationResult_SUCCESS}, nil
}

// PeersCluster rpc impl
func (s *Server) PeersCluster(ctx context.Context, null *google_protobuf.Empty) (*api.PeerList, error) {
	peers := s.Database.ClusterPeers()
	res := &api.PeerList{}
	res.Peers = make([]*api.Peer, 0, len(peers))
	for _, peer := range peers {
		res.Peers = append(res.Peers, &api.Peer{Name: peer.Name, Ip: peer.IP})
	}
	return res, nil
}
