package gossipdb

import (
	context "golang.org/x/net/context"

	api "github.com/docker/libnetwork/components/api/networkdb"
)

func (s *Server) JoinGroup(ctx context.Context, group *api.GroupID) (*api.Result, error) {
	err := s.Database.JoinNetwork(group.GetGroupName())

	return &api.Result{}, err
}

func (s *Server) LeaveGroup(ctx context.Context, group *api.GroupID) (*api.Result, error) {
	err := s.Database.LeaveNetwork(group.GetGroupName())

	return &api.Result{}, err
}

func (s *Server) PeersGroup(ctx context.Context, group *api.GroupID) (*api.PeerList, error) {
	peers := s.Database.Peers(group.GetGroupName())

	res := &api.PeerList{}
	for _, peer := range peers {
		res.Peers = append(res.Peers, &api.Peer{Name: peer.Name, Ip: peer.IP})
	}
	return res, nil
}
