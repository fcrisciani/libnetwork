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
