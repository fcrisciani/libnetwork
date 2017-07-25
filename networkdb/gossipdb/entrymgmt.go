package gossipdb

import (
	context "golang.org/x/net/context"

	api "github.com/docker/libnetwork/components/api/networkdb"
)

func (s *Server) CreateEntryRpc(ctx context.Context, entry *api.Entry) (*api.Result, error) {
	err := s.Database.CreateEntry(entry.GetTableName(), entry.GetGroup().GetGroupName(), entry.GetEntryName(), entry.GetValue())
	return &api.Result{}, err
}
func (s *Server) ReadEntryRpc(ctx context.Context, entry *api.Entry) (*api.Result, error) {
	// err := s.Database.CreateEntry(entry.GetTableName(), entry.GetGroup().GetGroupName(), entry.GetEntryName(), entry.GetValue())
	return &api.Result{}, nil
}
func (s *Server) UpdateEntryRpc(ctx context.Context, entry *api.Entry) (*api.Result, error) {
	err := s.Database.UpdateEntry(entry.GetTableName(), entry.GetGroup().GetGroupName(), entry.GetEntryName(), entry.GetValue())
	return &api.Result{}, err
}
func (s *Server) DeleteEntryRpc(ctx context.Context, entry *api.Entry) (*api.Result, error) {
	err := s.Database.DeleteEntry(entry.GetTableName(), entry.GetGroup().GetGroupName(), entry.GetEntryName())
	return &api.Result{}, err
}
