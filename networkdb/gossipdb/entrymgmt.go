package gossipdb

import (
	context "golang.org/x/net/context"

	api "github.com/docker/libnetwork/components/api/networkdb"
	"github.com/docker/libnetwork/networkdb"
)

func (s *Server) CreateEntryRpc(ctx context.Context, entry *api.EntryIn) (*api.Result, error) {
	err := s.Database.CreateEntry(entry.GetTable().GetTableName(), entry.GetTable().GetGroup().GetGroupName(),
		entry.GetEntry().GetKey(), entry.GetEntry().GetValue())
	return &api.Result{}, err
}
func (s *Server) ReadEntryRpc(ctx context.Context, entry *api.EntryIn) (*api.EntryIn, error) {
	// err := s.Database.CreateEntry(entry.GetTableName(), entry.GetGroup().GetGroupName(), entry.GetEntryName(), entry.GetValue())
	return &api.EntryIn{}, nil
}
func (s *Server) UpdateEntryRpc(ctx context.Context, entry *api.EntryIn) (*api.Result, error) {
	err := s.Database.UpdateEntry(entry.GetTable().GetTableName(), entry.GetTable().GetGroup().GetGroupName(),
		entry.GetEntry().GetKey(), entry.GetEntry().GetValue())
	return &api.Result{}, err
}
func (s *Server) DeleteEntryRpc(ctx context.Context, entry *api.EntryIn) (*api.Result, error) {
	err := s.Database.DeleteEntry(entry.GetTable().GetTableName(), entry.GetTable().GetGroup().GetGroupName(),
		entry.GetEntry().GetKey())
	return &api.Result{}, err
}

// Table operations
func (s *Server) ReadTable(ctx context.Context, table *api.TableID) (*api.EntryList, error) {
	entries := s.Database.GetTableByNetwork(table.GetTableName(), table.GetGroup().GetGroupName())
	list := make([]*api.Entry, 0, len(entries))
	for k, v := range entries {
		list = append(list, &api.Entry{Key: k, Value: v.([]byte)})
	}
	return &api.EntryList{Table: table, List: list}, nil
}

func (s *Server) WatchTable(table *api.TableID, stream api.EntryManagement_WatchTableServer) error {
	// ch, cancel := s.Database.Watch(table.GetTableName(), table.GetGroup().GetGroupName(), "")
	ch, _ := s.Database.Watch(table.GetTableName(), table.GetGroup().GetGroupName(), "")
	var tableEvent *api.TableEvent
	for {
		select {
		case ev := <-ch.C:
			tableEvent = &api.TableEvent{Table: table}
			switch event := ev.(type) {
			case networkdb.CreateEvent:
				tableEvent.Operation = api.TableOperation_CREATE
				tableEvent.Entry = &api.Entry{Key: event.Key, Value: event.Value}
			case networkdb.DeleteEvent:
				tableEvent.Operation = api.TableOperation_DELETE
				tableEvent.Entry = &api.Entry{Key: event.Key, Value: event.Value}
			case networkdb.UpdateEvent:
				tableEvent.Operation = api.TableOperation_UPDATE
				tableEvent.Entry = &api.Entry{Key: event.Key, Value: event.Value}
			}
			if err := stream.Send(tableEvent); err != nil {
				return err
			}
		case <-ch.Done():
			// Close the stream
			return nil
		}
	}
}
