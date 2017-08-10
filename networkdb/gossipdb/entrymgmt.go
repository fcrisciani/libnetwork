package gossipdb

import (
	context "golang.org/x/net/context"

	api "github.com/docker/libnetwork/components/api/networkdb"
	"github.com/docker/libnetwork/networkdb"
)

// CreateEntryRpc rpc impl
func (s *Server) CreateEntryRpc(ctx context.Context, entry *api.TableEntry) (*api.Result, error) {
	err := s.Database.CreateEntry(entry.GetTable().GetTableName(), entry.GetTable().GetGroup().GetGroupName(),
		entry.GetEntry().GetKey(), entry.GetEntry().GetValue())
	return &api.Result{Status: api.OperationResult_SUCCESS}, err
}

// ReadEntryRpc rpc impl
func (s *Server) ReadEntryRpc(ctx context.Context, entry *api.TableEntry) (*api.TableEntry, error) {
	fetched, err := s.Database.GetEntry(entry.GetTable().GetTableName(), entry.GetTable().GetGroup().GetGroupName(), entry.GetEntry().GetKey())
	entry.GetEntry().Value = fetched
	return &api.TableEntry{Table: entry.GetTable(), Entry: entry.GetEntry()}, err
}

// UpdateEntryRpc rpc impl
func (s *Server) UpdateEntryRpc(ctx context.Context, entry *api.TableEntry) (*api.Result, error) {
	err := s.Database.UpdateEntry(entry.GetTable().GetTableName(), entry.GetTable().GetGroup().GetGroupName(),
		entry.GetEntry().GetKey(), entry.GetEntry().GetValue())
	return &api.Result{Status: api.OperationResult_SUCCESS}, err
}

// DeleteEntryRpc rpc impl
func (s *Server) DeleteEntryRpc(ctx context.Context, entry *api.TableEntry) (*api.Result, error) {
	err := s.Database.DeleteEntry(entry.GetTable().GetTableName(), entry.GetTable().GetGroup().GetGroupName(),
		entry.GetEntry().GetKey())
	return &api.Result{Status: api.OperationResult_SUCCESS}, err
}

// Table operations
// ReadTable rpc impl
func (s *Server) ReadTable(ctx context.Context, table *api.Table) (*api.EntryList, error) {
	entries := s.Database.GetTableByNetwork(table.GetTableName(), table.GetGroup().GetGroupName())
	list := make([]*api.Entry, 0, len(entries))
	for k, v := range entries {
		list = append(list, &api.Entry{Key: k, Value: v.([]byte)})
	}
	return &api.EntryList{Table: table, List: list}, nil
}

// WatchTable rpc impl
func (s *Server) WatchTable(table *api.Table, stream api.EntryManagement_WatchTableServer) error {
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
				// the stream had an issue
				return err
			}
		case <-ch.Done():
			// Close the stream
			return nil
		}
	}
}
