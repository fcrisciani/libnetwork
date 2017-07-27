package gossipdb

import (
	context "golang.org/x/net/context"

	api "github.com/docker/libnetwork/components/api/networkdb"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
)

func (s *Server) Ready(context.Context, *google_protobuf.Empty) (*api.Result, error) {
	return &api.Result{Status: api.OperationResult_SUCCESS}, nil
}
