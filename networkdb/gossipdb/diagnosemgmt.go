package gossipdb

import (
	"fmt"
	"time"

	context "golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	api "github.com/docker/libnetwork/components/api/networkdb"
	google_protobuf "github.com/golang/protobuf/ptypes/empty"
)

func (s *Server) Ready(context.Context, *google_protobuf.Empty) (*api.Result, error) {
	return &api.Result{Status: api.OperationResult_SUCCESS}, fmt.Errorf("test error value ")
}

func (s *Server) WatchTest(null *google_protobuf.Empty, stream api.DiagnoseManagement_WatchTestServer) error {
	for {
		if err := stream.Send(&api.Result{Status: api.OperationResult_SUCCESS}); err != nil {
			logrus.Errorf("the stream returned the error:%s", err)
			return err
		}
		time.Sleep(1 * time.Second)
	}
}
