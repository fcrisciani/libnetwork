package gossipdb

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	rpc "github.com/docker/libnetwork/components/api/networkdb"
	"github.com/docker/libnetwork/networkdb"
)

type Server struct {
	Srv      *grpc.Server
	Database *networkdb.NetworkDB
}

func (s *Server) Init() {
	s.Srv = grpc.NewServer()
	// Register the configuration manager service
	rpc.RegisterConfigurationManagementServer(s.Srv, s)
}

func (s *Server) StartServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 5000))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s.Srv.Serve(lis)
}
