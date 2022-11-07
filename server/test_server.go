package server

import (
	"context"
	"net"
	"net/http/httptest"

	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type TestServer struct {
	*httptest.Server
	DialerOption grpc.DialOption
}

func (s *TestServer) GRPCDial(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(
		ctx,
		s.URL,
		s.DialerOption,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func (s *TestServer) GRPCClientOptions(ctx context.Context) ([]option.ClientOption, error) {
	conn, err := s.GRPCDial(ctx)
	if err != nil {
		return nil, err
	}
	return []option.ClientOption{
		option.WithGRPCConn(conn),
		option.WithEndpoint(s.URL),
		option.WithoutAuthentication(),
	}, nil
}

func (s *Server) TestServer() *TestServer {
	server := httptest.NewServer(s.Handler)
	s.httpServer = server.Config

	grpcListener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	registerStorageServer(grpcServer, s)
	s.grpcServer = grpcServer
	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil {
			panic(err)
		}
	}()
	testServer := &TestServer{Server: server}
	testServer.DialerOption = grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return grpcListener.Dial()
	})
	return testServer
}
