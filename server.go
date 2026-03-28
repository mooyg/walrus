package walrus

import (
	"fmt"
	"net"

	"github.com/mooyg/walrus/internal/broker"
	walrusgrpc "github.com/mooyg/walrus/internal/grpc"
	logger "github.com/mooyg/walrus/internal/log"
	proto "github.com/mooyg/walrus/proto"
	"google.golang.org/grpc"
)

type Config struct {
	Port     int
	DataDir  string
	LogLevel string
}

type Server struct {
	cfg        Config
	broker     *broker.Broker
	grpcServer *grpc.Server
}

func NewServer(cfg Config) (*Server, error) {
	if cfg.Port == 0 {
		cfg.Port = 9092
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}

	logger.Init(cfg.LogLevel)

	b, err := broker.NewBroker(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker: %w", err)
	}

	gs := grpc.NewServer()
	svc := walrusgrpc.NewServer(b)
	proto.RegisterBrokerServiceServer(gs, svc)

	return &Server{
		cfg:        cfg,
		broker:     b,
		grpcServer: gs,
	}, nil
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.Port))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.cfg.Port, err)
	}

	logger.Info(fmt.Sprintf("walrus server listening on :%d", s.cfg.Port), nil)
	return s.grpcServer.Serve(lis)
}

func (s *Server) Stop() error {
	s.grpcServer.GracefulStop()
	return s.broker.Close()
}
