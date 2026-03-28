package grpc

import (
	"context"

	"github.com/mooyg/walrus/internal/broker"
	proto "github.com/mooyg/walrus/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	proto.UnimplementedBrokerServiceServer
	broker *broker.Broker
}

func NewServer(b *broker.Broker) *Server {
	return &Server{broker: b}
}

func (s *Server) Produce(_ context.Context, req *proto.ProduceRequest) (*proto.ProduceResponse, error) {
	offset, err := s.broker.Produce(req.Topic, req.Data)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "produce: %v", err)
	}
	return &proto.ProduceResponse{Offset: offset}, nil
}

func (s *Server) Fetch(ctx context.Context, req *proto.FetchRequest) (*proto.FetchResponse, error) {
	msgs, _, err := s.broker.Consume(ctx, req.Topic, req.Offset, int(req.Limit))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "fetch: %v", err)
	}

	records := make([]*proto.Record, len(msgs))
	for i, m := range msgs {
		records[i] = &proto.Record{Offset: m.Offset, Data: m.Data}
	}

	var lastOffset int64
	if len(msgs) > 0 {
		lastOffset = msgs[len(msgs)-1].Offset
	}

	return &proto.FetchResponse{Records: records, LastOffset: lastOffset}, nil
}

func (s *Server) CommitOffset(_ context.Context, req *proto.CommitOffsetRequest) (*proto.CommitOffsetResponse, error) {
	if err := s.broker.CommitOffset(req.ConsumerId, req.Topic, req.Offset); err != nil {
		return nil, status.Errorf(codes.Internal, "commit offset: %v", err)
	}
	return &proto.CommitOffsetResponse{}, nil
}
