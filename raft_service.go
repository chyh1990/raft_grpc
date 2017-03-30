package main

import (
	"log"
	"net"
	raft "test_raft/raft"

	"google.golang.org/grpc"

	context "golang.org/x/net/context"
)

type RaftServiceImpl struct {
	cluster *RaftCluster
}

func (s *RaftServiceImpl) AppendEntries(ctx context.Context,
	request *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	return s.cluster.onAppendRequest(request), nil
}

func (s *RaftServiceImpl) RequestVote(ctx context.Context,
	request *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	return s.cluster.onVoteRequest(request), nil
}

func (s *RaftServiceImpl) Run() {
	addr := s.cluster.my.config.GetAddress()
	log.Printf("listening: %+v\n", addr)
	lis, err := net.Listen("tcp", addr)
	checkError(err)
	grpcServer := grpc.NewServer()
	raft.RegisterRaftServiceServer(grpcServer, s)

	grpcServer.Serve(lis)
}
