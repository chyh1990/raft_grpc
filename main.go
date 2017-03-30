package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	raft "test_raft/raft"
	"time"

	"google.golang.org/grpc"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/wal/walpb"
	proto "github.com/golang/protobuf/proto"
)

var (
	config = flag.String("config", "cluster.proto", "config file")
	nodeid = flag.Uint64("nodeid", 1, "node id")
)

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

type RaftState int

const (
	FOLLOWER  RaftState = 0
	CANDIDATE           = 1
	LEADER              = 2
)

type RaftNode struct {
	mu     sync.Mutex
	config raft.Node

	conn *grpc.ClientConn
}

func (n *RaftNode) Dial() {
	conn, err := grpc.Dial(n.config.GetAddress(), grpc.WithInsecure())
	checkError(err)
	n.conn = conn
}

func (n *RaftNode) Close() {
	if n.conn != nil {
		n.conn.Close()
	}
}

type RaftCluster struct {
	mu sync.Mutex

	my    *RaftNode
	peers map[uint64]*RaftNode

	state       RaftState
	currentTerm uint64
	votedFor    uint64
	log         []raft.LogEntry

	commitIndex uint64
	lastApplied uint64

	nextIndex  []uint64
	matchIndex []uint64

	voteResponse   chan *raft.RequestVoteResponse
	appendResponse chan *raft.AppendEntriesResponse
	votes          int

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	wal *wal.WAL
}

const HeartbeatTimeout = 1000 * time.Millisecond
const ElectionTimeoutMs = 3000

func (c *RaftCluster) setState(state RaftState) {
	oldState := c.state
	if oldState == state {
		return
	}
	c.state = state
	log.Printf("become: ", c.state)
}

func (c *RaftCluster) persistHardState() {
	h := raftpb.HardState{
		Term: c.currentTerm,
		Vote: c.votedFor,
	}
	ent := []raftpb.Entry{}
	c.wal.Save(h, ent)
}

func (c *RaftCluster) startElection() {
	if c.state == LEADER {
		return
	}
	log.Printf("start election")
	c.setState(CANDIDATE)
	c.votes = 1 // always vote to self
	c.votedFor = c.my.config.GetId()
	c.currentTerm += 1
	c.persistHardState()

	for _, v := range c.peers {
		client := raft.NewRaftServiceClient(v.conn)
		req := &raft.RequestVoteRequest{
			Term:         c.currentTerm,
			CandidateId:  c.my.config.GetId(),
			LastLogIndex: 0,
			LastLogTerm:  0,
		}
		go func() {
			r, err := client.RequestVote(context.Background(), req)
			if err != nil {
				log.Printf("Error: %+v", err)
			} else {
				c.voteResponse <- r
			}
		}()
	}
	timeout := time.Duration(ElectionTimeoutMs + rand.Intn(ElectionTimeoutMs/2))
	c.electionTimer.Reset(timeout * time.Millisecond)
}

// if returns false, skip the response
func (c *RaftCluster) checkTerm(term uint64) bool {
	if c.currentTerm < term {
		c.setState(FOLLOWER)
		c.currentTerm = term
		c.votedFor = 0
		c.persistHardState()
		c.electionTimer.Reset(ElectionTimeoutMs * time.Millisecond)
		return false
	}
	if c.currentTerm > term {
		return false
	}
	return true
}

func (c *RaftCluster) QuorumSize() int {
	return len(c.peers) + 1
}

func (c *RaftCluster) onVoteResponse(resp *raft.RequestVoteResponse) {
	log.Print("vote resp state: ", c.state, ", get: ", resp)
	if !c.checkTerm(resp.GetTerm()) {
		return
	}
	if c.state != CANDIDATE {
		return
	}
	if resp.GetVoteGranted() {
		c.votes += 1
		if c.votes > c.QuorumSize()/2 {
			c.setState(LEADER)
			c.heartbeatTimer.Reset(1)
		}
	}
}

func (c *RaftCluster) onAppendResponse(resp *raft.AppendEntriesResponse) {
	log.Print("append resp state: ", c.state, ", get: ", resp)
	if !c.checkTerm(resp.GetTerm()) {
		return
	}
	// TODO
	if c.state == LEADER {
		return
	}
}

func (c *RaftCluster) onHeartbeatTimer() {
	if c.state != LEADER {
		return
	}

	for _, v := range c.peers {
		client := raft.NewRaftServiceClient(v.conn)
		req := &raft.AppendEntriesRequest{
			Term:     c.currentTerm,
			LeaderId: c.my.config.GetId(),
		}
		go func() {
			r, err := client.AppendEntries(context.Background(), req)
			if err != nil {
				log.Printf("Error: %+v", err)
			} else {
				c.appendResponse <- r
			}
		}()
	}

	c.heartbeatTimer.Reset(HeartbeatTimeout)
}

// RPC request
func (c *RaftCluster) onVoteRequest(r *raft.RequestVoteRequest) *raft.RequestVoteResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Print("vote req state: ", c.state, ", get: ", r)

	_ = c.checkTerm(r.GetTerm())

	resp := raft.RequestVoteResponse{c.currentTerm, false}
	if (c.currentTerm == r.GetTerm()) && (c.votedFor == 0 || c.votedFor == r.GetCandidateId()) {
		c.votedFor = r.GetCandidateId()
		resp.VoteGranted = true
		c.persistHardState()
	}
	return &resp
}

func (c *RaftCluster) onAppendRequest(r *raft.AppendEntriesRequest) *raft.AppendEntriesResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Print("append req state: ", c.state, ", get: ", r)

	_ = c.checkTerm(r.GetTerm())
	resp := raft.AppendEntriesResponse{c.currentTerm, false}
	if c.currentTerm > r.GetTerm() {
		return &resp
	}
	resp.Success = true
	c.electionTimer.Reset(ElectionTimeoutMs * time.Millisecond)
	return &resp
}

func (c *RaftCluster) Run() {
	log.Printf("RaftCluster running")
	c.mu.Lock()
	if c.my.config.GetDataPath() == "" {
		panic("data path empty")
	}
	dataPath := c.my.config.GetDataPath()
	if wal.Exist(dataPath) {
		w, err := wal.Open(dataPath, walpb.Snapshot{})
		if err != nil {
			panic(err)
		}
		_, hard, _, err := w.ReadAll()
		if err != nil {
			panic(err)
		}
		c.wal = w
		c.currentTerm = hard.Term
		c.votedFor = hard.Vote
	} else {
		metadata := []byte{}
		w, err := wal.Create(dataPath, metadata)
		if err != nil {
			panic(err)
		}
		c.wal = w
	}

	for _, v := range c.peers {
		v.Dial()
	}
	c.electionTimer = time.NewTimer(ElectionTimeoutMs * time.Millisecond)
	c.heartbeatTimer = time.NewTimer(HeartbeatTimeout)
	c.mu.Unlock()

	for {
		select {
		case r := <-c.voteResponse:
			c.mu.Lock()
			c.onVoteResponse(r)
			c.mu.Unlock()
		case r := <-c.appendResponse:
			c.mu.Lock()
			c.onAppendResponse(r)
			c.mu.Unlock()
		case <-c.heartbeatTimer.C:
			c.mu.Lock()
			c.onHeartbeatTimer()
			c.mu.Unlock()
		case <-c.electionTimer.C:
			c.mu.Lock()
			c.startElection()
			c.mu.Unlock()
		}
	}

	log.Printf("RaftCluster exit")
}

func NewRaftNode(config raft.Node) *RaftNode {
	return &RaftNode{
		config: config,
	}
}

func NewRaftCluster(config raft.Cluster, myid uint64) *RaftCluster {
	cluster := RaftCluster{
		peers:          make(map[uint64]*RaftNode),
		voteResponse:   make(chan *raft.RequestVoteResponse),
		appendResponse: make(chan *raft.AppendEntriesResponse),
	}
	for _, e := range config.GetNodes() {
		if e.GetId() == 0 {
			panic("id must be larger than 0")
		}
		if e.GetId() == myid {
			cluster.my = NewRaftNode(*e)
		} else {
			cluster.peers[e.GetId()] = NewRaftNode(*e)
		}
	}
	if cluster.my == nil {
		panic("invalid nodeid")
	}
	return &cluster
}

func main() {
	flag.Parse()
	clusterConfig := raft.Cluster{}
	config, err := ioutil.ReadFile(*config)
	checkError(err)
	checkError(proto.UnmarshalText(string(config), &clusterConfig))
	fmt.Printf("%+v\n", clusterConfig)

	cluster := NewRaftCluster(clusterConfig, *nodeid)
	service := RaftServiceImpl{cluster: cluster}
	go cluster.Run()
	service.Run()
}
