/*
Copyright 2024 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package groupcache

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"strconv"
	"sync"

	"github.com/bjornleffler/groupcache/consistenthash"
	pb "github.com/bjornleffler/groupcache/groupcachepb"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GrpcPool implements PeerPicker for a pool of gRPC peers.
type GrpcPool struct {
	// Grpc TCP server port.
	port uint

	// this peer's base URL, e.g. "hostname1:1234"
	self string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash
	
	mu        sync.Mutex // guards peers and grpcPeers
	peers     *consistenthash.Map
	grpcPeers map[string]*grpcPeer // keyed by e.g. "hostname2:1234"

	// gRPC options.
	clientOpts []grpc.DialOption
}

// NewGrpcPool initializes a gRPC pool of peers, and registers itself as a PeerPicker.
// The self argument should be a valid host:port pair that points to the current server,
// for example "hostname1:1234".
func NewGrpcPool(self string, port uint) (*GrpcPool) {
	p := &GrpcPool{
		port:      port,
		self:      self,
		grpcPeers: make(map[string]*grpcPeer),
	}
	if p.Replicas == 0 {
		p.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.Replicas, p.HashFn)
	RegisterPeerPicker(func() PeerPicker { return p })

	// TODO(leffler): Add gRPC authentication.
	// Allow insecure gRPC connections.
	p.clientOpts = append(p.clientOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	return p
}

// NewRemoteGrpcPool initializes a gRPC pool for a remote set of peers.
// There is no local server: calls are always forwarded to remote servers.
func NewRemoteGrpcPool() (*GrpcPool) {
	p := &GrpcPool{
		port:      0,
		self:      "",
		grpcPeers: make(map[string]*grpcPeer),
	}
	if p.Replicas == 0 {
		p.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.Replicas, p.HashFn)
	RegisterPeerPicker(func() PeerPicker { return p })

	// TODO(leffler): Add gRPC authentication.
	// Allow insecure gRPC connections.
	p.clientOpts = append(p.clientOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	return p
}

// NewLocalGrpcPool initializes a gRPC pool for local use only.
// There are no remote peers.
func NewLocalGrpcPool(port uint) (*GrpcPool) {
	p := &GrpcPool{
		port:      port,
		self:      "localhost",
		grpcPeers: make(map[string]*grpcPeer),
	}
	if p.Replicas == 0 {
		p.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.Replicas, p.HashFn)
	RegisterPeerPicker(func() PeerPicker { return p })

	// TODO(leffler): Add gRPC authentication.
	// Allow insecure gRPC connections.
	p.clientOpts = append(p.clientOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	addr := fmt.Sprintf("localhost:%d", port)
	p.SetPeers(addr)

	return p
}

// StartGrpcServer starts the gRPC server.
func (p *GrpcPool) StartGrpcServer() error {
	if p.self == "" {
		return fmt.Errorf("Self hostname not set.")
	}
	log.Printf("Starting gRPC server on port %d. Self: %q", p.port, p.self)
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", p.port))
	if err != nil {
		log.Printf("Failed to listen to port %d: %v", p.port, err)
		return err
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterGroupCacheServer(grpcServer, p)
	go grpcServer.Serve(lis)
	return nil
}

// parseHostPort parses a peer "host:port" pair.
// Host is a valid IPv4 address or hostname.
// Port is optional.
func (p *GrpcPool) parseHostPort(peer string) (host string, port uint, err error) {
	port = p.port
	// TODO(leffler): IPv6.
	if i := strings.LastIndexByte(peer, ':'); i>0 {
		host = peer[:i]
		if p, err := strconv.ParseUint(peer[i+1:], 10, 32); err != nil {
			log.Printf("Failed to parse peer address: %q", peer)
			return host, port, err
		} else {
			port = uint(p)
		}
	}
	return
}

// SetPeers updates the pool's list of peers.
// Valid examples: "1.2.3.4", "2.2.2.2:2000", "host1:3000".
func (p *GrpcPool) SetPeers(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(p.Replicas, p.HashFn)
	p.peers.Add(peers...)
	p.grpcPeers = make(map[string]*grpcPeer, len(peers))
	for _, peer := range peers {
		if host, port, err := p.parseHostPort(peer); err == nil {
			p.grpcPeers[peer] = MakeGrpcPeer(host, port)
			p.grpcPeers[peer].Dial(p.clientOpts)
		}
	}
}

func (p *GrpcPool) PickPeer(key string) (Peer, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != p.self {
		return p.grpcPeers[peer], true
	}
	return nil, false
}

func (s *GrpcPool) Get(ctx context.Context, in *pb.GetRequest) (out *pb.GetResponse, err error) {
	// log.Printf("(server) GrpcPool::Get() on %s", s.self)

	// Fetch the value for this group/key.
	groupName := *in.Group
	group := GetGroup(groupName)
	if group == nil {
		log.Printf("Group not found: %q", groupName)
		// TODO(leffler): Return grpc.Error
		return out, fmt.Errorf("Group not found: %q", groupName)
	}
	
	group.Stats.ServerRequests.Add(1)
	var value []byte
	key := *in.Key
	if err := group.Get(ctx, key, AllocatingByteSliceSink(&value)); err != nil {
		// TODO(leffler): Return grpc.Error
		log.Printf("Failed to get group/key value: %v", err)
		return nil, err
	} else {
		minuteQps := float64(0.0)
		out = &pb.GetResponse{
			Value: value,
			MinuteQps: &minuteQps,
		}
	}
	return out, nil
}

func (s *GrpcPool) Set(ctx context.Context, in *pb.SetRequest) (out *emptypb.Empty, err error) {
	// log.Printf("(server) GrpcPool::Set() on %s", s.self)
	groupName := *in.Group
	key := *in.Key
	value := ByteView{b: in.Value}

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		log.Printf("Group not found: %q", groupName)
		// TODO(leffler): Return grpc.Error
		return out, fmt.Errorf("Group not found: %q", groupName)		
	}

	group.Stats.ServerRequests.Add(1)
	err = group.SetLocally(ctx, key, value)
	if err != nil {
		log.Printf("Failed to set value: %v", err)
		// TODO(leffler): Return grpc.Error
		return out, err
	}

	return out, nil
}

func (s *GrpcPool) Delete(ctx context.Context, in *pb.DeleteRequest) (out *emptypb.Empty, err error) {
	// log.Printf("(server) GrpcPool::Delete() on %s", s.self)
	groupName := *in.Group
	key := *in.Key

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		log.Printf("Group not found: %q", groupName)
		// TODO(leffler): Return grpc.Error
		return out, fmt.Errorf("Group not found: %q", groupName)
	}

	group.Stats.ServerRequests.Add(1)
	err = group.DeleteLocally(ctx, key)
	if err != nil {
		log.Printf("Failed to set value: %v", err)
		// TODO(leffler): Return grpc.Error
		return out, err
	}

	return out, nil
}

type grpcPeer struct {
	host string
	port uint
	conn *grpc.ClientConn
	client pb.GroupCacheClient
}

func MakeGrpcPeer(host string, port uint) *grpcPeer {
	return &grpcPeer{host: host, port: port}
}

func (gp *grpcPeer) Dial(opts []grpc.DialOption) (err error) {
	// log.Printf("GrpcPeer::Dial(%s)", gp.String())
	addr := gp.String()
	if gp.conn, err = grpc.Dial(addr, opts...); err != nil {
		// TODO(leffler): Keep trying?
		log.Printf("Failed to dial peer %q: %v", addr, err)
		return err
	}
	gp.client = pb.NewGroupCacheClient(gp.conn)
	return nil
}

func (gp *grpcPeer) String() string {
	return fmt.Sprintf("%s:%d", gp.host, gp.port)
}

func (gp *grpcPeer) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) (error) {
	// log.Printf("(client) GrpcPool::Get() to %s", gp.String())
	if gp.client == nil {
		log.Printf("No client for grpc peer %s", gp.String())
		return fmt.Errorf("No client for grpc peer %s", gp.String())
	}
	result, err := gp.client.Get(ctx, in)
	if result != nil {
		out.Value = result.Value
		out.MinuteQps = result.MinuteQps
	}
	return err
}

func (gp *grpcPeer) Set(ctx context.Context, in *pb.SetRequest, out *emptypb.Empty) error {
	// log.Printf("(client) GrpcPool::Set() to %s", gp.String())
	if gp.client == nil {
		log.Printf("No client for grpc peer %s", gp.String())
		return fmt.Errorf("No client for grpc peer %s", gp.String())
	}
	_, err := gp.client.Set(ctx, in)
	return err
}

func (gp *grpcPeer) Delete(ctx context.Context, in *pb.DeleteRequest, out *emptypb.Empty) error {
	// log.Printf("(client) GrpcPool::Delete() to %s", gp.String())
	if gp.client == nil {
		log.Printf("No client for grpc peer %s", gp.String())
		return fmt.Errorf("No client for grpc peer %s", gp.String())
	}
	_, err := gp.client.Delete(ctx, in)
	return err
}
