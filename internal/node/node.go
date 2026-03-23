/*
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>

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

package node

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ks-tool/yellow-pages/internal/config"
	"github.com/ks-tool/yellow-pages/internal/plugin"
	"github.com/ks-tool/yellow-pages/proto/gen"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ConnectionList manages gRPC connections and clients.
type ConnectionList struct {
	clients map[string]discovery.AgentServiceClient
	conns   map[string]*grpc.ClientConn
	mu      sync.RWMutex
}

func NewConnectionList() *ConnectionList {
	return &ConnectionList{
		clients: make(map[string]discovery.AgentServiceClient),
		conns:   make(map[string]*grpc.ClientConn),
	}
}

func (cl *ConnectionList) Add(key string, conn *grpc.ClientConn, client discovery.AgentServiceClient) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.conns[key] = conn
	cl.clients[key] = client
}

func (cl *ConnectionList) Get(key string) (discovery.AgentServiceClient, *grpc.ClientConn, bool) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	client, ok1 := cl.clients[key]
	conn, ok2 := cl.conns[key]
	if ok1 && ok2 {
		return client, conn, true
	}
	return nil, nil, false
}

func (cl *ConnectionList) Remove(key string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	if conn, ok := cl.conns[key]; ok {
		_ = conn.Close()
		delete(cl.conns, key)
	}
	delete(cl.clients, key)
}

func (cl *ConnectionList) ForEach(f func(key string, client discovery.AgentServiceClient, conn *grpc.ClientConn) error) error {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	for key, client := range cl.clients {
		conn := cl.conns[key]
		if err := f(key, client, conn); err != nil {
			return err
		}
	}
	return nil
}

func (cl *ConnectionList) CloseAll() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for _, conn := range cl.conns {
		_ = conn.Close()
	}
	cl.clients = make(map[string]discovery.AgentServiceClient)
	cl.conns = make(map[string]*grpc.ClientConn)
}

// Node represents a service discovery node (agent or seed).
type Node struct {
	discovery.UnimplementedAgentServiceServer

	cfg           *config.Config
	agentInfo     *discovery.Agent
	services      []*discovery.Service
	serviceByName map[string]*discovery.Service // cache for fast lookup
	isSeed        bool
	registry      *Registry // only for seed
	grpcServer    *grpc.Server
	shutdown      chan struct{}
	wg            sync.WaitGroup

	seedsConn  *ConnectionList // connections to seeds in the local cluster (key = address)
	remoteConn *ConnectionList // connections to seeds in remote clusters (key = cluster name)
	seedAddrs  []string        // current list of seed addresses (for refresh)
}

// NewNode creates a new node.
func NewNode(cfg *config.Config) (*Node, error) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = fmt.Sprintf("node-%d", time.Now().UnixNano())
	}
	agentID := fmt.Sprintf("%s-%s", cfg.Name, hostname)
	agentInfo := &discovery.Agent{
		Id:      agentID,
		Address: getOutboundIP(),
		Port:    int32(cfg.GRPCPort),
	}

	services := make([]*discovery.Service, len(cfg.Services))
	serviceByName := make(map[string]*discovery.Service, len(cfg.Services))
	for i, srv := range cfg.Services {
		endpoints := make([]*discovery.Endpoint, len(srv.Endpoints))
		for j, ep := range srv.Endpoints {
			addr := ep.Address
			if len(addr) == 0 {
				addr = agentInfo.Address
			}

			endpoints[j] = &discovery.Endpoint{
				Name:     ep.Name,
				Protocol: ep.Protocol,
				Address:  addr,
				Port:     int32(ep.Port),
				Path:     ep.Path,
				Metadata: ep.Metadata,
			}
		}
		service := &discovery.Service{
			Name:      srv.Name,
			Endpoints: endpoints,
			Tags:      srv.Tags,
			Metadata:  srv.Metadata,
		}
		services[i] = service
		serviceByName[srv.Name] = service
	}

	node := &Node{
		cfg:           cfg,
		agentInfo:     agentInfo,
		services:      services,
		serviceByName: serviceByName,
		isSeed:        cfg.Seed,
		seedsConn:     NewConnectionList(),
		remoteConn:    NewConnectionList(),
		shutdown:      make(chan struct{}),
	}
	if node.isSeed {
		node.registry = NewRegistry(time.Duration(cfg.TTLSeconds) * time.Second)
	}
	return node, nil
}

// Start starts the node.
func (n *Node) Start() error {
	// 1. Obtain seeds list (from config or plugin)
	if err := n.resolveSeeds(); err != nil {
		return fmt.Errorf("failed to resolve seeds: %w", err)
	}
	if n.cfg.DiscoverySeeds != nil {
		go n.refreshSeedsLoop()
	}

	// 2. Connect to seeds
	for _, addr := range n.seedAddrs {
		if err := n.connectToSeed(addr); err != nil {
			slog.Warn("failed to connect to seed", "addr", addr, "error", err)
		}
	}
	var hasSeeds bool
	_ = n.seedsConn.ForEach(func(key string, _ discovery.AgentServiceClient, _ *grpc.ClientConn) error {
		hasSeeds = true
		return nil
	})
	if !hasSeeds {
		slog.Warn("no seed connections established, running in standalone mode")
	}

	// 3. If not a seed, register on all seeds
	if !n.isSeed && hasSeeds {
		if err := n.registerOnAllSeeds(); err != nil {
			slog.Error("registration failed", "error", err)
		}
		go n.heartbeatLoop()
	}

	// 4. Start gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", n.cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	n.grpcServer = grpc.NewServer()
	discovery.RegisterAgentServiceServer(n.grpcServer, n)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		if err := n.grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server error", "error", err)
		}
	}()
	slog.Info("gRPC server started", "port", n.cfg.GRPCPort, "seed", n.isSeed)

	// 5. If seed, start registry cleanup
	if n.isSeed {
		go n.cleanupLoop()
	}

	return nil
}

// Stop gracefully stops the node.
func (n *Node) Stop() {
	close(n.shutdown)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if !n.isSeed {
		_ = n.seedsConn.ForEach(func(addr string, client discovery.AgentServiceClient, _ *grpc.ClientConn) error {
			_, _ = client.DeregisterAgent(ctx, &discovery.DeregisterAgentRequest{AgentId: n.agentInfo.Id})
			slog.Info("Deregistered from seed", "addr", addr)
			return nil
		})
	}
	if n.grpcServer != nil {
		n.grpcServer.GracefulStop()
	}
	n.seedsConn.CloseAll()
	n.remoteConn.CloseAll()
	n.wg.Wait()
	slog.Info("Node stopped")
}

// resolveSeeds fills seedAddrs from config or plugin.
func (n *Node) resolveSeeds() error {
	if n.cfg.DiscoverySeeds != nil {
		seeds, err := plugin.ExecPlugin(n.cfg.DiscoverySeeds.Name, n.cfg.DiscoverySeeds.Options)
		if err != nil {
			return err
		}
		n.seedAddrs = seeds
	} else {
		n.seedAddrs = n.cfg.Seeds
	}
	return nil
}

// refreshSeedsLoop periodically updates seeds from plugin.
func (n *Node) refreshSeedsLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdown:
			return
		case <-ticker.C:
			if n.cfg.DiscoverySeeds != nil {
				seeds, err := plugin.ExecPlugin(n.cfg.DiscoverySeeds.Name, n.cfg.DiscoverySeeds.Options)
				if err != nil {
					slog.Warn("failed to refresh seeds via plugin", "error", err)
					continue
				}
				// Only update if seeds have changed
				if !slicesEqual(n.seedAddrs, seeds) {
					// Get set of old addresses
					oldSet := make(map[string]bool)
					for _, addr := range n.seedAddrs {
						oldSet[addr] = true
					}
					// Get set of new addresses
					newSet := make(map[string]bool)
					for _, addr := range seeds {
						newSet[addr] = true
					}
					// Remove connections to seeds that are no longer in the list
					for _, addr := range n.seedAddrs {
						if !newSet[addr] {
							n.seedsConn.Remove(addr)
							slog.Info("Removed seed connection", "addr", addr)
						}
					}
					// Add connections to new seeds
					for _, addr := range seeds {
						if !oldSet[addr] {
							if err := n.connectToSeed(addr); err != nil {
								slog.Warn("failed to connect to refreshed seed", "addr", addr, "error", err)
							}
						}
					}
					n.seedAddrs = seeds
					slog.Info("Seeds refreshed", "count", len(seeds))
				}
			}
		}
	}
}

func (n *Node) connectToSeed(addr string) error {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	client := discovery.NewAgentServiceClient(conn)
	n.seedsConn.Add(addr, conn, client)
	slog.Info("Connected to seed", "addr", addr)
	return nil
}

func (n *Node) registerOnAllSeeds() error {
	req := &discovery.RegisterAgentRequest{
		Agent:      n.agentInfo,
		Services:   n.services,
		TtlSeconds: n.cfg.TTLSeconds,
	}
	var lastErr error
	_ = n.seedsConn.ForEach(func(addr string, client discovery.AgentServiceClient, _ *grpc.ClientConn) error {
		_, err := client.RegisterAgent(context.Background(), req)
		if err != nil {
			slog.Error("register failed", "seed", addr, "error", err)
			lastErr = err
		} else {
			slog.Info("Registered", "seed", addr)
		}
		return nil
	})
	return lastErr
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(time.Duration(n.cfg.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdown:
			return
		case <-ticker.C:
			n.sendHeartbeat()
		}
	}
}

func (n *Node) sendHeartbeat() {
	req := &discovery.HeartbeatRequest{
		AgentId:    n.agentInfo.Id,
		Services:   n.services,
		TtlSeconds: n.cfg.TTLSeconds,
	}
	_ = n.seedsConn.ForEach(func(addr string, client discovery.AgentServiceClient, _ *grpc.ClientConn) error {
		_, err := client.Heartbeat(context.Background(), req)
		if err != nil {
			slog.Warn("heartbeat failed", "seed", addr, "error", err)
		}
		return nil
	})
}

func (n *Node) cleanupLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-n.shutdown:
			return
		case <-ticker.C:
			if n.registry != nil {
				n.registry.Cleanup()
			}
		}
	}
}

// --- gRPC methods ---

// GetEndpoints is implemented by all nodes.
func (n *Node) GetEndpoints(_ context.Context, req *discovery.GetEndpointsRequest) (*discovery.GetEndpointsResponse, error) {
	svc, ok := n.serviceByName[req.ServiceName]
	if !ok {
		return &discovery.GetEndpointsResponse{}, nil
	}
	return &discovery.GetEndpointsResponse{Endpoints: svc.Endpoints}, nil
}

// RegisterAgent is only implemented by seeds.
func (n *Node) RegisterAgent(_ context.Context, req *discovery.RegisterAgentRequest) (*discovery.RegisterAgentResponse, error) {
	if !n.isSeed {
		return nil, status.Error(codes.Unimplemented, "this node is not a seed")
	}
	n.registry.Register(req.Agent, req.Services, req.TtlSeconds, 0)
	slog.Info("Agent registered", "agent_id", req.Agent.Id, "cluster", n.cfg.Name)
	return &discovery.RegisterAgentResponse{Success: true}, nil
}

// Heartbeat is only implemented by seeds.
func (n *Node) Heartbeat(_ context.Context, req *discovery.HeartbeatRequest) (*discovery.HeartbeatResponse, error) {
	if !n.isSeed {
		return nil, status.Error(codes.Unimplemented, "this node is not a seed")
	}
	ok := n.registry.Heartbeat(req.AgentId, req.Services, req.TtlSeconds, 0)
	if !ok {
		return &discovery.HeartbeatResponse{Success: false, Message: "agent not found"}, nil
	}
	return &discovery.HeartbeatResponse{Success: true}, nil
}

// DeregisterAgent is only implemented by seeds.
func (n *Node) DeregisterAgent(_ context.Context, req *discovery.DeregisterAgentRequest) (*discovery.DeregisterAgentResponse, error) {
	if !n.isSeed {
		return nil, status.Error(codes.Unimplemented, "this node is not a seed")
	}
	n.registry.Deregister(req.AgentId)
	slog.Info("Agent deregistered", "agent_id", req.AgentId, "cluster", n.cfg.Name)
	return &discovery.DeregisterAgentResponse{Success: true}, nil
}

// GetServiceAgents is only implemented by seeds.
func (n *Node) GetServiceAgents(_ context.Context, req *discovery.GetServiceAgentsRequest) (*discovery.GetServiceAgentsResponse, error) {
	if !n.isSeed {
		return nil, status.Error(codes.Unimplemented, "this node is not a seed")
	}
	records := n.registry.GetAgentsForService(req.ServiceName, req.Filters)
	resp := &discovery.GetServiceAgentsResponse{
		Agents: make([]*discovery.AgentInfo, len(records)),
	}
	for i, rec := range records {
		resp.Agents[i] = &discovery.AgentInfo{
			Agent:    rec.Info,
			LastSeen: rec.LastSeen.UnixNano(),
		}
	}
	return resp, nil
}

// GetRemoteService is used for cross‑cluster queries. It returns local agents (for now).
func (n *Node) GetRemoteService(_ context.Context, req *discovery.GetRemoteServiceRequest) (*discovery.GetRemoteServiceResponse, error) {
	if !n.isSeed {
		return nil, status.Error(codes.Unimplemented, "this node is not a seed")
	}
	records := n.registry.GetAgentsForService(req.ServiceName, nil)
	resp := &discovery.GetRemoteServiceResponse{
		Agents: make([]*discovery.AgentInfo, len(records)),
	}
	for i, rec := range records {
		resp.Agents[i] = &discovery.AgentInfo{
			Agent:    rec.Info,
			LastSeen: rec.LastSeen.UnixNano(),
		}
	}
	return resp, nil
}

// fetchRemoteService is a helper to query a remote cluster (not used in the current version).
func (n *Node) fetchRemoteService(clusterName, serviceName string) ([]*discovery.AgentInfo, error) {
	var targetSeeds []string
	for _, cl := range n.cfg.Clusters {
		if cl.Name == clusterName {
			targetSeeds = cl.Seeds
			break
		}
	}
	if len(targetSeeds) == 0 {
		return nil, fmt.Errorf("unknown cluster %s", clusterName)
	}
	addr := targetSeeds[0]
	client, conn, ok := n.remoteConn.Get(clusterName)
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("failed to dial remote seed: %w", err)
		}
		client = discovery.NewAgentServiceClient(conn)
		n.remoteConn.Add(clusterName, conn, client)
	}
	resp, err := client.GetRemoteService(context.Background(), &discovery.GetRemoteServiceRequest{
		ServiceName: serviceName,
	})
	if err != nil {
		return nil, err
	}
	return resp.Agents, nil
}

// getOutboundIP returns the preferred outbound IP of the node.
func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer func() { _ = conn.Close() }()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

// slicesEqual checks if two string slices have the same elements (order-independent check).
func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aSet := make(map[string]bool)
	for _, v := range a {
		aSet[v] = true
	}
	for _, v := range b {
		if !aSet[v] {
			return false
		}
	}
	return true
}
