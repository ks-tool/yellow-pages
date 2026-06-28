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

// Package grpcresolver provides the yp:// gRPC name resolver. After Register,
// grpc.NewClient("yp:///user-api") discovers healthy instances through the local
// agent and updates the connection's addresses live as they register and
// deregister — no restart. Targets are yp://[agent-host:port]/service-name; an
// empty authority uses the default local agent. Per-instance Weights are attached
// to each address for weight-aware balancers.
package grpcresolver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"

	"github.com/ks-tool/yellow-pages/client/sdk"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// Scheme is the resolver scheme ("yp").
const Scheme = "yp"

// roundRobin is the default balancing policy: spread RPCs across all instances.
const roundRobin = `{"loadBalancingConfig":[{"round_robin":{}}]}`

type weightKey struct{}

// Register installs the yp:// resolver globally. dialOpts are used to dial the
// local agent (default: insecure loopback). Call once at startup.
func Register(dialOpts ...grpc.DialOption) {
	resolver.Register(&builder{dialOpts: dialOpts})
}

// Weight returns the instance weight attached to a resolver address (default 1),
// for use by weight-aware balancers.
func Weight(a resolver.Address) uint32 {
	if a.Attributes == nil {
		return 1
	}
	if w, ok := a.Attributes.Value(weightKey{}).(uint32); ok {
		return w
	}
	return 1
}

type builder struct {
	dialOpts []grpc.DialOption
}

func (b *builder) Scheme() string { return Scheme }

func (b *builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	agentAddr := target.URL.Host
	if agentAddr == "" {
		agentAddr = sdk.DefaultAgentAddress
	}
	service := strings.TrimPrefix(target.URL.Path, "/")
	if service == "" {
		return nil, fmt.Errorf("yp resolver: target %q has no service name", target.URL.String())
	}

	client, err := sdk.Dial(agentAddr, b.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("yp resolver: dial agent %q: %w", agentAddr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	updates, err := client.Watch(ctx, &discoveryv1.Query{Name: service, OnlyHealthy: true})
	if err != nil {
		cancel()
		_ = client.Close()
		return nil, fmt.Errorf("yp resolver: watch %q: %w", service, err)
	}

	r := &ypResolver{cc: cc, client: client, cancel: cancel}
	go r.run(updates)
	return r, nil
}

type ypResolver struct {
	cc     resolver.ClientConn
	client *sdk.Client
	cancel context.CancelFunc
}

func (r *ypResolver) run(updates <-chan []*discoveryv1.ServiceEntry) {
	cfg := r.cc.ParseServiceConfig(roundRobin)
	for entries := range updates {
		state := resolver.State{Addresses: toAddresses(entries)}
		if cfg != nil && cfg.Err == nil {
			state.ServiceConfig = cfg
		}
		_ = r.cc.UpdateState(state)
	}
}

// ResolveNow is a no-op: the watch already pushes updates live.
func (r *ypResolver) ResolveNow(resolver.ResolveNowOptions) {}

// Close stops the watch and closes the agent connection.
func (r *ypResolver) Close() {
	r.cancel()
	_ = r.client.Close()
}

func toAddresses(entries []*discoveryv1.ServiceEntry) []resolver.Address {
	addrs := make([]resolver.Address, 0, len(entries))
	for _, e := range entries {
		host := e.GetService().GetAddress()
		if host == "" {
			host = e.GetNode().GetAddress()
		}
		if host == "" {
			continue
		}
		addr := resolver.Address{Addr: net.JoinHostPort(host, strconv.Itoa(int(e.GetService().GetPort())))}
		addr.Attributes = addr.Attributes.WithValue(weightKey{}, weightOf(e))
		addrs = append(addrs, addr)
	}
	return addrs
}

func weightOf(e *discoveryv1.ServiceEntry) uint32 {
	if w := e.GetService().GetWeights().GetPassing(); w > 0 {
		return w
	}
	return 1
}
