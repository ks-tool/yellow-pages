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

// Package sdk is the public Go client for yellow-pages. It is a thin wrapper over
// the discovery.v1 AgentService exposed by the local agent (127.0.0.1): an app
// registers, renews and deregisters its own services and discovers others. It
// depends ONLY on the generated proto (proto/discovery/v1) and google.golang.org/grpc
// — never on internal packages — so it is a stable, importable consumer surface.
package sdk

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// DefaultAgentAddress is the local agent's default gRPC address.
const DefaultAgentAddress = "127.0.0.1:9900"

// Client talks to a local yellow-pages agent.
type Client struct {
	conn  *grpc.ClientConn
	owned bool
	agent discoveryv1.AgentServiceClient
}

// Dial connects to a local agent at target (default 127.0.0.1:9900). With no
// dial options it uses an insecure connection (trusted loopback). Close closes
// the dialed connection.
func Dial(target string, opts ...grpc.DialOption) (*Client, error) {
	if target == "" {
		target = DefaultAgentAddress
	}
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, owned: true, agent: discoveryv1.NewAgentServiceClient(conn)}, nil
}

// Wrap builds a Client over an existing connection. Close does NOT close it.
func Wrap(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn, agent: discoveryv1.NewAgentServiceClient(conn)}
}

// Close closes the connection if this Client dialed it.
func (c *Client) Close() error {
	if c.owned && c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Register registers a node and its services. It is idempotent: re-registering
// the same data only refreshes the lease.
func (c *Client) Register(ctx context.Context, reg *discoveryv1.Registration) error {
	_, err := c.agent.Register(ctx, &discoveryv1.RegisterRequest{Registration: reg})
	return err
}

// Renew refreshes the leases of the node's services (all when serviceIDs empty).
func (c *Client) Renew(ctx context.Context, nodeID string, serviceIDs ...string) error {
	_, err := c.agent.Renew(ctx, &discoveryv1.RenewRequest{NodeId: nodeID, ServiceIds: serviceIDs})
	return err
}

// Deregister removes the node and all its services.
func (c *Client) Deregister(ctx context.Context, nodeID string) error {
	_, err := c.agent.Deregister(ctx, &discoveryv1.DeregisterRequest{NodeId: nodeID})
	return err
}

// DeregisterService removes a single service.
func (c *Client) DeregisterService(ctx context.Context, nodeID, serviceID string) error {
	_, err := c.agent.DeregisterService(ctx, &discoveryv1.DeregisterServiceRequest{NodeId: nodeID, ServiceId: serviceID})
	return err
}

// Discover returns the matching service instances. Set query.OnlyHealthy to
// exclude critical/maintenance instances.
func (c *Client) Discover(ctx context.Context, query *discoveryv1.Query) ([]*discoveryv1.ServiceEntry, error) {
	resp, err := c.agent.Lookup(ctx, &discoveryv1.LookupRequest{Query: query})
	if err != nil {
		return nil, err
	}
	return resp.GetEntries(), nil
}

// Watch streams the current instance set for the query and an updated set on
// every change, until ctx is cancelled (which closes the returned channel). The
// channel keeps only the latest set, so a slow consumer never blocks the stream.
func (c *Client) Watch(ctx context.Context, query *discoveryv1.Query) (<-chan []*discoveryv1.ServiceEntry, error) {
	stream, err := c.agent.Watch(ctx, &discoveryv1.WatchRequest{Query: query})
	if err != nil {
		return nil, err
	}

	out := make(chan []*discoveryv1.ServiceEntry, 1)
	go func() {
		defer close(out)
		set := make(map[string]*discoveryv1.ServiceEntry)
		ready := false
		for {
			resp, rerr := stream.Recv()
			if rerr != nil {
				return
			}
			if ev := resp.GetEvent(); ev.GetEntry() != nil {
				k := ev.GetEntry().GetNode().GetId() + "\x00" + ev.GetEntry().GetService().GetId()
				switch ev.GetType() {
				case discoveryv1.ChangeType_CHANGE_TYPE_PUT:
					set[k] = ev.GetEntry()
				case discoveryv1.ChangeType_CHANGE_TYPE_DELETE:
					delete(set, k)
				}
			}
			if resp.GetSnapshotDone() {
				ready = true
			}
			if ready {
				emit(out, snapshot(set))
			}
		}
	}()
	return out, nil
}

func snapshot(set map[string]*discoveryv1.ServiceEntry) []*discoveryv1.ServiceEntry {
	out := make([]*discoveryv1.ServiceEntry, 0, len(set))
	for _, e := range set {
		out = append(out, e)
	}
	return out
}

// emit delivers snap on out, replacing any undelivered older snapshot.
func emit(out chan []*discoveryv1.ServiceEntry, snap []*discoveryv1.ServiceEntry) {
	for {
		select {
		case out <- snap:
			return
		default:
		}
		select {
		case <-out:
		default:
		}
	}
}
