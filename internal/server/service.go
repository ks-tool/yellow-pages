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

// Package server implements the native discovery.v1 gRPC surface and the
// component that serves it. The service is a thin projection of the domain: it
// converts proto to model at the boundary (internal/protoconv), calls the Store,
// applies the shared internal/health filter on reads, and maps domain errors to
// gRPC status codes — errors never travel in response bodies.
package server

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/health"
	"github.com/ks-tool/yellow-pages/internal/model"
	"github.com/ks-tool/yellow-pages/internal/protoconv"
	"github.com/ks-tool/yellow-pages/internal/store"
	"github.com/ks-tool/yellow-pages/internal/watch"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// Service implements discoveryv1.AgentServiceServer over a Store. In M3 it is
// the seed's registry surface (Lookup reads the local Store, single-seed path);
// the agent's local-agent-proxy fan-out arrives in M6, and Watch in M8.
type Service struct {
	discoveryv1.UnimplementedAgentServiceServer
	store   store.Store
	watcher *watch.Watcher
	log     *slog.Logger
}

// New builds the AgentService over st.
func New(st store.Store, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: st, log: log}
}

// SetWatcher enables the Watch RPC, backed by w (fed from the Store's change
// notifier). Without it, Watch reports Unimplemented.
func (s *Service) SetWatcher(w *watch.Watcher) *Service {
	s.watcher = w
	return s
}

// Register creates or updates the caller's node and services.
func (s *Service) Register(_ context.Context, req *discoveryv1.RegisterRequest) (*discoveryv1.RegisterResponse, error) {
	if err := s.store.Register(protoconv.RegistrationFromProto(req.GetRegistration())); err != nil {
		return nil, mapError(err)
	}
	return &discoveryv1.RegisterResponse{}, nil
}

// Renew refreshes the per-service lease of the caller's node.
func (s *Service) Renew(_ context.Context, req *discoveryv1.RenewRequest) (*discoveryv1.RenewResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if err := s.store.Renew(req.GetNodeId(), req.GetServiceIds()); err != nil {
		return nil, mapError(err)
	}
	return &discoveryv1.RenewResponse{}, nil
}

// Deregister removes the caller's node and all its services.
func (s *Service) Deregister(_ context.Context, req *discoveryv1.DeregisterRequest) (*discoveryv1.DeregisterResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	if err := s.store.Deregister(req.GetNodeId()); err != nil {
		return nil, mapError(err)
	}
	return &discoveryv1.DeregisterResponse{}, nil
}

// DeregisterService removes a single service from the caller's node.
func (s *Service) DeregisterService(_ context.Context, req *discoveryv1.DeregisterServiceRequest) (*discoveryv1.DeregisterServiceResponse, error) {
	if req.GetNodeId() == "" || req.GetServiceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_id are required")
	}
	if err := s.store.DeregisterService(req.GetNodeId(), req.GetServiceId()); err != nil {
		return nil, mapError(err)
	}
	return &discoveryv1.DeregisterServiceResponse{}, nil
}

// Lookup returns the matching service instances, applying the shared health
// filter when the query asks for healthy-only.
func (s *Service) Lookup(_ context.Context, req *discoveryv1.LookupRequest) (*discoveryv1.LookupResponse, error) {
	q := protoconv.QueryFromProto(req.GetQuery())
	if q.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "query.name is required")
	}
	res := s.store.Lookup(q)
	if q.OnlyHealthy {
		res.Entries = health.Filter(res.Entries, health.FilterOptions{OnlyPassing: true})
	}
	return protoconv.LookupResultToProto(res), nil
}

// Watch streams the initial snapshot (a put event per existing instance, ended
// by snapshot_done) and then live ChangeEvents for the queried service. Register,
// deregister and expire emit events; renews do not.
func (s *Service) Watch(req *discoveryv1.WatchRequest, stream discoveryv1.AgentService_WatchServer) error {
	if s.watcher == nil {
		return status.Error(codes.Unimplemented, "watch is not enabled on this node")
	}
	q := protoconv.QueryFromProto(req.GetQuery())
	if q.Name == "" {
		return status.Error(codes.InvalidArgument, "query.name is required")
	}

	// Subscribe before snapshotting so no change between the two is missed.
	events, cancel := s.watcher.Subscribe(q)
	defer cancel()

	res := s.store.Lookup(q)
	for _, e := range res.Entries {
		ev := protoconv.ChangeEventToProto(model.ChangeEvent{Type: model.ChangePut, Entry: e})
		if err := stream.Send(&discoveryv1.WatchResponse{Event: ev, Index: s.watcher.CurrentIndex(q)}); err != nil {
			return err
		}
	}
	if err := stream.Send(&discoveryv1.WatchResponse{SnapshotDone: true, Index: s.watcher.CurrentIndex(q)}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev := <-events:
			resp := &discoveryv1.WatchResponse{Event: protoconv.ChangeEventToProto(ev), Index: ev.Index}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

// mapError translates a Store error into a gRPC status code. Unknown errors
// become codes.Internal with a generic message (no internal details leak).
func mapError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
