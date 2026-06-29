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

package server

import (
	"context"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/grpcx"
	"github.com/ks-tool/yellow-pages/internal/model"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// UnaryAuthzInterceptor enforces write ownership under acl.mode=enforce: the
// caller's Principal (resolved from the mTLS cert subject or an ACL token) must
// own the node a write targets. Reads pass through. In disabled/allow modes the
// authorizer never denies, so this is a cheap pass-through.
func UnaryAuthzInterceptor(id cred.Identity, authz *cred.Authorizer, log *slog.Logger) grpc.UnaryServerInterceptor {
	if log == nil {
		log = slog.Default()
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		nodeID, isWrite := writeTarget(req)
		if isWrite && authz.Enforcing() {
			p := id.Principal(ctx)
			if err := authz.Authorize(p, nodeID); err != nil {
				log.Warn("write denied by acl",
					"method", info.FullMethod,
					"node", nodeID,
					"principal", principalName(p),
					"peer", grpcx.PeerAddr(ctx),
					"reason", err.Error(),
				)
				return nil, status.Error(codes.PermissionDenied, "permission denied")
			}
		}
		return handler(ctx, req)
	}
}

// UnaryAuditInterceptor writes a structured audit record for every write RPC,
// capturing the method, target node, resolved principal, peer and result code.
func UnaryAuditInterceptor(id cred.Identity, log *slog.Logger) grpc.UnaryServerInterceptor {
	if log == nil {
		log = slog.Default()
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		nodeID, isWrite := writeTarget(req)
		resp, err := handler(ctx, req)
		if isWrite {
			log.LogAttrs(ctx, slog.LevelInfo, "audit",
				slog.String("method", info.FullMethod),
				slog.String("node", nodeID),
				slog.String("principal", principalName(id.Principal(ctx))),
				slog.String("peer", grpcx.PeerAddr(ctx)),
				slog.String("code", status.Code(err).String()),
			)
		}
		return resp, err
	}
}

// writeTarget reports the node a request mutates, and whether it is a write at
// all. Reads (Lookup/Watch) return ("", false).
func writeTarget(req any) (nodeID string, isWrite bool) {
	switch r := req.(type) {
	case *discoveryv1.RegisterRequest:
		return r.GetRegistration().GetNode().GetId(), true
	case *discoveryv1.RenewRequest:
		return r.GetNodeId(), true
	case *discoveryv1.DeregisterRequest:
		return r.GetNodeId(), true
	case *discoveryv1.DeregisterServiceRequest:
		return r.GetNodeId(), true
	default:
		return "", false
	}
}

func principalName(p model.Principal) string {
	if p.Anonymous || p.ID == "" {
		return "anonymous"
	}
	return p.ID
}
