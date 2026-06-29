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

// Package grpcx holds tiny gRPC plumbing helpers shared across surfaces (kept in
// one leaf package so the same logic is not re-derived per package). It imports
// only the gRPC peer API.
package grpcx

import (
	"context"
	"net"

	"google.golang.org/grpc/peer"
)

// PeerAddr returns the caller's address from the gRPC peer info, or "unknown".
// Used for access logging and as a rate-limit key base.
func PeerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}

// PeerHost is PeerAddr without the ephemeral port — the stable per-source-IP key
// for rate limiting (the source port changes on every connection).
func PeerHost(ctx context.Context) string {
	addr := PeerAddr(ctx)
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}
