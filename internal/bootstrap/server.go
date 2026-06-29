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

package bootstrap

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"

	"github.com/ks-tool/yellow-pages/internal/clock"
	"github.com/ks-tool/yellow-pages/internal/config"
)

// metadataToken is the gRPC metadata key carrying the bootstrap token.
const metadataToken = "bootstrap-token"

// Options configures the bootstrap Service.
type Options struct {
	Config        *config.Config // the serving seed's config (sanitized at render)
	SigningKey    []byte         // HMAC key validating short-lived tokens
	AllowSeedJoin bool           // permit role=seed (high risk)
	Seeds         []string       // advertised seed list written into served configs
	RateLimit     int            // per-client requests/sec (<= 0 disables; cmd passes a positive default)
	Clock         clock.Clock    // time source for token expiry + rate-limit window (defaults to System)
	Log           *slog.Logger
}

// Service implements the BootstrapService gRPC RPC on a seed: it serves
// sanitized configs to callers presenting a valid short-lived token. It runs on
// the SAME gRPC server as AgentService (no extra listener), reusing its TLS/mTLS
// and interceptor chain.
type Service struct {
	discoveryv1.UnimplementedBootstrapServiceServer
	cfg           *config.Config
	signingKey    []byte
	allowSeedJoin bool
	seeds         []string
	rrl           *ratelimit.Limiter
	clk           clock.Clock
	log           *slog.Logger
}

// NewService builds the bootstrap gRPC service.
func NewService(opts Options) *Service {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = clock.System()
	}
	return &Service{
		cfg: opts.Config, signingKey: opts.SigningKey, allowSeedJoin: opts.AllowSeedJoin,
		seeds: opts.Seeds, rrl: ratelimit.New(opts.RateLimit, opts.Clock), clk: opts.Clock, log: opts.Log,
	}
}

// Register registers the service on the gRPC server.
func (s *Service) Register(reg grpc.ServiceRegistrar) {
	discoveryv1.RegisterBootstrapServiceServer(reg, s)
}

// GetConfig returns a sanitized config for the requested role after validating
// the short-lived token and the seed-join gate. Errors use gRPC status codes.
func (s *Service) GetConfig(ctx context.Context, req *discoveryv1.GetConfigRequest) (*discoveryv1.GetConfigResponse, error) {
	client := peerAddr(ctx)
	// Rate-limit per source HOST, not host:port — the ephemeral source port
	// changes on every connection, so port-keying would let a reconnecting client
	// bypass the limit. The full address is still used for the audit log.
	if !s.rrl.allow(peerHost(ctx)) {
		return nil, status.Error(codes.ResourceExhausted, "bootstrap rate limit exceeded")
	}
	if err := ValidateToken(s.signingKey, tokenFromMetadata(ctx), s.clk.Now()); err != nil {
		s.log.Warn("bootstrap denied", "client", client, "reason", err.Error())
		return nil, status.Error(codes.Unauthenticated, "invalid or expired bootstrap token")
	}

	role := config.Role(strings.ToLower(req.GetRole()))
	if role == "" {
		role = config.RoleAgent
	}
	switch role {
	case config.RoleAgent:
	case config.RoleSeed:
		if !s.allowSeedJoin {
			s.log.Warn("bootstrap denied: seed-join disabled", "client", client)
			return nil, status.Error(codes.PermissionDenied, "bootstrapping as a seed is disabled on this cluster")
		}
	default:
		return nil, status.Errorf(codes.InvalidArgument, "role must be agent or seed, got %q", req.GetRole())
	}

	body, err := Render(s.cfg, role, s.seeds)
	if err != nil {
		s.log.Error("bootstrap render failed", "error", err)
		return nil, status.Error(codes.Internal, "render failed")
	}
	s.log.Info("bootstrap served", "client", client, "role", string(role))
	return &discoveryv1.GetConfigResponse{Config: body}, nil
}

func tokenFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if v := md.Get(metadataToken); len(v) > 0 {
		return strings.TrimSpace(v[0])
	}
	if v := md.Get("authorization"); len(v) > 0 {
		return strings.TrimSpace(strings.TrimPrefix(v[0], "Bearer "))
	}
	return ""
}

func peerAddr(ctx context.Context) string {
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		return p.Addr.String()
	}
	return "unknown"
}

// peerHost is the source host without the ephemeral port (the rate-limit key).
func peerHost(ctx context.Context) string {
	addr := peerAddr(ctx)
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// rateLimiter is a per-client fixed-window limiter (bootstrap DoS guard).
type rateLimiter struct {
	limit int
	clk   clock.Clock

	mu      sync.Mutex
	counts  map[string]int
	resetAt time.Time
}

func newRateLimiter(perSecond int, clk clock.Clock) *rateLimiter {
	if clk == nil {
		clk = clock.System()
	}
	return &rateLimiter{limit: perSecond, clk: clk, counts: map[string]int{}}
}

func (r *rateLimiter) allow(client string) bool {
	if r == nil || r.limit <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.clk.Now()
	if now.After(r.resetAt) {
		r.counts = make(map[string]int, len(r.counts))
		r.resetAt = now.Add(time.Second)
	}
	r.counts[client]++
	return r.counts[client] <= r.limit
}
