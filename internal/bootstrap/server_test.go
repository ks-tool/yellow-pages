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
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/clock"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

var clockT0 = time.Unix(1_700_000_000, 0)

func testService(t *testing.T, allowSeedJoin bool, rate int, clk clock.Clock) *Service {
	t.Helper()
	return NewService(Options{
		Config:        secretSeedConfig(),
		SigningKey:    signKey,
		AllowSeedJoin: allowSeedJoin,
		Seeds:         []string{"seed-a:9900"},
		RateLimit:     rate,
		Clock:         clk,
		Log:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func callCtx(token string) context.Context {
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 9), Port: 5000}})
	if token != "" {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(metadataToken, token))
	}
	return ctx
}

func get(s *Service, token, role string) (*discoveryv1.GetConfigResponse, error) {
	return s.GetConfig(callCtx(token), &discoveryv1.GetConfigRequest{Role: role})
}

func TestServiceTokenAuth(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(clockT0)
	s := testService(t, false, 0, clk)
	valid, _ := MintToken(signKey, 30*time.Second, clk.Now())

	if _, err := get(s, "", "agent"); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no token = %v, want Unauthenticated", err)
	}
	if _, err := get(s, "garbage", "agent"); status.Code(err) != codes.Unauthenticated {
		t.Errorf("garbage token = %v, want Unauthenticated", err)
	}
	resp, err := get(s, valid, "agent")
	if err != nil {
		t.Fatalf("valid token = %v, want ok", err)
	}
	if !strings.Contains(string(resp.GetConfig()), "role: agent") {
		t.Errorf("config missing agent role:\n%s", resp.GetConfig())
	}

	// The token expires once the clock passes its TTL.
	clk.Advance(31 * time.Second)
	if _, err := get(s, valid, "agent"); status.Code(err) != codes.Unauthenticated {
		t.Errorf("expired token = %v, want Unauthenticated", err)
	}
}

func TestServiceSeedJoinGate(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(clockT0)
	tok, _ := MintToken(signKey, time.Minute, clk.Now())

	denied := testService(t, false, 0, clk)
	if _, err := get(denied, tok, "seed"); status.Code(err) != codes.PermissionDenied {
		t.Errorf("seed-join (disabled) = %v, want PermissionDenied", err)
	}
	if _, err := get(denied, tok, "agent"); err != nil {
		t.Errorf("agent (seed-join disabled) = %v, want ok", err)
	}

	allowed := testService(t, true, 0, clk)
	resp, err := get(allowed, tok, "seed")
	if err != nil {
		t.Fatalf("seed-join (enabled) = %v, want ok", err)
	}
	if !strings.Contains(string(resp.GetConfig()), "role: seed") {
		t.Errorf("config missing seed role:\n%s", resp.GetConfig())
	}
}

func TestServiceRateLimit(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(clockT0)
	s := testService(t, false, 1, clk)
	tok, _ := MintToken(signKey, time.Minute, clk.Now())

	if _, err := get(s, tok, "agent"); err != nil {
		t.Fatalf("first = %v, want ok", err)
	}
	if _, err := get(s, tok, "agent"); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("second within window = %v, want ResourceExhausted", err)
	}
}
