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
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/cred"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

func registerReq(nodeID string) *discoveryv1.RegisterRequest {
	return &discoveryv1.RegisterRequest{Registration: &discoveryv1.Registration{
		Node:     &discoveryv1.Node{Id: nodeID},
		Services: []*discoveryv1.Service{{Name: "web"}},
	}}
}

func tokenCtx(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-consul-token", token))
}

func TestAuthzInterceptorEnforce(t *testing.T) {
	t.Parallel()
	id := cred.NewIdentity(map[string]string{"tok1": "agent-1"})
	intc := UnaryAuthzInterceptor(id, cred.NewAuthorizer(cred.ModeEnforce), testLogger())

	var called bool
	handler := func(context.Context, any) (any, error) { called = true; return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/discovery.v1.AgentService/Register"}

	cases := []struct {
		name     string
		ctx      context.Context
		req      any
		wantCode codes.Code
		wantCall bool
	}{
		{"owner allowed", tokenCtx("tok1"), registerReq("agent-1"), codes.OK, true},
		{"non-owner denied", tokenCtx("tok1"), registerReq("agent-2"), codes.PermissionDenied, false},
		{"anonymous denied", context.Background(), registerReq("agent-1"), codes.PermissionDenied, false},
		{
			"reads pass through", context.Background(),
			&discoveryv1.LookupRequest{Query: &discoveryv1.Query{Name: "web"}}, codes.OK, true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called = false
			readInfo := info
			if _, ok := tc.req.(*discoveryv1.LookupRequest); ok {
				readInfo = &grpc.UnaryServerInfo{FullMethod: "/discovery.v1.AgentService/Lookup"}
			}
			_, err := intc(tc.ctx, tc.req, readInfo, handler)
			if got := status.Code(err); got != tc.wantCode {
				t.Errorf("code = %v, want %v", got, tc.wantCode)
			}
			if called != tc.wantCall {
				t.Errorf("handler called = %v, want %v", called, tc.wantCall)
			}
		})
	}
}

func TestAuthzInterceptorDisabledNeverDenies(t *testing.T) {
	t.Parallel()
	intc := UnaryAuthzInterceptor(cred.NewIdentity(nil), cred.NewAuthorizer(cred.ModeAllow), testLogger())
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	info := &grpc.UnaryServerInfo{FullMethod: "/discovery.v1.AgentService/Register"}

	// Anonymous write under allow mode is permitted.
	if _, err := intc(context.Background(), registerReq("agent-1"), info, handler); err != nil {
		t.Errorf("allow mode denied an anonymous write: %v", err)
	}
}

func TestAuditInterceptorLogsWrites(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	intc := UnaryAuditInterceptor(cred.NewIdentity(nil), log)
	handler := func(context.Context, any) (any, error) { return "ok", nil }

	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 9), Port: 1234}})
	info := &grpc.UnaryServerInfo{FullMethod: "/discovery.v1.AgentService/Register"}
	if _, err := intc(ctx, registerReq("agent-1"), info, handler); err != nil {
		t.Fatalf("interceptor: %v", err)
	}

	line := buf.String()
	for _, want := range []string{`"msg":"audit"`, `"node":"agent-1"`, `"principal":"anonymous"`, `"peer":"10.0.0.9:1234"`, `"code":"OK"`} {
		if !strings.Contains(line, want) {
			t.Errorf("audit log missing %q in %s", want, line)
		}
	}

	// A read is not audited.
	buf.Reset()
	readInfo := &grpc.UnaryServerInfo{FullMethod: "/discovery.v1.AgentService/Lookup"}
	if _, err := intc(ctx, &discoveryv1.LookupRequest{Query: &discoveryv1.Query{Name: "web"}}, readInfo, handler); err != nil {
		t.Fatalf("interceptor: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("read produced an audit line: %s", buf.String())
	}
}
