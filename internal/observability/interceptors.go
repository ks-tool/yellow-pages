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

package observability

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/grpcx"
)

// UnaryServerInterceptor returns the server-side unary interceptor chain:
// it recovers from handler panics (turning them into codes.Internal so the
// process stays up), records the call into metrics, and emits a structured
// access-log line with code, latency and peer.
func UnaryServerInterceptor(log *slog.Logger, m Metrics) grpc.UnaryServerInterceptor {
	log = orDefault(log)
	m = metricsOrNop(m)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		resp, err = recoverUnary(ctx, req, info, handler, log)
		observe(log, m, SideServer, info.FullMethod, err, time.Since(start), grpcx.PeerAddr(ctx))
		return resp, err
	}
}

// StreamServerInterceptor is the server-side streaming counterpart (Watch).
func StreamServerInterceptor(log *slog.Logger, m Metrics) grpc.StreamServerInterceptor {
	log = orDefault(log)
	m = metricsOrNop(m)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		start := time.Now()
		err = recoverStream(srv, ss, info, handler, log)
		observe(log, m, SideServer, info.FullMethod, err, time.Since(start), grpcx.PeerAddr(ss.Context()))
		return err
	}
}

// UnaryClientInterceptor returns the client-side unary interceptor: it records
// metrics and an access-log line for outgoing calls (used by the SeedClient from
// M6). It does not recover — panics on the client are the caller's own.
func UnaryClientInterceptor(log *slog.Logger, m Metrics) grpc.UnaryClientInterceptor {
	log = orDefault(log)
	m = metricsOrNop(m)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		observe(log, m, SideClient, method, err, time.Since(start), cc.Target())
		return err
	}
}

// StreamClientInterceptor is the client-side streaming counterpart.
func StreamClientInterceptor(log *slog.Logger, m Metrics) grpc.StreamClientInterceptor {
	log = orDefault(log)
	m = metricsOrNop(m)
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()
		cs, err := streamer(ctx, desc, cc, method, opts...)
		observe(log, m, SideClient, method, err, time.Since(start), cc.Target())
		return cs, err
	}
}

func recoverUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler, log *slog.Logger) (resp any, err error) {
	defer func() {
		if r := recover(); r != nil {
			logPanic(log, info.FullMethod, r)
			resp, err = nil, status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(ctx, req)
}

func recoverStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler, log *slog.Logger) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logPanic(log, info.FullMethod, r)
			err = status.Error(codes.Internal, "internal error")
		}
	}()
	return handler(srv, ss)
}

func logPanic(log *slog.Logger, method string, r any) {
	log.Error("panic recovered in handler",
		"method", method,
		"panic", r,
		"stack", string(debug.Stack()),
	)
}

// observe records the finished call into metrics and the access log.
func observe(log *slog.Logger, m Metrics, side, method string, err error, latency time.Duration, peer string) {
	code := status.Code(err)
	m.ObserveRPC(side, method, code.String(), latency)

	level := slog.LevelInfo
	if code != codes.OK {
		level = slog.LevelWarn
	}
	log.LogAttrs(context.Background(), level, "rpc",
		slog.String("side", side),
		slog.String("method", method),
		slog.String("code", code.String()),
		slog.Duration("latency", latency),
		slog.String("peer", peer),
	)
}

func orDefault(log *slog.Logger) *slog.Logger {
	if log == nil {
		return slog.Default()
	}
	return log
}

func metricsOrNop(m Metrics) Metrics {
	if m == nil {
		return Nop{}
	}
	return m
}
