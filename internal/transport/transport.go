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

// Package transport is the seam that builds gRPC servers and client connections
// with the configured security. M3 ships the trusted-L3 default (Insecure, no
// transport security, explicitly documented); M4 plugs in TLS/mTLS via the
// Credentials seam behind this same interface, so the server and seedclient code
// never change. Interceptors and other per-call options are passed through.
package transport

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Transport constructs the gRPC server and client side of every connection.
type Transport interface {
	// NewServer builds a *grpc.Server carrying the transport's server security
	// plus the supplied options (interceptor chains, etc.).
	NewServer(opts ...grpc.ServerOption) *grpc.Server
	// Dial creates a client connection to target with the transport's client
	// security plus the supplied options. The connection is lazy (it connects on
	// first use), matching grpc.NewClient semantics.
	Dial(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)
}

// Insecure is the default trusted-L3 transport: no transport security on either
// side. It suits the on-premise, network-restricted target and is explicitly
// documented; turning on mTLS is an M4 config choice, not a code change.
type Insecure struct{}

// compile-time assertion that Insecure satisfies Transport.
var _ Transport = Insecure{}

// NewServer builds an insecure gRPC server with the supplied options.
func (Insecure) NewServer(opts ...grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(opts...)
}

// Dial creates an insecure client connection to target.
func (Insecure) Dial(_ context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	all := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	return grpc.NewClient(target, all...)
}
