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

// Package transport builds the gRPC server and client side of every connection,
// applying the security supplied by the internal/cred seam. Swapping the trusted-L3
// default for TLS/mTLS is a cred/config choice; the server and seedclient code
// that use a Transport never change. Interceptors and other per-call options are
// passed through.
package transport

import (
	"context"

	"google.golang.org/grpc"

	"github.com/ks-tool/yellow-pages/internal/cred"
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

// New returns a Transport that applies creds' security to servers and dials.
func New(creds cred.Credentials) Transport { return secured{creds: creds} }

// Insecure returns a Transport with no transport security (trusted-L3 default).
func Insecure() Transport { return New(cred.Insecure()) }

type secured struct{ creds cred.Credentials }

// compile-time assertion that secured satisfies Transport.
var _ Transport = secured{}

func (s secured) NewServer(opts ...grpc.ServerOption) *grpc.Server {
	return grpc.NewServer(append(s.creds.ServerOptions(), opts...)...)
}

func (s secured) Dial(_ context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	all := append(s.creds.DialOptions(), opts...)
	return grpc.NewClient(target, all...)
}
