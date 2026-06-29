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

package cred

import (
	"context"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/ks-tool/yellow-pages/internal/model"
)

// Identity resolves the caller's Principal from a request context. Precedence:
// a verified mTLS client-cert subject (CN), else a recognised ACL token mapped
// to a principal, else anonymous. It is transport-agnostic — the cert is present
// only under mTLS, tokens arrive in metadata under either transport.
type Identity struct {
	tokens map[string]string // token -> principal id
}

// NewIdentity builds an Identity with an optional token→principal map. A nil map
// means no tokens are recognised (cert subject or anonymous only).
func NewIdentity(tokens map[string]string) Identity {
	return Identity{tokens: tokens}
}

// Principal resolves the caller identity from ctx.
func (i Identity) Principal(ctx context.Context) model.Principal {
	if cn := verifiedCommonName(ctx); cn != "" {
		return model.Principal{ID: cn, Attributes: map[string]string{"auth": "mtls"}}
	}
	if tok := tokenFromMetadata(ctx); tok != "" {
		if id, ok := i.tokens[tok]; ok {
			return model.Principal{ID: id, Attributes: map[string]string{"auth": "token"}}
		}
	}
	return model.Principal{Anonymous: true}
}

// PrincipalForToken resolves a raw ACL token to a principal (anonymous when
// empty or unknown). Used by the Consul HTTP surface, where tokens arrive in
// headers or the ?token query rather than gRPC metadata.
func (i Identity) PrincipalForToken(token string) model.Principal {
	if token != "" {
		if id, ok := i.tokens[token]; ok {
			return model.Principal{ID: id, Attributes: map[string]string{"auth": "token"}}
		}
	}
	return model.Principal{Anonymous: true}
}

// verifiedCommonName returns the CN of the verified client certificate, or "".
func verifiedCommonName(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ""
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}
	chains := tlsInfo.State.VerifiedChains
	if len(chains) == 0 || len(chains[0]) == 0 {
		return ""
	}
	return chains[0][0].Subject.CommonName
}

// tokenFromMetadata extracts an ACL token from gRPC metadata. It accepts the
// Consul-style X-Consul-Token header and an Authorization: Bearer header.
func tokenFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if v := md.Get("x-consul-token"); len(v) > 0 && v[0] != "" {
		return v[0]
	}
	if v := md.Get("authorization"); len(v) > 0 {
		return strings.TrimSpace(strings.TrimPrefix(v[0], "Bearer "))
	}
	return ""
}
