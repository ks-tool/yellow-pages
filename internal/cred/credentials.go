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

// Package cred is the security seam. It supplies the gRPC transport security
// (insecure trusted-L3 by default, or TLS/mTLS enabled purely by config — no
// code change), resolves the caller Principal from a request context (verified
// mTLS cert subject or ACL token, else anonymous), and decides write
// authorization under acl.mode. Server certificates hot-reload on rotation
// without a restart. The domain stays clean: only model.Principal crosses back.
package cred

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Credentials supplies the gRPC server and dial security for the node. The same
// node identity (cert/key) is presented both when serving (as a server cert) and
// when dialing seeds (as a client cert under mTLS).
type Credentials interface {
	// ServerOptions returns the grpc.ServerOptions carrying transport security.
	ServerOptions() []grpc.ServerOption
	// DialOptions returns the grpc.DialOptions carrying transport security.
	DialOptions() []grpc.DialOption
}

// Insecure returns the default trusted-L3 credentials: no transport security on
// either side. Suits the on-premise target and is explicitly documented.
func Insecure() Credentials { return insecureCreds{} }

type insecureCreds struct{}

func (insecureCreds) ServerOptions() []grpc.ServerOption { return nil }

func (insecureCreds) DialOptions() []grpc.DialOption {
	return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
}

// TLSConfig configures TLS/mTLS credentials.
type TLSConfig struct {
	// CertFile and KeyFile are the node's certificate and private key (PEM).
	CertFile string
	KeyFile  string
	// CAFile is the trust anchor used to verify peers (PEM). Required for mTLS.
	CAFile string
	// MutualTLS requires and verifies a client certificate (server side) and
	// presents the node cert when dialing (client side).
	MutualTLS bool
	// ServerName overrides the name verified against the seed's certificate when
	// dialing; empty uses the dial target host.
	ServerName string
}

// tlsCreds is the TLS/mTLS implementation. The certificate is served through a
// reloader so rotation is picked up without a restart.
type tlsCreds struct {
	cfg      TLSConfig
	reloader *certReloader
	caPool   *x509.CertPool
}

// compile-time assertion that *tlsCreds satisfies Credentials.
var _ Credentials = (*tlsCreds)(nil)

// NewTLS builds TLS/mTLS credentials from cfg, loading (and validating) the
// certificate and trust anchor up front.
func NewTLS(cfg TLSConfig) (Credentials, error) {
	reloader, err := newCertReloader(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, err
	}
	var pool *x509.CertPool
	if cfg.CAFile != "" {
		pool, err = loadCAPool(cfg.CAFile)
		if err != nil {
			return nil, err
		}
	}
	if cfg.MutualTLS && pool == nil {
		return nil, fmt.Errorf("cred: mutual TLS requires a ca_file")
	}
	return &tlsCreds{cfg: cfg, reloader: reloader, caPool: pool}, nil
}

func (t *tlsCreds) ServerOptions() []grpc.ServerOption {
	sc := &tls.Config{
		GetCertificate: t.reloader.GetCertificate,
		MinVersion:     tls.VersionTLS12,
	}
	if t.cfg.MutualTLS {
		sc.ClientAuth = tls.RequireAndVerifyClientCert
		sc.ClientCAs = t.caPool
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(sc))}
}

func (t *tlsCreds) DialOptions() []grpc.DialOption {
	cc := &tls.Config{
		RootCAs:    t.caPool,
		ServerName: t.cfg.ServerName,
		MinVersion: tls.VersionTLS12,
	}
	if t.cfg.MutualTLS {
		cc.GetClientCertificate = t.reloader.GetClientCertificate
	}
	return []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(cc))}
}

func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path) //nolint:gosec // operator-provided trusted path
	if err != nil {
		return nil, fmt.Errorf("cred: read ca %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("cred: ca %q: no certificates found", path)
	}
	return pool, nil
}
