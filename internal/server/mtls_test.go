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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ks-tool/yellow-pages/internal/cred"
	"github.com/ks-tool/yellow-pages/internal/transport"
	discoveryv1 "github.com/ks-tool/yellow-pages/proto/discovery/v1"
)

// TestMutualTLSIdentityAndOwnership exercises the full security stack: a real
// mTLS handshake, the caller Principal derived from the verified client-cert
// subject, and acl.mode=enforce ownership — a client authenticated as "agent-1"
// may register its own node but not another's.
func TestMutualTLSIdentityAndOwnership(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	caCert, caKey := genCA(t)
	caPath := writeCert(t, dir, "ca", encodeCertPEM(caCert.Raw))

	srvCert, srvKey := signCert(t, caCert, caKey, "seed-1", []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback})
	srvCertPath := writeCert(t, dir, "srv-cert", srvCert)
	srvKeyPath := writeCert(t, dir, "srv-key", srvKey)

	cliCert, cliKey := signCert(t, caCert, caKey, "agent-1", nil)
	cliCertPath := writeCert(t, dir, "cli-cert", cliCert)
	cliKeyPath := writeCert(t, dir, "cli-key", cliKey)

	serverCreds, err := cred.NewTLS(cred.TLSConfig{
		CertFile: srvCertPath, KeyFile: srvKeyPath, CAFile: caPath, MutualTLS: true,
	})
	if err != nil {
		t.Fatalf("server creds: %v", err)
	}
	clientCreds, err := cred.NewTLS(cred.TLSConfig{
		CertFile: cliCertPath, KeyFile: cliKeyPath, CAFile: caPath, MutualTLS: true, ServerName: "127.0.0.1",
	})
	if err != nil {
		t.Fatalf("client creds: %v", err)
	}

	comp := NewComponent(Options{
		Addr:      "127.0.0.1:0",
		Service:   New(memStore(t), testLogger()),
		Transport: transport.New(serverCreds),
		Identity:  cred.NewIdentity(nil), // identity comes from the verified cert
		Authz:     cred.NewAuthorizer(cred.ModeEnforce),
		Log:       testLogger(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = comp.Start(ctx) }()
	addr := comp.Addr()
	if addr == nil {
		cancel()
		t.Fatal("server failed to bind")
	}
	conn, err := transport.New(clientCreds).Dial(ctx, addr.String())
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		sctx, scancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer scancel()
		_ = comp.Stop(sctx)
		cancel()
	})

	cli := discoveryv1.NewAgentServiceClient(conn)

	// The client's verified cert subject is "agent-1": it may register its own node.
	if _, err := cli.Register(context.Background(), &discoveryv1.RegisterRequest{
		Registration: &discoveryv1.Registration{
			Node:     &discoveryv1.Node{Id: "agent-1"},
			Services: []*discoveryv1.Service{{Name: "web", TtlSeconds: 30}},
		},
	}); err != nil {
		t.Fatalf("owner register denied: %v", err)
	}

	// It may NOT register a node it does not own.
	_, err = cli.Register(context.Background(), &discoveryv1.RegisterRequest{
		Registration: &discoveryv1.Registration{
			Node:     &discoveryv1.Node{Id: "agent-2"},
			Services: []*discoveryv1.Service{{Name: "web", TtlSeconds: 30}},
		},
	})
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("non-owner register code = %v, want PermissionDenied", got)
	}
}

func genCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: "yp-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return cert, key
}

// signCert issues a leaf cert/key PEM pair with CN cn, signed by the CA.
func signCert(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, ips []net.IP) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(t),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("sign cert: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return encodeCertPEM(der), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func encodeCertPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func serial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	return n
}

func writeCert(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name+".pem")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}
