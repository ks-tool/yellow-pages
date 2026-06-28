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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/ks-tool/yellow-pages/internal/model"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestNodeIDExplicitName(t *testing.T) {
	t.Parallel()
	id, err := NodeID("seed-1", t.TempDir(), discardLogger())
	if err != nil {
		t.Fatalf("NodeID: %v", err)
	}
	if id != "seed-1" {
		t.Errorf("id = %q, want seed-1 (configured name wins)", id)
	}
}

func TestNodeIDPersistenceAndLoss(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	id1, err := NodeID("", dir, discardLogger())
	if err != nil {
		t.Fatalf("NodeID: %v", err)
	}
	id2, err := NodeID("", dir, discardLogger())
	if err != nil {
		t.Fatalf("NodeID: %v", err)
	}
	if id1 != id2 {
		t.Errorf("persisted id changed across calls: %q vs %q", id1, id2)
	}

	// Losing the file yields a fresh id and a warning.
	if err := os.Remove(filepath.Join(dir, nodeIDFile)); err != nil {
		t.Fatalf("remove node-id: %v", err)
	}
	var warn bytes.Buffer
	wlog := slog.New(slog.NewTextHandler(&warn, &slog.HandlerOptions{Level: slog.LevelWarn}))
	id3, err := NodeID("", dir, wlog)
	if err != nil {
		t.Fatalf("NodeID: %v", err)
	}
	if id3 == id1 {
		t.Errorf("lost id was not regenerated: still %q", id3)
	}
	if !bytes.Contains(warn.Bytes(), []byte("new persisted node id")) {
		t.Errorf("missing warning on id regeneration: %s", warn.String())
	}
}

func TestAuthorizer(t *testing.T) {
	t.Parallel()
	owner := model.Principal{ID: "agent-1"}
	anon := model.Principal{Anonymous: true}

	cases := []struct {
		name      string
		mode      Mode
		principal model.Principal
		nodeID    string
		denied    bool
	}{
		{"disabled never denies", ModeDisabled, anon, "agent-1", false},
		{"allow never denies", ModeAllow, anon, "agent-1", false},
		{"enforce owner allowed", ModeEnforce, owner, "agent-1", false},
		{"enforce non-owner denied", ModeEnforce, owner, "agent-2", true},
		{"enforce anonymous denied", ModeEnforce, anon, "agent-1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := NewAuthorizer(tc.mode).Authorize(tc.principal, tc.nodeID)
			if tc.denied && err == nil {
				t.Errorf("expected denial, got nil")
			}
			if !tc.denied && err != nil {
				t.Errorf("expected allow, got %v", err)
			}
		})
	}
}

func TestIdentityFromCertSubject(t *testing.T) {
	t.Parallel()
	certPEM, _ := genSelfSigned(t, "agent-7")
	block, _ := pem.Decode(certPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			VerifiedChains: [][]*x509.Certificate{{cert}},
		}},
	})
	p := NewIdentity(nil).Principal(ctx)
	if p.Anonymous || p.ID != "agent-7" {
		t.Errorf("principal = %+v, want ID agent-7 from cert subject", p)
	}
}

func TestIdentityFromToken(t *testing.T) {
	t.Parallel()
	id := NewIdentity(map[string]string{"tok-a": "principal-a"})

	cases := []struct {
		name string
		md   metadata.MD
		want model.Principal
	}{
		{"x-consul-token", metadata.Pairs("x-consul-token", "tok-a"), model.Principal{ID: "principal-a"}},
		{"bearer", metadata.Pairs("authorization", "Bearer tok-a"), model.Principal{ID: "principal-a"}},
		{"unknown token -> anonymous", metadata.Pairs("x-consul-token", "nope"), model.Principal{Anonymous: true}},
		{"no metadata -> anonymous", metadata.MD{}, model.Principal{Anonymous: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := metadata.NewIncomingContext(context.Background(), tc.md)
			got := id.Principal(ctx)
			if got.ID != tc.want.ID || got.Anonymous != tc.want.Anonymous {
				t.Errorf("principal = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCertReloaderRotation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	certA, keyA := genSelfSigned(t, "node-a")
	writeFile(t, certPath, certA)
	writeFile(t, keyPath, keyA)

	r, err := newCertReloader(certPath, keyPath)
	if err != nil {
		t.Fatalf("newCertReloader: %v", err)
	}
	first, err := r.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}

	// Rotate the files on disk; bump mtime to guarantee the change is detected.
	certB, keyB := genSelfSigned(t, "node-b")
	writeFile(t, certPath, certB)
	writeFile(t, keyPath, keyB)
	future := time.Now().Add(2 * time.Second)
	for _, p := range []string{certPath, keyPath} {
		if err := os.Chtimes(p, future, future); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	second, err := r.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate after rotation: %v", err)
	}
	if bytes.Equal(first.Certificate[0], second.Certificate[0]) {
		t.Errorf("certificate was not hot-reloaded after rotation")
	}
}

func TestParseModeAndLoadTokens(t *testing.T) {
	t.Parallel()
	if m, err := ParseMode(""); err != nil || m != ModeDisabled {
		t.Errorf("ParseMode(\"\") = %q, %v; want disabled", m, err)
	}
	if _, err := ParseMode("bogus"); err == nil {
		t.Error("ParseMode(bogus) should error")
	}

	if m, err := LoadTokens(""); err != nil || m != nil {
		t.Errorf("LoadTokens(\"\") = %v, %v; want nil map", m, err)
	}
	path := filepath.Join(t.TempDir(), "tokens.yaml")
	writeFile(t, path, []byte("tok-a: principal-a\ntok-b: principal-b\n"))
	tokens, err := LoadTokens(path)
	if err != nil {
		t.Fatalf("LoadTokens: %v", err)
	}
	if tokens["tok-a"] != "principal-a" || tokens["tok-b"] != "principal-b" {
		t.Errorf("tokens = %v", tokens)
	}
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// genSelfSigned returns a self-signed cert/key PEM pair with the given CN.
func genSelfSigned(t *testing.T, cn string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}
