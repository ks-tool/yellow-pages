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
	"crypto/tls"
	"os"
	"sync"
	"time"
)

// certReloader loads a certificate/key pair and hot-reloads it when either file
// changes on disk, so rotation needs no restart. The reload is lazy: every TLS
// handshake calls GetCertificate/GetClientCertificate, which re-reads the pair
// only when a modification time changed, and only swaps to a valid pair —
// a mismatched cert/key written non-atomically keeps the previous one in use.
type certReloader struct {
	certFile, keyFile string

	mu      sync.RWMutex
	cached  *tls.Certificate
	modCert time.Time
	modKey  time.Time
}

func newCertReloader(certFile, keyFile string) (*certReloader, error) {
	r := &certReloader{certFile: certFile, keyFile: keyFile}
	if err := r.reload(); err != nil { // initial load validates the pair
		return nil, err
	}
	return r, nil
}

// GetCertificate serves the current server certificate (tls.Config hook).
func (r *certReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return r.current(), nil
}

// GetClientCertificate serves the current client certificate (tls.Config hook).
func (r *certReloader) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return r.current(), nil
}

// current returns the cached certificate, reloading first if the files changed.
// A reload error is swallowed so the last good certificate stays in use.
func (r *certReloader) current() *tls.Certificate {
	if r.changed() {
		_ = r.reload()
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cached
}

func (r *certReloader) changed() bool {
	ci, err1 := os.Stat(r.certFile)
	ki, err2 := os.Stat(r.keyFile)
	if err1 != nil || err2 != nil {
		return false // a transiently-missing file: keep the cached cert
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return !ci.ModTime().Equal(r.modCert) || !ki.ModTime().Equal(r.modKey)
}

func (r *certReloader) reload() error {
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return err
	}
	ci, err1 := os.Stat(r.certFile)
	ki, err2 := os.Stat(r.keyFile)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.cached = &cert
	if err1 == nil {
		r.modCert = ci.ModTime()
	}
	if err2 == nil {
		r.modKey = ki.ModTime()
	}
	return nil
}
