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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"time"
)

// Bootstrap tokens are stateless: a token is `base64url(exp || nonce || mac)`
// where mac = HMAC-SHA256(signing_key, exp||nonce). The serving seed validates
// the signature and expiry with the SAME signing key from its config — no shared
// mutable state between `yp bootstrap create-token` and the running server. The
// signing key never travels on the wire; only short-lived tokens do.
const (
	tokenPayloadLen = 16 // 8-byte expiry (unix seconds) + 8-byte random nonce
	tokenMACLen     = sha256.Size
	defaultTokenTTL = 30 * time.Second
)

var (
	// ErrTokenInvalid is a malformed or wrongly-signed token.
	ErrTokenInvalid = errors.New("bootstrap: invalid token")
	// ErrTokenExpired is a well-signed token past its expiry.
	ErrTokenExpired = errors.New("bootstrap: token expired")
)

// MintToken issues a token signed by key, valid for ttl from now.
func MintToken(key []byte, ttl time.Duration, now time.Time) (string, error) {
	if len(key) == 0 {
		return "", errors.New("bootstrap: empty signing key")
	}
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	payload := make([]byte, tokenPayloadLen)
	binary.BigEndian.PutUint64(payload[:8], uint64(now.Add(ttl).Unix())) //nolint:gosec // unix seconds are non-negative
	if _, err := rand.Read(payload[8:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(append(payload, sign(key, payload)...)), nil
}

// ValidateToken checks a token's signature and expiry against key at now.
func ValidateToken(key []byte, token string, now time.Time) error {
	if len(key) == 0 {
		return ErrTokenInvalid
	}
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil || len(data) != tokenPayloadLen+tokenMACLen {
		return ErrTokenInvalid
	}
	payload, mac := data[:tokenPayloadLen], data[tokenPayloadLen:]
	if !hmac.Equal(mac, sign(key, payload)) {
		return ErrTokenInvalid
	}
	exp := int64(binary.BigEndian.Uint64(payload[:8])) //nolint:gosec // round-trips the value MintToken wrote
	if now.Unix() > exp {
		return ErrTokenExpired
	}
	return nil
}

func sign(key, payload []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(payload)
	return m.Sum(nil)
}

// ResolveSigningKey returns the signing key from an inline value or a file
// (whichever is set; the file takes precedence). Returns nil when neither is set.
func ResolveSigningKey(inline, file string) ([]byte, error) {
	if strings.TrimSpace(file) != "" {
		data, err := os.ReadFile(file) //nolint:gosec // operator-provided key path
		if err != nil {
			return nil, err
		}
		return []byte(strings.TrimSpace(string(data))), nil
	}
	if inline != "" {
		return []byte(inline), nil
	}
	return nil, nil
}
