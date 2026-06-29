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
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var signKey = []byte("test-signing-key-0123456789abcdef")

func TestMintAndValidate(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1_700_000_000, 0)
	tok, err := MintToken(signKey, 30*time.Second, t0)
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}

	if err := ValidateToken(signKey, tok, t0); err != nil {
		t.Errorf("valid-at-mint = %v, want nil", err)
	}
	if err := ValidateToken(signKey, tok, t0.Add(29*time.Second)); err != nil {
		t.Errorf("just-before-expiry = %v, want nil", err)
	}
	if err := ValidateToken(signKey, tok, t0.Add(31*time.Second)); !errors.Is(err, ErrTokenExpired) {
		t.Errorf("after-expiry = %v, want ErrTokenExpired", err)
	}
	if err := ValidateToken([]byte("other-key"), tok, t0); !errors.Is(err, ErrTokenInvalid) {
		t.Errorf("wrong-key = %v, want ErrTokenInvalid", err)
	}
	if err := ValidateToken(signKey, tok+"AA", t0); !errors.Is(err, ErrTokenInvalid) {
		t.Errorf("tampered = %v, want ErrTokenInvalid", err)
	}
	if err := ValidateToken(signKey, "not-base64!!", t0); !errors.Is(err, ErrTokenInvalid) {
		t.Errorf("garbage = %v, want ErrTokenInvalid", err)
	}
	if err := ValidateToken(nil, tok, t0); !errors.Is(err, ErrTokenInvalid) {
		t.Errorf("empty-key = %v, want ErrTokenInvalid", err)
	}
}

func TestMintUniqueTokens(t *testing.T) {
	t.Parallel()
	t0 := time.Unix(1_700_000_000, 0)
	a, _ := MintToken(signKey, time.Minute, t0)
	b, _ := MintToken(signKey, time.Minute, t0)
	if a == b {
		t.Error("two tokens minted at the same instant are identical (nonce not applied)")
	}
}

func TestResolveSigningKey(t *testing.T) {
	t.Parallel()
	if k, _ := ResolveSigningKey("inline-key", ""); string(k) != "inline-key" {
		t.Errorf("inline = %q, want inline-key", k)
	}
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, []byte("  file-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if k, _ := ResolveSigningKey("inline-key", path); string(k) != "file-key" {
		t.Errorf("file (precedence) = %q, want file-key", k)
	}
	if k, _ := ResolveSigningKey("", ""); k != nil {
		t.Errorf("none = %v, want nil", k)
	}
}
