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
	"errors"
	"fmt"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/ks-tool/yellow-pages/internal/model"
)

// Mode selects the authorization posture.
type Mode string

const (
	// ModeDisabled performs no identity mapping or authorization (default).
	ModeDisabled Mode = "disabled"
	// ModeAllow resolves a Principal (for audit) but never denies — anonymous
	// callers are allowed. Tokens are accepted and mapped but not enforced.
	ModeAllow Mode = "allow"
	// ModeEnforce maps a token/cert to a Principal and checks write ownership.
	ModeEnforce Mode = "enforce"
)

// ErrPermissionDenied is returned by Authorize when a write is not permitted.
var ErrPermissionDenied = errors.New("permission denied")

// Authorizer decides whether a Principal may mutate a node. Ownership is simple
// and stable: a principal owns the node whose id equals the principal id (an
// agent authenticates as its own node id and may only mutate its own
// registrations). Reads are never gated here.
type Authorizer struct {
	mode Mode
}

// NewAuthorizer builds an Authorizer for the given mode.
func NewAuthorizer(mode Mode) *Authorizer { return &Authorizer{mode: mode} }

// Enforcing reports whether write authorization is enforced.
func (a *Authorizer) Enforcing() bool { return a.mode == ModeEnforce }

// Authorize permits or denies a write to nodeID by p. In disabled/allow it never
// denies; in enforce the principal must be non-anonymous and own the node.
func (a *Authorizer) Authorize(p model.Principal, nodeID string) error {
	if a.mode != ModeEnforce {
		return nil
	}
	if p.Anonymous || p.ID == "" {
		return fmt.Errorf("%w: anonymous caller", ErrPermissionDenied)
	}
	if p.ID != nodeID {
		return fmt.Errorf("%w: principal %q may not modify node %q", ErrPermissionDenied, p.ID, nodeID)
	}
	return nil
}

// ParseMode validates and normalises an acl.mode string (empty -> disabled).
func ParseMode(s string) (Mode, error) {
	switch Mode(s) {
	case "", ModeDisabled:
		return ModeDisabled, nil
	case ModeAllow:
		return ModeAllow, nil
	case ModeEnforce:
		return ModeEnforce, nil
	default:
		return "", fmt.Errorf("cred: invalid acl mode %q (want disabled|allow|enforce)", s)
	}
}

// LoadTokens reads a YAML token→principal map from path. An empty path returns a
// nil map (no tokens recognised).
func LoadTokens(path string) (map[string]string, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path) //nolint:gosec // operator-provided trusted path
	if err != nil {
		return nil, fmt.Errorf("cred: read tokens %q: %w", path, err)
	}
	var tokens map[string]string
	if err := yaml.Unmarshal(data, &tokens); err != nil {
		return nil, fmt.Errorf("cred: parse tokens %q: %w", path, err)
	}
	return tokens, nil
}

// WarnPolicyMismatch logs a loud warning when running in allow mode while the
// configured default policy is deny — the migration footgun where enforcement
// is silently dropped after switching from a Consul cluster with ACLs on.
func WarnPolicyMismatch(mode Mode, defaultPolicy string, log *slog.Logger) {
	if mode == ModeAllow && defaultPolicy == "deny" {
		log.Warn("acl.mode=allow with default_policy=deny: write authorization is NOT enforced; " +
			"set acl.mode=enforce to restore ownership checks")
	}
}
