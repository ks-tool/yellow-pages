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
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// nodeIDFile is the file under data_dir that persists a generated node id.
const nodeIDFile = "node-id"

// NodeID resolves this node's stable agent id, carried on every RPC and used as
// the ownership key under acl.mode=enforce. A configured nodeName always wins
// (recommended). Otherwise a UUID is persisted under dataDir and reused across
// restarts; if dataDir is empty or the file is lost, a fresh UUID is generated
// (its old registrations linger as ghosts until their TTL) and a warning logged.
func NodeID(nodeName, dataDir string, log *slog.Logger) (string, error) {
	if nodeName != "" {
		return nodeName, nil
	}
	if dataDir == "" {
		id, err := newUUID()
		if err != nil {
			return "", err
		}
		log.Warn("no node_name or data_dir: using an ephemeral node id "+
			"(stale registrations ghost until TTL on restart)", "node_id", id)
		return id, nil
	}

	path := filepath.Join(dataDir, nodeIDFile)
	if data, err := os.ReadFile(path); err == nil { //nolint:gosec // operator-provided data_dir
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	}

	id, err := newUUID()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return "", fmt.Errorf("cred: create data_dir %q: %w", dataDir, err)
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("cred: persist node id %q: %w", path, err)
	}
	log.Warn("generated a new persisted node id (a stable node_name is recommended)",
		"node_id", id, "path", path)
	return id, nil
}

// newUUID returns a random RFC 4122 version-4 UUID using crypto/rand, avoiding a
// third-party dependency.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("cred: generate node id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
