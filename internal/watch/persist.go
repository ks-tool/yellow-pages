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

package watch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ks-tool/yellow-pages/internal/clock"
)

const indexFile = "agent-index"

// LoadBase reads the persisted agent-index high-watermark from dataDir, returning
// 0 when there is none (empty dataDir, missing or unreadable file). The agent
// resumes its synthesised index from this base so it never regresses on restart.
func LoadBase(dataDir string) uint64 {
	if dataDir == "" {
		return 0
	}
	data, err := os.ReadFile(filepath.Join(dataDir, indexFile)) //nolint:gosec // operator-provided data_dir
	if err != nil {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// SaveBase persists index under dataDir. A no-op when dataDir is empty.
func SaveBase(dataDir string, index uint64) error {
	if dataDir == "" {
		return nil
	}
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return fmt.Errorf("watch: create data_dir %q: %w", dataDir, err)
	}
	path := filepath.Join(dataDir, indexFile)
	if err := os.WriteFile(path, []byte(strconv.FormatUint(index, 10)+"\n"), 0o600); err != nil {
		return fmt.Errorf("watch: persist index %q: %w", path, err)
	}
	return nil
}

// Flusher is an app.Component that periodically persists a Watcher's index and
// flushes once more on stop, so an agent restart resumes from a recent base.
type Flusher struct {
	watcher  *Watcher
	dataDir  string
	interval time.Duration
	clock    clock.Clock
	log      *slog.Logger
}

// NewFlusher builds the index flusher.
func NewFlusher(w *Watcher, dataDir string, interval time.Duration, clk clock.Clock, log *slog.Logger) *Flusher {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if clk == nil {
		clk = clock.System()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Flusher{watcher: w, dataDir: dataDir, interval: interval, clock: clk, log: log}
}

// Name identifies the component.
func (f *Flusher) Name() string { return "index-flusher" }

// Start persists the index on every tick until ctx is cancelled.
func (f *Flusher) Start(ctx context.Context) error {
	if f.dataDir == "" {
		<-ctx.Done() // nothing to persist
		return nil
	}
	ticker := f.clock.NewTicker(f.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			f.flush()
		}
	}
}

// Stop persists the index one last time.
func (f *Flusher) Stop(context.Context) error {
	f.flush()
	return nil
}

func (f *Flusher) flush() {
	if err := SaveBase(f.dataDir, f.watcher.Index()); err != nil {
		f.log.Warn("failed to persist agent index", "error", err)
	}
}
