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

package clock

import (
	"testing"
	"time"
)

func TestFake_AfterFiresOnAdvance(t *testing.T) {
	t.Parallel()

	f := NewFake(time.Unix(0, 0))
	ch := f.After(10 * time.Second)

	select {
	case <-ch:
		t.Fatal("After fired before the clock advanced")
	default:
	}

	f.Advance(9 * time.Second)
	select {
	case <-ch:
		t.Fatal("After fired early")
	default:
	}

	f.Advance(1 * time.Second)
	select {
	case got := <-ch:
		if want := time.Unix(10, 0); !got.Equal(want) {
			t.Errorf("fire time = %v, want %v", got, want)
		}
	default:
		t.Fatal("After did not fire after crossing its deadline")
	}
}

func TestFake_NowAndSince(t *testing.T) {
	t.Parallel()

	start := time.Unix(1000, 0)
	f := NewFake(start)
	if !f.Now().Equal(start) {
		t.Fatalf("Now = %v, want %v", f.Now(), start)
	}
	f.Advance(3 * time.Second)
	if got := f.Since(start); got != 3*time.Second {
		t.Errorf("Since = %v, want 3s", got)
	}
}

func TestFake_BlockUntil(t *testing.T) {
	t.Parallel()

	f := NewFake(time.Unix(0, 0))
	done := make(chan struct{})
	go func() {
		_ = f.After(time.Second)
		close(done)
	}()

	f.BlockUntil(1) // returns once the goroutine has armed its waiter
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("BlockUntil returned before the waiter was registered")
	}
}

func TestFake_TickerFiresPerPeriod(t *testing.T) {
	t.Parallel()

	f := NewFake(time.Unix(0, 0))
	tk := f.NewTicker(time.Second)
	defer tk.Stop()

	// Advancing 3s while nobody drains the (cap-1) channel yields a single
	// pending tick, mirroring *time.Ticker coalescing.
	f.Advance(3 * time.Second)
	select {
	case <-tk.C():
	default:
		t.Fatal("ticker did not fire after advancing past its period")
	}
}
