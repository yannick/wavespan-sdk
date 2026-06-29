//go:build !linux

package wavespan

import (
	"log"
	"sync"
	"time"
)

// monoBase anchors the monotonic fallback clock; time.Since(monoBase) carries Go's monotonic reading.
var monoBase = time.Now()

// warnOnce emits the best-effort-fencing warning a single time per process.
var warnOnce sync.Once

// nowMono returns a monotonic timestamp in nanoseconds. Off Linux there is no portable CLOCK_BOOTTIME, so
// this falls back to Go's monotonic clock (CLOCK_MONOTONIC on most platforms), which FREEZES during host
// suspend/sleep. The suspend self-fence (§2 C1) is therefore best-effort here: a suspend longer than the
// pause budget that also freezes this clock would not be detected. The §7 suspend nemesis is Linux-only
// for exactly this reason. Production holders that must be suspend-safe should run on Linux.
//
// The returned value is offset off zero so the first reading is always > 0 (callers use 0 as the
// "un-anchored" sentinel for lastSeenMon).
func nowMono() int64 {
	warnOnce.Do(func() {
		log.Printf("wavespan: nowMono() falling back to CLOCK_MONOTONIC (non-Linux); suspend self-fencing is best-effort — run on Linux for CLOCK_BOOTTIME guarantees")
	})
	// +1 keeps the very first reading strictly positive even if time.Since rounds to 0.
	return int64(time.Since(monoBase)) + 1
}
