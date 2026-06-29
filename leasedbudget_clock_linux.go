//go:build linux

package wavespan

import "golang.org/x/sys/unix"

// nowMono returns a suspend-aware monotonic timestamp in nanoseconds, read from CLOCK_BOOTTIME.
//
// LOAD-BEARING (§2 C1): the node lease cache's self-fence and deadline must count time spent while the
// host is suspended (VM migrate / host sleep). Go's monotonic clock (time.Now()'s monotonic reading,
// time.Since) is CLOCK_MONOTONIC, which FREEZES during suspend — a holder using it could resume spending
// a lease the grantor already reclaimed (re-opens H-B/H-S1, same money live twice). CLOCK_BOOTTIME counts
// suspended time, so the self-fence fires on resume. Do NOT replace this with time.Now() on Linux.
//
// On the (vanishingly rare) syscall failure, fall back to 0 so the caller's lastSeenMon != 0 guard treats
// the first reading as un-anchored rather than crashing; a persistent failure degrades to a non-advancing
// clock, which only makes the fence MORE eager (it never serves past a stale lease) — the safe direction.
func nowMono() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0
	}
	return ts.Nano()
}
