package wavespan

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestRefillInstallsNextAndPromotes proves the double-buffer refill (§4.2): a low-watermark Spend draws
// the next chunk (installing it with a deadline stamped from the grant echo and setting the self-fence
// budget), a buffered next is not re-drawn, and next is promoted to cur when cur drains.
func TestRefillInstallsNextAndPromotes(t *testing.T) {
	clk := newFakeClock(1000)
	cell := &budgetCell{
		now:          clk.now,
		wallNow:      func() int64 { return 0 },
		chunk:        100,
		lowWatermark: 60,
		cur:          &leaseChunk{remaining: 50, deadlineMon: 1 << 62, leaseID: []byte("L0")},
	}
	var draws int
	cell.drawFn = func(_ context.Context, _ []byte, amt int64) (drawResult, error) {
		draws++
		return drawResult{granted: amt, grantedMs: 0, ttlMs: 30_000, selfGuardMs: 700, maxPauseMs: 2000}, nil
	}
	cell.reportFn = func(_ context.Context, _ []byte, _ int64) error { return nil }
	cell.triggerRefill = func() { cell.refillOnce(context.Background()) } // synchronous for determinism

	// A spend at the low watermark draws next but does NOT promote (cur still has units).
	if err := cell.Spend(10); err != nil {
		t.Fatal(err)
	}
	if draws != 1 {
		t.Fatalf("draws = %d, want 1", draws)
	}
	if cell.next == nil || cell.next.remaining != 100 {
		t.Fatalf("next not installed: %+v", cell.next)
	}
	if cell.cur == nil || cell.cur.remaining != 40 {
		t.Fatalf("cur = %+v, want remaining 40", cell.cur)
	}
	if cell.maxPauseNs != 2000*nsPerMs {
		t.Fatalf("maxPauseNs = %d, want %d", cell.maxPauseNs, 2000*nsPerMs)
	}
	wantDeadline := int64(1000) + (30_000-700)*nsPerMs
	if cell.next.deadlineMon != wantDeadline {
		t.Fatalf("next deadline = %d, want %d", cell.next.deadlineMon, wantDeadline)
	}

	// Draining cur must not re-draw (next already buffered).
	if err := cell.Spend(40); err != nil {
		t.Fatal(err)
	}
	if draws != 1 {
		t.Fatalf("draws = %d after draining, want 1 (next already buffered)", draws)
	}
	// The next Spend promotes next -> cur.
	if err := cell.Spend(1); err != nil {
		t.Fatal(err)
	}
	if cell.cur == nil || cell.cur.remaining != 99 {
		t.Fatalf("after promote cur = %+v, want remaining 99", cell.cur)
	}
	if cell.next != nil {
		t.Fatalf("next = %+v, want nil after promote", cell.next)
	}
	if cell.cur.deadlineMon != wantDeadline {
		t.Fatalf("promoted deadline = %d, want %d", cell.cur.deadlineMon, wantDeadline)
	}
}

// TestRefillFreshnessGateRejectsStaleGrant proves the §16 edit #2 gate: a grant whose transit exceeded
// self_guard is dropped and a fresh lease is redrawn (a different nonce), so the local deadline is only
// ever anchored on a fresh receipt.
func TestRefillFreshnessGateRejectsStaleGrant(t *testing.T) {
	clk := newFakeClock(5000)
	const wall int64 = 1_000_000 // holder wall clock (ms)
	cell := &budgetCell{
		now:     clk.now,
		wallNow: func() int64 { return wall },
		chunk:   100,
		holder:  []byte("nodeA"),
	}
	var draws int
	var firstID, secondID []byte
	cell.drawFn = func(_ context.Context, id []byte, amt int64) (drawResult, error) {
		draws++
		if draws == 1 {
			firstID = append([]byte{}, id...)
			// stale: transit (wall - granted_ms) = 701 > self_guard 700
			return drawResult{granted: amt, grantedMs: wall - 701, ttlMs: 30_000, selfGuardMs: 700, maxPauseMs: 2000}, nil
		}
		secondID = append([]byte{}, id...)
		return drawResult{granted: amt, grantedMs: wall, ttlMs: 30_000, selfGuardMs: 700, maxPauseMs: 2000}, nil
	}
	cell.reportFn = func(_ context.Context, _ []byte, _ int64) error { return nil }

	cell.refillOnce(context.Background())
	if draws != 2 {
		t.Fatalf("draws = %d, want 2 (stale grant rejected, redrew)", draws)
	}
	// With no prior cur, the freshly drawn chunk promotes straight to cur.
	if cell.cur == nil || cell.cur.remaining != 100 {
		t.Fatalf("fresh grant not installed: cur=%+v next=%+v", cell.cur, cell.next)
	}
	if string(firstID) == string(secondID) {
		t.Fatalf("redraw reused the stale lease_id %x; want a fresh nonce", firstID)
	}
}

// TestRefillSingleFlight proves concurrent low-watermark triggers issue exactly one in-flight Draw.
func TestRefillSingleFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var draws atomic.Int64
	cell := &budgetCell{
		now:     func() int64 { return 1 },
		wallNow: func() int64 { return 0 },
		chunk:   100,
	}
	cell.drawFn = func(_ context.Context, _ []byte, amt int64) (drawResult, error) {
		draws.Add(1)
		started <- struct{}{} // signal we entered the Draw
		<-release             // block until released
		return drawResult{granted: amt, ttlMs: 0}, nil
	}
	cell.reportFn = func(_ context.Context, _ []byte, _ int64) error { return nil }
	cell.triggerRefill = cell.requestRefill

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); cell.requestRefill() }()
	}
	wg.Wait() // every requestRefill returned (the winner launched a goroutine; losers bailed on the CAS)
	<-started // the single in-flight Draw has begun

	if got := draws.Load(); got != 1 {
		t.Fatalf("concurrent Draws = %d, want 1 (single-flight)", got)
	}
	close(release) // let it finish

	// Bounded spin (no sleep) for the install + single-flight reset.
	for i := 0; i < 1_000_000 && cell.refilling.Load(); i++ {
		runtime.Gosched()
	}
	cell.mu.Lock()
	cur := cell.cur
	cell.mu.Unlock()
	if cur == nil || cur.remaining != 100 {
		t.Fatalf("chunk not installed after refill: %+v", cur)
	}
}
