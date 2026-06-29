package wavespan

import (
	"sync/atomic"
	"testing"
)

// fakeClock is a deterministic, goroutine-safe stand-in for nowMono so the cell's self-fence, deadline,
// and pacing logic can be driven exactly in tests (no real time, no flakiness).
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock(t int64) *fakeClock {
	c := &fakeClock{}
	c.ns.Store(t)
	return c
}

func (c *fakeClock) now() int64  { return c.ns.Load() }
func (c *fakeClock) set(v int64) { c.ns.Store(v) }

// newTestCell builds a cell with a single installed cur chunk, an injected clock, and pacing disabled
// (rate 0). maxPauseNs == 0 disables the self-fence.
func newTestCell(clk *fakeClock, maxPauseNs, curRemaining, deadline int64) *budgetCell {
	return &budgetCell{
		now:        clk.now,
		maxPauseNs: maxPauseNs,
		cur:        &leaseChunk{remaining: curRemaining, deadlineMon: deadline},
	}
}

// TestSpendDecrementsLocally proves the zero-coordination fast path: an in-budget Spend just decrements
// the cached chunk (no RPC), and draining it returns ErrBudgetUnavailable while triggering a refill (§4.1).
func TestSpendDecrementsLocally(t *testing.T) {
	clk := newFakeClock(1000)
	cell := newTestCell(clk, 0, 100, 1<<62) // self-fence off, deadline far
	cell.lowWatermark = 0                    // only the empty path triggers a refill (deterministic count)
	var refills int
	cell.triggerRefill = func() { refills++ }

	for i := 0; i < 10; i++ {
		if err := cell.Spend(10); err != nil {
			t.Fatalf("spend %d: %v", i, err)
		}
	}
	if cell.cur.remaining != 0 {
		t.Fatalf("remaining = %d, want 0", cell.cur.remaining)
	}
	if err := cell.Spend(10); err != ErrBudgetUnavailable {
		t.Fatalf("empty spend = %v, want ErrBudgetUnavailable", err)
	}
	if refills == 0 {
		t.Fatal("expected a refill trigger when the chunk drained")
	}
}

// TestSpendSelfFencesOnPause proves the suspend self-fence (§2 C1 / §4.1 a): a monotonic gap past the
// pause budget drops cur/next and refuses to serve — without emitting a report (I-1).
func TestSpendSelfFencesOnPause(t *testing.T) {
	clk := newFakeClock(1000)
	cell := newTestCell(clk, 500, 100, 10_000) // maxPauseNs 500
	if err := cell.Spend(1); err != nil {
		t.Fatal(err)
	} // lastSeenMon = 1000
	clk.set(1000 + 600) // jump > maxPause(500): simulated suspend
	if err := cell.Spend(1); err != ErrBudgetUnavailable {
		t.Fatalf("self-fence: got %v want ErrBudgetUnavailable", err)
	}
	if cell.cur != nil {
		t.Fatal("cur not dropped on self-fence")
	}
	if cell.next != nil {
		t.Fatal("next not dropped on self-fence")
	}
}

// TestSpendStopsAtDeadline proves the holder hard-stops at its own monotonic deadline (§2 / §4.1 c).
func TestSpendStopsAtDeadline(t *testing.T) {
	clk := newFakeClock(100)
	cell := newTestCell(clk, 0, 100, 5000) // deadline 5000
	if err := cell.Spend(1); err != nil {
		t.Fatalf("pre-deadline spend: %v", err)
	}
	clk.set(5000) // now == deadline
	if err := cell.Spend(1); err != ErrBudgetUnavailable {
		t.Fatalf("at deadline: got %v want ErrBudgetUnavailable", err)
	}
	if cell.cur != nil {
		t.Fatal("cur not dropped at deadline")
	}
}

// TestSpendPacingThrottled proves the node token bucket gates delivery (§4.1 b): when fewer tokens than
// requested have accrued, Spend returns ErrPacingThrottled without touching the cached chunk.
func TestSpendPacingThrottled(t *testing.T) {
	clk := newFakeClock(1_000_000)
	cell := newTestCell(clk, 0, 1000, 1<<62)
	cell.rate = 100 // units/sec
	cell.burst = 50
	cell.tokens = 5 // fewer than the 10 requested
	cell.lastTokenMon = clk.now()

	if err := cell.Spend(10); err != ErrPacingThrottled {
		t.Fatalf("pacing: got %v want ErrPacingThrottled", err)
	}
	if cell.cur.remaining != 1000 {
		t.Fatalf("throttled spend touched the chunk: remaining = %d, want 1000", cell.cur.remaining)
	}
	// A spend within the token budget still passes and consumes tokens.
	if err := cell.Spend(5); err != nil {
		t.Fatalf("in-budget spend: %v", err)
	}
	if cell.tokens != 0 {
		t.Fatalf("tokens = %d, want 0", cell.tokens)
	}
	if cell.cur.remaining != 995 {
		t.Fatalf("remaining = %d, want 995", cell.cur.remaining)
	}
}
