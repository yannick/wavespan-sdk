package wavespan

import "testing"

// TestNowMonoAdvancesAndIsMonotonic checks the suspend-aware monotonic clock that the node lease cache's
// self-fence and deadline stamping ride on (§2 C1): two successive reads are non-decreasing and nonzero.
// On Linux this exercises CLOCK_BOOTTIME; elsewhere the best-effort monotonic fallback.
func TestNowMonoAdvancesAndIsMonotonic(t *testing.T) {
	a := nowMono()
	if a <= 0 {
		t.Fatalf("nowMono() = %d, want > 0", a)
	}
	// Do a little work so a coarse clock has a chance to tick; we only require non-decreasing.
	sum := 0
	for i := 0; i < 1_000_000; i++ {
		sum += i
	}
	_ = sum
	b := nowMono()
	if b < a {
		t.Fatalf("nowMono() not monotonic: second read %d < first %d", b, a)
	}
}
