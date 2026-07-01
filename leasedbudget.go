package wavespan

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
	"google.golang.org/grpc/codes"
)

// LeasedBudget node-side cache (design/35 Stage 2, §4). An adserver Acquires a budget once and Spends
// against an in-memory token/lease cache with ZERO Raft per spend; consensus is touched ~once per refill.
// A crashed/paused/partitioned holder can never cause STRICT overspend (worst case = underspend), because
// the holder stops EARLY on a single suspend-aware monotonic clock (CLOCK_BOOTTIME) while the grantor
// reclaims LATE on a replicated logical deadline. This is the holder surface; the Stage-1 BudgetClient is
// the controller surface.

// ErrBudgetUnavailable is returned by Spend when the cached lease has no capacity to satisfy the request
// (drained, expired, or self-fenced). It is a normal back-pressure signal — the caller serves a no-budget
// fallback; underspend is acceptable, overspend never is. A refill is triggered off-path.
var ErrBudgetUnavailable = errors.New("wavespan: budget unavailable (no cached lease capacity)")

// ErrPacingThrottled is returned by Spend when the node-side token bucket has fewer than the requested
// tokens — the spend is paced out, to be retried after tokens accrue. Distinct from ErrBudgetUnavailable:
// capacity exists, but delivery is rate-limited.
var ErrPacingThrottled = errors.New("wavespan: pacing throttled (node token bucket empty)")

// defaultChunkUnits is the per-refill Draw size when WithChunk is not given.
const defaultChunkUnits int64 = 1000

// nsPerSec is the nanoseconds-per-second divisor for the node token bucket (nowMono is in ns).
const nsPerSec int64 = 1_000_000_000

// nsPerMs converts the echoed millisecond timing (ttl/self_guard/max_pause) into nowMono's nanoseconds.
const nsPerMs int64 = 1_000_000

// maxMonoDeadline is the deadline for a non-timed chunk (ttl echoed as 0): effectively never stop. The
// node cache is intended for TIMED budgets; a non-timed grant yields no holder stop and no self-fence.
const maxMonoDeadline int64 = 1<<63 - 1

// maxRefillAttempts bounds the redraw loop (freshness rejection / already-settled) so a pathologically
// skewed clock or a tombstone storm cannot spin forever; on exhaustion the refill gives up and a later
// Spend re-triggers it.
const maxRefillAttempts = 4

// refillTimeout caps a single background refill's RPCs.
const refillTimeout = 10 * time.Second

// BudgetKey identifies a budget pool: its namespace and budget id (the same (namespace, budget) pair the
// Stage-1 BudgetClient addresses).
type BudgetKey struct {
	Namespace string
	Budget    []byte
}

// LeasedBudgetClient is the node-side lease-cache surface. Obtain one via [Client.LeasedBudget].
type LeasedBudgetClient struct {
	c *Client
}

// LeasedBudget returns the node-side LeasedBudget cache client (§4). Distinct from [Client.Budget], which
// is the controller surface (Define/Grant/Report/Return/Stat).
func (c *Client) LeasedBudget() *LeasedBudgetClient { return &LeasedBudgetClient{c: c} }

// acquireOptions configures the node pacing gate and refill chunking for an Acquire. ttl/self_guard/
// max_pause are NOT set here — they come echoed from the grant result at refill (§4.2), so the node never
// hard-codes the server's timing.
type acquireOptions struct {
	rate  int64 // node token-bucket refill rate (units/sec); 0 disables node pacing
	burst int64 // node token-bucket ceiling (units); 0 defaults to chunk
	chunk int64 // per-refill Draw size (units)
}

// AcquireOption customizes an Acquire.
type AcquireOption func(*acquireOptions)

// WithRate sets the node-side token-bucket refill rate (units/sec) that shapes per-spend delivery. Zero
// (the default) disables node pacing — Spend never returns ErrPacingThrottled.
func WithRate(unitsPerSec int64) AcquireOption {
	return func(o *acquireOptions) { o.rate = unitsPerSec }
}

// WithBurst sets the node-side token-bucket ceiling (units). Zero defaults to the chunk size.
func WithBurst(units int64) AcquireOption {
	return func(o *acquireOptions) { o.burst = units }
}

// WithChunk sets the per-refill Draw size (units). Larger chunks touch consensus less often but waste more
// on a crash (bounded by 2·chunk). Zero defaults to defaultChunkUnits.
func WithChunk(units int64) AcquireOption {
	return func(o *acquireOptions) { o.chunk = units }
}

// leaseChunk is one drawn lease held in the node cache: its remaining capacity, its own monotonic stop
// deadline, the units already spent from it (cumulative, reported at refill/return), the granted amount,
// and the server lease_id it was drawn under.
type leaseChunk struct {
	remaining   int64
	deadlineMon int64 // suspend-aware monotonic deadline (ns); Spend stops when now >= deadlineMon
	spent       int64 // units spent from this chunk so far (cumulative)
	reported    int64 // last cumulative spent attested to the server (avoids duplicate reports)
	amount      int64 // units granted for this chunk
	leaseID     []byte
}

// budgetCell is the in-memory lease cache for one Acquired budget: a double-buffered cur/next pair of
// drawn chunks plus a node-side token bucket. All spend decisions read a single suspend-aware monotonic
// clock (injected as now, defaulting to nowMono) under mu, immediately before the decrement (§4.1). The
// lock is NEVER held across an RPC — refills run off-path, single-flight.
type budgetCell struct {
	mu  sync.Mutex
	now func() int64 // injectable clock (ns); nowMono in production, a fake in tests

	cur  *leaseChunk
	next *leaseChunk

	// node pacing token bucket (from Acquire opts; rate == 0 disables pacing).
	rate         int64
	burst        int64
	tokens       int64
	lastTokenMon int64 // monotonic time of the last token accrual (ns)

	chunk        int64 // per-refill Draw size (units)
	lowWatermark int64 // refill trigger threshold on cur.remaining

	// maxPauseNs is the self-fence budget (ns); 0 disables the fence. Set from the grant echo's
	// max_pause_budget_ms at refill (§4.2) — the node does not hard-code it.
	maxPauseNs  int64
	lastSeenMon int64 // monotonic time of the last served spend (0 = un-anchored)

	// triggerRefill kicks an off-path, single-flight refill. Set by Acquire; nil in pure cell unit tests.
	triggerRefill func()

	// --- refill machinery (§4.2) ---
	closed    atomic.Bool // set by Return: Spend stops serving and no further refills start
	refilling atomic.Bool // single-flight guard: at most one in-flight Draw per cell
	holder    []byte      // node identity; the lease_id namespace (holder || be(nonce))
	nonce     uint64      // per-refill nonce; rotated after a committed refill, bumped on redraw

	// last echoed timing (set at install, used to stamp the deadline / informational).
	selfGuardMs int64
	ttlMs       int64

	// injectable RPC hooks so the refill is testable without a server. wallNow is the holder WALL clock
	// (UnixMilli) for the freshness gate — distinct from now (the suspend-aware monotonic clock).
	drawFn   func(ctx context.Context, leaseID []byte, amount int64) (drawResult, error)
	reportFn func(ctx context.Context, leaseID []byte, spent int64) error
	wallNow  func() int64
}

// drawResult is the typed echo of a refill Draw (BudgetGrant): the granted amount plus the effective
// leader-stamped timing the node needs to stamp its deadline, run the freshness gate, and set the fence.
type drawResult struct {
	granted     int64
	grantedMs   int64
	ttlMs       int64
	selfGuardMs int64
	maxPauseMs  int64
}

// errLeaseSettled signals that a Draw's lease_id was already settled server-side (tombstoned); the refill
// mints a fresh nonce and redraws. draw translates a codes.AlreadyExists status into this sentinel.
var errLeaseSettled = errors.New("wavespan: lease already settled")

// Budget is a handle to one Acquired budget's node-side cache. Spend is zero-coordination; Return folds the
// final spend and credits the unspent remainder back on graceful shutdown.
type Budget struct {
	lb   *LeasedBudgetClient
	key  BudgetKey
	cell *budgetCell
}

// Acquire returns a node-side cache for the (namespace, budget) pool. The pacing gate (WithRate/WithBurst)
// and refill chunk size (WithChunk) are node-local; the lease timing (ttl/self_guard/max_pause) is echoed
// from the server on each refill and installed on the cell, so the node never hard-codes the server's
// clock model. The first Spend (or the initial refill wired in a later step) fills the cache.
func (lb *LeasedBudgetClient) Acquire(ctx context.Context, key BudgetKey, opts ...AcquireOption) (*Budget, error) {
	o := acquireOptions{chunk: defaultChunkUnits}
	for _, fn := range opts {
		fn(&o)
	}
	if o.chunk <= 0 {
		o.chunk = defaultChunkUnits
	}
	if o.burst <= 0 {
		o.burst = o.chunk
	}
	cell := &budgetCell{
		now:          nowMono,
		rate:         o.rate,
		burst:        o.burst,
		tokens:       o.burst, // a token bucket starts full (mirrors the server seeding, §3.1)
		lastTokenMon: nowMono(),
		chunk:        o.chunk,
		lowWatermark: lowWatermarkFor(o.chunk),
		holder:       randomHolder(),
		wallNow:      func() int64 { return time.Now().UnixMilli() },
	}
	cell.drawFn = func(ctx context.Context, leaseID []byte, amount int64) (drawResult, error) {
		return lb.draw(ctx, key, cell.holder, amount, leaseID)
	}
	cell.reportFn = func(ctx context.Context, leaseID []byte, spent int64) error {
		return lb.c.Budget().Report(ctx, key.Namespace, key.Budget, leaseID, cell.holder, spent)
	}
	cell.triggerRefill = cell.requestRefill
	b := &Budget{lb: lb, key: key, cell: cell}
	// Fill the cache synchronously so the returned Budget is immediately usable; the caller's ctx bounds
	// this initial Draw. Subsequent refills run off-path on a background context (refillTimeout).
	cell.refillOnce(ctx)
	return b, nil
}

// lowWatermarkFor picks the refill trigger threshold: refill when cur drops below ~25% of a chunk, so the
// next chunk is in flight well before the current one drains (bounds underspend without thrashing).
func lowWatermarkFor(chunk int64) int64 {
	lw := chunk / 4
	if lw < 1 {
		lw = 1
	}
	return lw
}

// Spend consumes n units from the node cache with no RPC and no Raft on the fast path (§4.1). It returns
// nil on success, ErrPacingThrottled when the node token bucket is empty, or ErrBudgetUnavailable when the
// cached lease cannot satisfy the spend (drained / expired / self-fenced), in which case a refill is
// triggered off-path. n <= 0 is a no-op.
func (b *Budget) Spend(n int64) error { return b.cell.Spend(n) }

// Remaining reports the units currently cached across cur and next (a hint, not a guarantee — a concurrent
// Spend or self-fence may change it).
func (b *Budget) Remaining() int64 {
	c := b.cell
	c.mu.Lock()
	defer c.mu.Unlock()
	var r int64
	if c.cur != nil {
		r += c.cur.remaining
	}
	if c.next != nil {
		r += c.next.remaining
	}
	return r
}

// Return settles the node cache on graceful shutdown (§3.6, the attested path). It closes the Budget (no
// further Spend serves, no further refills start), drains any single in-flight refill, then folds each
// held chunk's final cumulative spent into a BudgetReturn so the server CREDITS the true unspent remainder
// back to available. Return is idempotent: a chunk that already auto-expired is a server-side tombstone
// no-op. After Return the Budget must not be reused. The first settlement error (if any) is reported.
func (b *Budget) Return(ctx context.Context) error {
	c := b.cell
	c.closed.Store(true)
	// Let any in-flight single-flight refill finish installing so its chunk is settled here rather than
	// stranded to a forced expiry (shutdown path; the at-most-one refill resolves quickly).
	for i := 0; i < 1_000_000 && c.refilling.Load(); i++ {
		runtime.Gosched()
	}

	c.mu.Lock()
	chunks := make([]*leaseChunk, 0, 2)
	if c.cur != nil {
		chunks = append(chunks, c.cur)
	}
	if c.next != nil {
		chunks = append(chunks, c.next)
	}
	c.cur, c.next = nil, nil
	c.mu.Unlock()

	var firstErr error
	for _, ch := range chunks {
		if len(ch.leaseID) == 0 {
			continue
		}
		if err := b.lb.c.Budget().Return(ctx, b.key.Namespace, b.key.Budget, ch.leaseID, b.cell.holder, ch.spent); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Spend implements the §4.1 fast path EXACTLY in order. now is re-read under the lock immediately before
// the decrement (not latched earlier). The lock is never held across the refill trigger (run after unlock).
func (c *budgetCell) Spend(n int64) error {
	if n <= 0 {
		return nil
	}
	if c.closed.Load() {
		return ErrBudgetUnavailable // Returned: terminal, never re-acquires
	}
	c.mu.Lock()
	now := c.now() // re-read under the lock, immediately before the decrement (§16 #6/#B)

	// (a) self-fence — a monotonic gap past the pause budget means the lease may have been reclaimed
	// during a suspend/pause: drop cur/next and serve nothing. NO report (I-1).
	if c.maxPauseNs > 0 && c.lastSeenMon != 0 && now-c.lastSeenMon > c.maxPauseNs {
		c.cur = nil
		c.next = nil
		c.lastSeenMon = now
		trigger := c.triggerRefill
		c.mu.Unlock()
		if trigger != nil {
			trigger()
		}
		return ErrBudgetUnavailable
	}

	// (b) pacing gate — node token bucket; if fewer tokens than requested have accrued, pace out.
	if c.rate > 0 {
		c.accrueTokensLocked(now)
		if c.tokens < n {
			c.mu.Unlock()
			return ErrPacingThrottled
		}
	}

	// (c) drop cur if it is past its own monotonic deadline (or drained), promoting a buffered chunk.
	c.promoteLocked(now)

	// (d) fast path: decrement the cached chunk, debit a token, and trigger a refill at the low watermark.
	if c.cur != nil && c.cur.remaining >= n {
		c.cur.remaining -= n
		c.cur.spent += n
		if c.rate > 0 {
			c.tokens -= n
		}
		c.lastSeenMon = now
		needRefill := c.cur.remaining < c.lowWatermark && c.next == nil
		trigger := c.triggerRefill
		c.mu.Unlock()
		if needRefill && trigger != nil {
			trigger()
		}
		return nil
	}

	// (e) STRICT empty — nothing cached can satisfy the spend; trigger a refill and report unavailable.
	trigger := c.triggerRefill
	c.mu.Unlock()
	if trigger != nil {
		trigger()
	}
	return ErrBudgetUnavailable
}

// accrueTokensLocked tops up the node token bucket for the monotonic time elapsed since the last accrual,
// capped at burst. Called with mu held. Overflow-safe: a gap longer than a full burst-refill just tops up.
func (c *budgetCell) accrueTokensLocked(now int64) {
	if c.rate <= 0 {
		return
	}
	elapsed := now - c.lastTokenMon
	if elapsed <= 0 {
		// Monotone-forward only; never accrue on a backward/zero step (the injected clock or a coarse
		// timer can repeat a reading).
		c.lastTokenMon = now
		return
	}
	// If the gap would refill more than a full burst, just top up — avoids rate*elapsed overflow.
	fullRefillNs := (c.burst/maxI64(c.rate, 1) + 1) * nsPerSec
	if elapsed >= fullRefillNs {
		c.tokens = c.burst
	} else {
		c.tokens += c.rate * elapsed / nsPerSec
		if c.tokens > c.burst {
			c.tokens = c.burst
		}
	}
	c.lastTokenMon = now
}

func maxI64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// promoteLocked drops cur when it is expired or drained, then promotes a buffered next chunk into the
// empty cur slot (dropping the promotion too if it is already past its deadline). Called with mu held.
func (c *budgetCell) promoteLocked(now int64) {
	if c.cur != nil && (now >= c.cur.deadlineMon || c.cur.remaining == 0) {
		c.cur = nil
	}
	if c.cur == nil && c.next != nil {
		c.cur = c.next
		c.next = nil
		if now >= c.cur.deadlineMon {
			c.cur = nil
		}
	}
}

// requestRefill kicks an off-path, single-flight background refill. If a refill is already in flight it is
// a no-op (the double buffer means at most one extra chunk is ever in flight). Never blocks the caller.
func (c *budgetCell) requestRefill() {
	if c.drawFn == nil || c.closed.Load() {
		return
	}
	if !c.refilling.CompareAndSwap(false, true) {
		return // already refilling: single-flight
	}
	go func() {
		defer c.refilling.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), refillTimeout)
		defer cancel()
		c.refillOnce(ctx)
	}()
}

// refillOnce performs one logical refill (§4.2): attest the draining lease's cumulative spend, Draw the
// next chunk under a stable lease_id (redrawing with a fresh nonce on a freshness rejection or an
// already-settled tombstone), run the freshness gate, then install the chunk and stamp its suspend-aware
// deadline. It holds the cell lock only to snapshot and to install — NEVER across an RPC. Idempotent w.r.t.
// the double buffer: it no-ops when a next chunk is already buffered.
func (c *budgetCell) refillOnce(ctx context.Context) {
	if c.drawFn == nil {
		return
	}
	c.mu.Lock()
	if c.next != nil {
		c.mu.Unlock()
		return // already buffered
	}
	var repID []byte
	var repSpent int64
	if c.cur != nil && len(c.cur.leaseID) > 0 && c.cur.spent > c.cur.reported {
		repID = c.cur.leaseID
		repSpent = c.cur.spent
	}
	chunk := c.chunk
	nonce := c.nonce
	holder := c.holder
	c.mu.Unlock()

	// Attest the draining lease's cumulative spend (best-effort: a failure only delays attestation; a
	// forced expiry debits any unreported tail, so safety never depends on this report — §3.5/I-1).
	if repID != nil && c.reportFn != nil {
		if err := c.reportFn(ctx, repID, repSpent); err == nil {
			c.mu.Lock()
			if c.cur != nil && bytes.Equal(c.cur.leaseID, repID) {
				c.cur.reported = repSpent
			}
			c.mu.Unlock()
		}
	}

	for attempt := 0; attempt < maxRefillAttempts; attempt++ {
		leaseID := makeLeaseID(holder, nonce)
		gr, err := c.drawFn(ctx, leaseID, chunk)
		if err != nil {
			if errors.Is(err, errLeaseSettled) {
				nonce++ // the id is tombstoned: mint a fresh lease and retry
				continue
			}
			return // transient/other: a later Spend re-triggers the refill
		}
		if gr.granted <= 0 {
			return // no capacity right now: underspend, retry later
		}
		// Freshness gate (§16 edit #2): reject a grant whose transit exceeded self_guard — anchoring the
		// local deadline on a stale receipt would be unsound. Drop it (it auto-expires server-side) and
		// redraw with a fresh nonce (the same id would echo the same stale granted_ms forever).
		if gr.ttlMs > 0 && c.wallNow()-gr.grantedMs > gr.selfGuardMs {
			nonce++
			continue
		}
		c.mu.Lock()
		now := c.now()
		deadline := maxMonoDeadline
		if gr.ttlMs > 0 {
			// Stop on the holder's OWN suspend-aware clock at ttl - self_guard (§2). May be non-positive
			// for a misconfigured ttl < self_guard; such a chunk is simply dropped on first Spend (safe).
			deadline = now + (gr.ttlMs-gr.selfGuardMs)*nsPerMs
		}
		c.next = &leaseChunk{remaining: gr.granted, amount: gr.granted, deadlineMon: deadline, leaseID: leaseID}
		if gr.maxPauseMs > 0 {
			c.maxPauseNs = gr.maxPauseMs * nsPerMs
		}
		c.selfGuardMs = gr.selfGuardMs
		c.ttlMs = gr.ttlMs
		c.lastSeenMon = now // re-anchor the fence so a freshly installed chunk is not false-fenced
		c.nonce = nonce + 1 // rotate for the next logical refill (this one committed)
		c.promoteLocked(now)
		c.mu.Unlock()
		return
	}
}

// draw issues a refill BudgetGrant and returns the typed timing echo. A codes.AlreadyExists status (the
// lease_id was settled/tombstoned, §3.7) is translated to errLeaseSettled so the caller mints a fresh id.
// A depleted pool is a normal no_capacity result (granted 0, nil error), not an error.
func (lb *LeasedBudgetClient) draw(ctx context.Context, key BudgetKey, holder []byte, amount int64, leaseID []byte) (drawResult, error) {
	resp, err := lb.c.budget.BudgetGrant(ctx, &wavespanv1.BudgetGrantRequest{
		Namespace:   key.Namespace,
		Budget:      key.Budget,
		HolderId:    string(holder),
		AmountUnits: amount,
		LeaseId:     leaseID,
		// TtlMs 0 ⇒ the server applies the budget's DefaultTtlMs and echoes the effective value.
	})
	if err != nil {
		if CodeOf(err) == codes.AlreadyExists {
			return drawResult{}, errLeaseSettled
		}
		return drawResult{}, wrapErr("BudgetGrant", err)
	}
	return drawResult{
		granted:     resp.GetGrantedUnits(),
		grantedMs:   resp.GetGrantedMs(),
		ttlMs:       resp.GetTtlMs(),
		selfGuardMs: resp.GetSelfGuardMs(),
		maxPauseMs:  resp.GetMaxPauseBudgetMs(),
	}, nil
}

// makeLeaseID derives the stable per-refill lease_id as holder || be(nonce). Stable across RPC retries of
// one logical refill (exactly-once with the 2b tombstone); unique per refill (the nonce rotates) and per
// node (the random holder prefix).
func makeLeaseID(holder []byte, nonce uint64) []byte {
	id := make([]byte, len(holder)+8)
	copy(id, holder)
	binary.BigEndian.PutUint64(id[len(holder):], nonce)
	return id
}

// randomHolder mints a per-process-per-Budget node identity that namespaces this holder's lease ids. It is
// hex-encoded so it is a valid proto3 string (holder_id) — random binary would fail UTF-8 marshaling.
func randomHolder() []byte {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		// Best-effort fallback; lease-id uniqueness within a budget still holds via the rotating nonce.
		binary.BigEndian.PutUint64(raw, uint64(time.Now().UnixNano()))
	}
	enc := make([]byte, hex.EncodedLen(len(raw)))
	hex.Encode(enc, raw)
	return enc
}
