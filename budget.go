package wavespan

import (
	"context"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
)

// BudgetMode selects a budget pool's escrow discipline (design/35). Stage 1 ships STRICT only; RELAXED
// is reserved for a later stage. The zero value (Unspecified) is invalid for [BudgetClient.Define].
type BudgetMode int32

const (
	// BudgetModeUnspecified is the invalid zero value; Define rejects it with InvalidArgument.
	BudgetModeUnspecified BudgetMode = BudgetMode(wavespanv1.BudgetMode_BUDGET_MODE_UNSPECIFIED)
	// BudgetModeStrict enforces cap == available + leasedOut + spent on every entry (the only Stage-1 mode).
	BudgetModeStrict BudgetMode = BudgetMode(wavespanv1.BudgetMode_BUDGET_MODE_STRICT)
	// BudgetModeRelaxed is reserved for a later stage and rejected by a Stage-1 server.
	BudgetModeRelaxed BudgetMode = BudgetMode(wavespanv1.BudgetMode_BUDGET_MODE_RELAXED)
)

// BudgetClient is the ergonomic client for the LeasedBudget escrow API (design/35, Stage 1: single-cluster
// STRICT). A budget is a pool of int64 micro-units leased out, spent against, and returned under the
// conservation invariant cap == available + leasedOut + spent. Mutations are linearizable through the
// owning shard's leader (like [CollectionsClient]); Stat reads default to bounded-stale local reads — pass
// linearizable=true for a quorum read. Obtain one via [Client.Budget].
type BudgetClient struct {
	c    *Client
	idem string
}

// Budget returns the LeasedBudget sub-client.
func (c *Client) Budget() *BudgetClient { return &BudgetClient{c: c} }

// WithIdempotencyKey returns a sub-client whose next Define carries the given idempotency key, so a retry
// (after a timeout) applies exactly once and returns the original result. Grant/Report/Return are made
// idempotent by their lease_id instead, so this key only affects Define. Use a fresh key per logical write.
func (bc *BudgetClient) WithIdempotencyKey(key string) *BudgetClient {
	clone := *bc
	clone.idem = key
	return &clone
}

func (bc *BudgetClient) idemPtr() *string {
	if bc.idem == "" {
		return nil
	}
	return &bc.idem
}

// writeClient returns the BudgetService client a write to (namespace, budget) should use. Budget mutations
// are linearizable through the owning shard's leader; shard-aware direct routing is currently
// collections-only (the router holds CollectionService clients), so budget writes use the default
// endpoint and rely on the server forwarding to the leader. The hook mirrors [CollectionsClient] so
// direct routing can be wired in later without changing call sites.
func (bc *BudgetClient) writeClient(_ context.Context, _ string, _ []byte) wavespanv1.BudgetServiceClient {
	return bc.c.budget
}

// noteWriteErr lets the shard-aware router observe a write error so it refreshes its routing table for the
// next op (budget and collection writes share the same consensus-tier leaders, so a leadership change seen
// here is relevant to both). It is a no-op when routing is disabled, and returns err unchanged.
func (bc *BudgetClient) noteWriteErr(ctx context.Context, err error) error {
	if bc.c.router != nil {
		return bc.c.router.noteError(ctx, err)
	}
	return err
}

// BudgetStat is a pool's accounting snapshot, holding the conservation invariant
// cap == available + leasedOut + spent (when the pool exists).
type BudgetStat struct {
	Exists         bool
	CapUnits       int64
	AvailableUnits int64
	LeasedOutUnits int64
	SpentUnits     int64
	// SpentReportedUnits is the cumulative spend actually attested by holders (<= SpentUnits). The gap
	// SpentUnits - SpentReportedUnits is the maximum recoverable stranding from forced expiries.
	SpentReportedUnits int64
	Epoch              uint64
	Mode               BudgetMode
}

// BudgetDefineOptions carries the Stage-2 pacing + timing config for Define. The zero value reproduces
// the Stage-1 non-paced, non-expiring budget. A timed budget (DefaultTtlMs > 0) requires SelfGuardMs and
// DedupRetryWindowMs above the server's safety floors, else Define fails with InvalidArgument (I2/I3).
type BudgetDefineOptions struct {
	RateUnitsPerSec    int64
	BurstUnits         int64
	SelfGuardMs        int64
	MaxPauseMs         int64
	DefaultTTLMs       int64
	DedupRetryWindowMs int64
}

// Define creates a budget pool with the given cap and mode (Stage 1: STRICT only). An invalid cap, a
// non-STRICT mode, or an out-of-bounds pacing/timing param fails with InvalidArgument; defining an
// existing pool fails with FailedPrecondition. Pass nil opts for a non-paced, non-expiring budget.
func (bc *BudgetClient) Define(ctx context.Context, namespace string, budget []byte, capUnits int64, mode BudgetMode, opts *BudgetDefineOptions) error {
	req := &wavespanv1.BudgetDefineRequest{
		Namespace: namespace, Budget: budget, CapUnits: capUnits,
		Mode: wavespanv1.BudgetMode(mode), IdempotencyKey: bc.idemPtr(),
	}
	if opts != nil {
		req.RateUnitsPerSec = opts.RateUnitsPerSec
		req.BurstUnits = opts.BurstUnits
		req.SelfGuardMs = opts.SelfGuardMs
		req.MaxPauseMs = opts.MaxPauseMs
		req.DefaultTtlMs = opts.DefaultTTLMs
		req.DedupRetryWindowMs = opts.DedupRetryWindowMs
	}
	if _, err := bc.writeClient(ctx, namespace, budget).BudgetDefine(ctx, req); err != nil {
		return wrapErr("BudgetDefine", bc.noteWriteErr(ctx, err))
	}
	return nil
}

// Grant atomically leases up to amountUnits from the pool to holder, returning the units actually granted
// (saturated at the pool's available units). A STRICT pool with nothing left returns 0 and a nil error
// (no capacity is a normal result, not an error). leaseID makes the grant idempotent for the lease's
// lifetime — a retry with the same leaseID returns the original grant. A wrong-datatype target fails with
// FailedPrecondition (WRONGTYPE).
func (bc *BudgetClient) Grant(ctx context.Context, namespace string, budget, holder []byte, amountUnits int64, leaseID []byte) (int64, error) {
	resp, err := bc.writeClient(ctx, namespace, budget).BudgetGrant(ctx, &wavespanv1.BudgetGrantRequest{
		Namespace: namespace, Budget: budget, HolderId: string(holder), AmountUnits: amountUnits, LeaseId: leaseID,
	})
	if err != nil {
		return 0, wrapErr("BudgetGrant", bc.noteWriteErr(ctx, err))
	}
	return resp.GetGrantedUnits(), nil
}

// Report folds a cumulative-per-lease spent total into the pool (idempotent max fold): spentCumulative is
// the lease's total spend so far, not a delta, so a retry or out-of-order report is safe. holder binds the
// report to the lease's grantee — pass the same holder used at Grant; a mismatch fails with
// PermissionDenied. Pass nil to omit the check (lenient, back-compat).
func (bc *BudgetClient) Report(ctx context.Context, namespace string, budget, leaseID, holder []byte, spentCumulative int64) error {
	_, err := bc.writeClient(ctx, namespace, budget).BudgetReport(ctx, &wavespanv1.BudgetReportRequest{
		Namespace: namespace, Budget: budget, LeaseId: leaseID, HolderId: string(holder), SpentCumulative: spentCumulative,
	})
	if err != nil {
		return wrapErr("BudgetReport", bc.noteWriteErr(ctx, err))
	}
	return nil
}

// Return releases a lease's unspent remainder (folding spentCumulative as the lease's final spend) and
// deletes the lease row, returning its leased-out units to the pool's available balance. holder binds the
// return to the lease's grantee (same match-or-PermissionDenied rule as Report; nil is lenient).
func (bc *BudgetClient) Return(ctx context.Context, namespace string, budget, leaseID, holder []byte, spentCumulative int64) error {
	_, err := bc.writeClient(ctx, namespace, budget).BudgetReturn(ctx, &wavespanv1.BudgetReturnRequest{
		Namespace: namespace, Budget: budget, LeaseId: leaseID, HolderId: string(holder), SpentCumulative: spentCumulative,
	})
	if err != nil {
		return wrapErr("BudgetReturn", bc.noteWriteErr(ctx, err))
	}
	return nil
}

// Reconcile re-credits a budget to its authoritative external Σ-acked spend, recovering the headroom a
// forced lease expiry stranded as underspend — without overspend (design/35 §3.8). This is the CONTROLLER
// surface: pass the cumulative Σ-acked total from the external impression/billing ledger; the server clamps
// it to [spentReported, cap] (never under-credits provably-reported spend) and books it as the pool's
// spend. It returns the recovered amount (old spent - new spent; negative if the external total booked more
// than the pool had). A reconcile against an undefined pool fails with FailedPrecondition.
func (bc *BudgetClient) Reconcile(ctx context.Context, namespace string, budget []byte, trueAckedUnits int64) (int64, error) {
	resp, err := bc.writeClient(ctx, namespace, budget).BudgetReconcile(ctx, &wavespanv1.BudgetReconcileRequest{
		Namespace: namespace, Budget: budget, TrueAckedUnits: trueAckedUnits, IdempotencyKey: bc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("BudgetReconcile", bc.noteWriteErr(ctx, err))
	}
	return resp.GetRecoveredUnits(), nil
}

// Stat reads the pool accounting (bounded-stale local read unless linearizable=true, which forces a quorum
// read). When the pool does not exist, the returned BudgetStat has Exists=false and zero accounting.
func (bc *BudgetClient) Stat(ctx context.Context, namespace string, budget []byte, linearizable bool) (BudgetStat, error) {
	resp, err := bc.c.budget.BudgetStat(ctx, &wavespanv1.BudgetStatRequest{
		Namespace: namespace, Budget: budget, Linearizable: linearizable,
	})
	if err != nil {
		return BudgetStat{}, wrapErr("BudgetStat", err)
	}
	return BudgetStat{
		Exists:             resp.GetExists(),
		CapUnits:           resp.GetCapUnits(),
		AvailableUnits:     resp.GetAvailableUnits(),
		LeasedOutUnits:     resp.GetLeasedOutUnits(),
		SpentUnits:         resp.GetSpentUnits(),
		SpentReportedUnits: resp.GetSpentReportedUnits(),
		Epoch:              resp.GetEpoch(),
		Mode:               BudgetMode(resp.GetMode()),
	}, nil
}
