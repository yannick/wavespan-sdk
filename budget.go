package wavespan

import (
	"context"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
)

// BudgetClient is the ergonomic client for the LeasedBudget escrow API (design/35, Stage 1:
// single-cluster STRICT). A budget is a pool of int64 micro-units that is leased out, spent against,
// and returned, holding the conservation invariant cap == available + leasedOut + spent on every
// entry. Mutations are linearizable through the owning shard's leader; reads default to bounded-stale
// — pass linearizable=true for a quorum read. Obtain one via [Client.Budget].
type BudgetClient struct {
	c    *Client
	idem string
}

// Budget returns the LeasedBudget sub-client.
func (c *Client) Budget() *BudgetClient { return &BudgetClient{c: c} }

// WithIdempotencyKey returns a sub-client whose next Define carries the given idempotency key, so a
// retry (after a timeout) applies exactly once and returns the original result. Grant/Report/Return
// are made idempotent by their lease_id instead, so this key only affects Define. Use a fresh key per
// logical write.
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

// BudgetMode selects the escrow discipline. Stage 1 ships STRICT only; RELAXED is reserved for
// Stage 4.
type BudgetMode = wavespanv1.BudgetMode

const (
	// BudgetModeUnspecified is the zero value; passing it to Define fails with InvalidArgument.
	BudgetModeUnspecified BudgetMode = wavespanv1.BudgetMode_BUDGET_MODE_UNSPECIFIED
	// BudgetModeStrict is the single-cluster strict-conservation pool (the only Stage 1 mode).
	BudgetModeStrict BudgetMode = wavespanv1.BudgetMode_BUDGET_MODE_STRICT
	// BudgetModeRelaxed is reserved for Stage 4 and rejected in Stage 1.
	BudgetModeRelaxed BudgetMode = wavespanv1.BudgetMode_BUDGET_MODE_RELAXED
)

// BudgetStat is a pool's accounting snapshot. The conservation invariant holds: CapUnits ==
// AvailableUnits + LeasedOutUnits + SpentUnits.
type BudgetStat struct {
	Exists         bool       // false = no such pool (the other fields are zero)
	CapUnits       int64      // total pool capacity
	AvailableUnits int64      // units free to be granted
	LeasedOutUnits int64      // units currently held by live leases, not yet reported spent
	SpentUnits     int64      // units reported spent (folded as a max per lease)
	Epoch          uint64     // monotonic pool epoch (bumped on each mutation)
	Mode           BudgetMode // escrow discipline (STRICT in Stage 1)
}

func budgetStat(r *wavespanv1.BudgetStatResult) BudgetStat {
	return BudgetStat{
		Exists:         r.GetExists(),
		CapUnits:       r.GetCapUnits(),
		AvailableUnits: r.GetAvailableUnits(),
		LeasedOutUnits: r.GetLeasedOutUnits(),
		SpentUnits:     r.GetSpentUnits(),
		Epoch:          r.GetEpoch(),
		Mode:           r.GetMode(),
	}
}

// Define creates a STRICT pool with the given cap. A non-STRICT mode or invalid cap fails with
// InvalidArgument; defining an existing pool fails with FailedPrecondition.
func (bc *BudgetClient) Define(ctx context.Context, namespace string, budget []byte, capUnits int64, mode BudgetMode) error {
	_, err := bc.c.budget.BudgetDefine(ctx, &wavespanv1.BudgetDefineRequest{
		Namespace: namespace, Budget: budget, CapUnits: capUnits, Mode: mode, IdempotencyKey: bc.idemPtr(),
	})
	if err != nil {
		return wrapErr("BudgetDefine", err)
	}
	return nil
}

// Grant atomically leases up to amountUnits to holder, returning the units actually granted (which is
// saturated at the pool's available units, so it may be less than requested — zero when the STRICT
// pool had nothing to give). leaseID makes the grant idempotent for the lease's lifetime
// (single-use-forever in Stage 1).
func (bc *BudgetClient) Grant(ctx context.Context, namespace string, budget, holder []byte, amountUnits int64, leaseID []byte) (int64, error) {
	resp, err := bc.c.budget.BudgetGrant(ctx, &wavespanv1.BudgetGrantRequest{
		Namespace: namespace, Budget: budget, HolderId: string(holder), AmountUnits: amountUnits, LeaseId: leaseID,
	})
	if err != nil {
		return 0, wrapErr("BudgetGrant", err)
	}
	return resp.GetGrantedUnits(), nil
}

// Report folds a cumulative-per-lease spent total into the pool (idempotent max fold), returning the
// pool's accounting after the fold.
func (bc *BudgetClient) Report(ctx context.Context, namespace string, budget, leaseID []byte, spentCumulative int64) error {
	_, err := bc.c.budget.BudgetReport(ctx, &wavespanv1.BudgetReportRequest{
		Namespace: namespace, Budget: budget, LeaseId: leaseID, SpentCumulative: spentCumulative,
	})
	if err != nil {
		return wrapErr("BudgetReport", err)
	}
	return nil
}

// Return releases a lease's unspent remainder (folding spentCumulative as a final spent total) and
// deletes the lease row.
func (bc *BudgetClient) Return(ctx context.Context, namespace string, budget, leaseID []byte, spentCumulative int64) error {
	_, err := bc.c.budget.BudgetReturn(ctx, &wavespanv1.BudgetReturnRequest{
		Namespace: namespace, Budget: budget, LeaseId: leaseID, SpentCumulative: spentCumulative,
	})
	if err != nil {
		return wrapErr("BudgetReturn", err)
	}
	return nil
}

// Stat reads the pool accounting (bounded-stale unless linearizable). The returned BudgetStat has
// Exists=false when no such pool is defined.
func (bc *BudgetClient) Stat(ctx context.Context, namespace string, budget []byte, linearizable bool) (BudgetStat, error) {
	resp, err := bc.c.budget.BudgetStat(ctx, &wavespanv1.BudgetStatRequest{
		Namespace: namespace, Budget: budget, Linearizable: linearizable,
	})
	if err != nil {
		return BudgetStat{}, wrapErr("BudgetStat", err)
	}
	return budgetStat(resp), nil
}
