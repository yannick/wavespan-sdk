package wavespan

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// fakeBudgetSrv is a conservation-faithful in-process BudgetService used to exercise the node lease
// cache (Acquire/Spend/Return/Stat) end-to-end over real gRPC, without the server module (sdk/go depends
// only on protobuf + grpc). It preserves INV-LOCAL (cap == available + leasedOut + spent) on every entry
// and mirrors the real settlement asymmetry: graceful Return CREDITS the attested remainder. The full
// real-Raft-shard nemesis soak is the Stage-2e cross-module harness, not this unit-scope test.
type fakeBudgetSrv struct {
	wavespanv1.UnimplementedBudgetServiceServer
	mu    sync.Mutex
	pools map[string]*fakePool
}

type fakePool struct {
	capU, available, leasedOut, spent int64
	selfGuard, maxPause, defaultTTL   int64
	leases                            map[string]*fakeLease
	tombs                             map[string]bool
}

type fakeLease struct {
	amount, spent, grantedMs, ttl int64
	holder                        string // grantee identity; report/return must match it when both are non-empty
}

// holderMismatch mirrors the server's lenient holder-match guard: a non-empty caller holder that
// contradicts the lease's recorded holder is rejected; an empty on either side is lenient.
func holderMismatch(leaseHolder, caller string) bool {
	return leaseHolder != "" && caller != "" && leaseHolder != caller
}

func fakeKey(ns string, b []byte) string { return ns + "\x00" + string(b) }

func (s *fakeBudgetSrv) statLocked(k string) *wavespanv1.BudgetStatResult {
	p := s.pools[k]
	return &wavespanv1.BudgetStatResult{
		Exists: true, CapUnits: p.capU, AvailableUnits: p.available,
		LeasedOutUnits: p.leasedOut, SpentUnits: p.spent,
		Mode: wavespanv1.BudgetMode_BUDGET_MODE_STRICT,
	}
}

func (s *fakeBudgetSrv) BudgetDefine(_ context.Context, m *wavespanv1.BudgetDefineRequest) (*wavespanv1.BudgetStatResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := fakeKey(m.GetNamespace(), m.GetBudget())
	if _, ok := s.pools[k]; ok {
		return nil, status.Error(codes.FailedPrecondition, "budget exists")
	}
	s.pools[k] = &fakePool{
		capU: m.GetCapUnits(), available: m.GetCapUnits(),
		selfGuard: m.GetSelfGuardMs(), maxPause: m.GetMaxPauseMs(), defaultTTL: m.GetDefaultTtlMs(),
		leases: map[string]*fakeLease{}, tombs: map[string]bool{},
	}
	return s.statLocked(k), nil
}

func (s *fakeBudgetSrv) BudgetGrant(_ context.Context, m *wavespanv1.BudgetGrantRequest) (*wavespanv1.BudgetGrantResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := fakeKey(m.GetNamespace(), m.GetBudget())
	p := s.pools[k]
	if p == nil {
		return nil, status.Error(codes.FailedPrecondition, "no budget")
	}
	lid := string(m.GetLeaseId())
	if p.tombs[lid] {
		return nil, status.Error(codes.AlreadyExists, "lease settled")
	}
	if l := p.leases[lid]; l != nil { // idempotent echo (same timing, §3.7 Gap#2)
		return &wavespanv1.BudgetGrantResult{
			GrantedUnits: l.amount, GrantedMs: l.grantedMs, TtlMs: l.ttl,
			SelfGuardMs: p.selfGuard, MaxPauseBudgetMs: p.maxPause,
		}, nil
	}
	grant := min(m.GetAmountUnits(), p.available)
	if grant <= 0 {
		return &wavespanv1.BudgetGrantResult{NoCapacity: true}, nil
	}
	ttl := m.GetTtlMs()
	if ttl == 0 {
		ttl = p.defaultTTL
	}
	gms := time.Now().UnixMilli()
	p.available -= grant
	p.leasedOut += grant
	p.leases[lid] = &fakeLease{amount: grant, grantedMs: gms, ttl: ttl, holder: m.GetHolderId()}
	return &wavespanv1.BudgetGrantResult{
		GrantedUnits: grant, Partial: grant < m.GetAmountUnits(),
		GrantedMs: gms, TtlMs: ttl, SelfGuardMs: p.selfGuard, MaxPauseBudgetMs: p.maxPause,
	}, nil
}

func (s *fakeBudgetSrv) foldReportLocked(p *fakePool, l *fakeLease, reported int64) {
	if reported > l.spent {
		if reported > l.amount {
			reported = l.amount
		}
		d := reported - l.spent
		l.spent = reported
		p.leasedOut -= d
		p.spent += d
	}
}

func (s *fakeBudgetSrv) BudgetReport(_ context.Context, m *wavespanv1.BudgetReportRequest) (*wavespanv1.BudgetStatResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := fakeKey(m.GetNamespace(), m.GetBudget())
	p := s.pools[k]
	if p == nil {
		return nil, status.Error(codes.FailedPrecondition, "no budget")
	}
	l := p.leases[string(m.GetLeaseId())]
	if l == nil {
		return nil, status.Error(codes.FailedPrecondition, "no lease")
	}
	if holderMismatch(l.holder, m.GetHolderId()) {
		return nil, status.Error(codes.PermissionDenied, "holder mismatch")
	}
	s.foldReportLocked(p, l, m.GetSpentCumulative())
	return s.statLocked(k), nil
}

func (s *fakeBudgetSrv) BudgetReturn(_ context.Context, m *wavespanv1.BudgetReturnRequest) (*wavespanv1.BudgetStatResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := fakeKey(m.GetNamespace(), m.GetBudget())
	p := s.pools[k]
	if p == nil {
		return nil, status.Error(codes.FailedPrecondition, "no budget")
	}
	lid := string(m.GetLeaseId())
	if p.tombs[lid] { // already settled: tombstone no-op (§3.6)
		return s.statLocked(k), nil
	}
	l := p.leases[lid]
	if l == nil { // unknown/already-returned: lenient no-op
		return s.statLocked(k), nil
	}
	if holderMismatch(l.holder, m.GetHolderId()) {
		return nil, status.Error(codes.PermissionDenied, "holder mismatch")
	}
	s.foldReportLocked(p, l, m.GetSpentCumulative())
	rem := l.amount - l.spent // CREDIT the attested remainder back to available
	p.available += rem
	p.leasedOut -= rem
	delete(p.leases, lid)
	p.tombs[lid] = true
	return s.statLocked(k), nil
}

func (s *fakeBudgetSrv) BudgetStat(_ context.Context, m *wavespanv1.BudgetStatRequest) (*wavespanv1.BudgetStatResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := fakeKey(m.GetNamespace(), m.GetBudget())
	if _, ok := s.pools[k]; !ok {
		return &wavespanv1.BudgetStatResult{Exists: false}, nil
	}
	return s.statLocked(k), nil
}

// startFakeBudget serves a fresh fakeBudgetSrv over an in-process bufconn and returns a Client dialed to
// it through the SDK's real transport stack.
func startFakeBudget(t *testing.T) *Client {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	wavespanv1.RegisterBudgetServiceServer(gs, &fakeBudgetSrv{pools: map[string]*fakePool{}})
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	c, err := Dial(Options{
		Endpoint: "bufnet",
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
		},
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestReturnReleasesUnspent drives the full node-cache lifecycle over real gRPC: Define a timed budget,
// Acquire a chunk, Spend locally (zero round-trips), then Return — and assert the server credited the
// unspent remainder back to available and conservation holds, and that the Budget is terminal afterward.
func TestReturnReleasesUnspent(t *testing.T) {
	c := startFakeBudget(t)
	ctx := context.Background()
	ns, budget := "ad", []byte("li/1/total")

	if err := c.Budget().Define(ctx, ns, budget, 1000, BudgetModeStrict, &BudgetDefineOptions{
		SelfGuardMs: 700, MaxPauseMs: 2000, DefaultTTLMs: 60_000, DedupRetryWindowMs: 30_000,
	}); err != nil {
		t.Fatalf("Define: %v", err)
	}

	b, err := c.LeasedBudget().Acquire(ctx, BudgetKey{Namespace: ns, Budget: budget}, WithChunk(200))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	for i := 0; i < 50; i++ {
		if err := b.Spend(1); err != nil {
			t.Fatalf("Spend %d: %v", i, err)
		}
	}
	if rem := b.Remaining(); rem != 150 {
		t.Fatalf("Remaining = %d, want 150", rem)
	}

	if err := b.Return(ctx); err != nil {
		t.Fatalf("Return: %v", err)
	}

	st, err := c.Budget().Stat(ctx, ns, budget, true)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.CapUnits != st.AvailableUnits+st.LeasedOutUnits+st.SpentUnits {
		t.Fatalf("conservation broken: cap=%d != avail=%d + leased=%d + spent=%d",
			st.CapUnits, st.AvailableUnits, st.LeasedOutUnits, st.SpentUnits)
	}
	if st.SpentUnits != 50 {
		t.Fatalf("spent = %d, want 50", st.SpentUnits)
	}
	if st.LeasedOutUnits != 0 {
		t.Fatalf("leasedOut = %d, want 0 after Return", st.LeasedOutUnits)
	}
	if st.AvailableUnits != 950 {
		t.Fatalf("available = %d, want 950 (unspent credited back)", st.AvailableUnits)
	}

	if err := b.Spend(1); err != ErrBudgetUnavailable {
		t.Fatalf("post-Return Spend = %v, want ErrBudgetUnavailable", err)
	}
}

// TestReportReturnHolderMatch exercises the Stage-2.x holder binding through the real SDK transport: a
// Report/Return whose holder contradicts the lease's grantee fails with PermissionDenied; the matching
// holder (and an omitted holder) succeed.
func TestReportReturnHolderMatch(t *testing.T) {
	c := startFakeBudget(t)
	ctx := context.Background()
	ns, budget := "ad", []byte("li/2/total")
	holderA, holderB := []byte("node-A"), []byte("node-B")

	if err := c.Budget().Define(ctx, ns, budget, 1000, BudgetModeStrict, nil); err != nil {
		t.Fatalf("Define: %v", err)
	}
	if _, err := c.Budget().Grant(ctx, ns, budget, holderA, 600, []byte("lease-1")); err != nil {
		t.Fatalf("Grant: %v", err)
	}

	// wrong holder on Report -> PermissionDenied.
	if err := c.Budget().Report(ctx, ns, budget, []byte("lease-1"), holderB, 100); CodeOf(err) != codes.PermissionDenied {
		t.Fatalf("Report(wrong holder) code = %v (err=%v), want PermissionDenied", CodeOf(err), err)
	}
	// matching holder on Report -> ok.
	if err := c.Budget().Report(ctx, ns, budget, []byte("lease-1"), holderA, 100); err != nil {
		t.Fatalf("Report(matching holder): %v", err)
	}
	// omitted holder on Report -> lenient, ok.
	if err := c.Budget().Report(ctx, ns, budget, []byte("lease-1"), nil, 150); err != nil {
		t.Fatalf("Report(omitted holder): %v", err)
	}
	// wrong holder on Return -> PermissionDenied (lease not settled).
	if err := c.Budget().Return(ctx, ns, budget, []byte("lease-1"), holderB, 150); CodeOf(err) != codes.PermissionDenied {
		t.Fatalf("Return(wrong holder) code = %v (err=%v), want PermissionDenied", CodeOf(err), err)
	}
	// matching holder on Return -> settles, credits the remainder.
	if err := c.Budget().Return(ctx, ns, budget, []byte("lease-1"), holderA, 150); err != nil {
		t.Fatalf("Return(matching holder): %v", err)
	}
	st, err := c.Budget().Stat(ctx, ns, budget, true)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.SpentUnits != 150 || st.LeasedOutUnits != 0 || st.AvailableUnits != 850 {
		t.Fatalf("post-settle stat = spent%d leased%d avail%d, want 150/0/850", st.SpentUnits, st.LeasedOutUnits, st.AvailableUnits)
	}
}
