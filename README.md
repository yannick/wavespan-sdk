# WaveSpan Go SDK

The official Go client for [WaveSpan](../../README.md) — a Kubernetes-native, eventually-consistent
distributed KV / graph / vector store. It wraps the generated grpc-go stubs with typed, ergonomic
methods so you never touch gRPC request/stream plumbing directly.

```go
import wavespan "github.com/yannick/wavespan-sdk"
```

## Why a separate module

The WaveSpan server module (`github.com/yannick/wavespan`) is private and pins its storage engine via a
local `replace` directive, so it is not `go get`-able. This SDK is its **own module** that vendors a
private copy of the protobuf/grpc stubs (under `internal/gen`) and depends on **only**
`google.golang.org/grpc` + `google.golang.org/protobuf`. Importing it never drags in the server or
storage engine.

## Install

```sh
go get github.com/yannick/wavespan-sdk
```

## Quick start

```go
ctx := context.Background()

c, err := wavespan.Dial(wavespan.Options{Endpoint: "localhost:7800"})
if err != nil {
	log.Fatal(err)
}
defer c.Close()

// Key/Value
if _, err := c.Put(ctx, "users", []byte("u1"), []byte("alice")); err != nil {
	log.Fatal(err)
}
rec, err := c.Get(ctx, "users", []byte("u1"))
if err == nil && rec.Found {
	fmt.Printf("%s (served by %s, %s)\n", rec.Value, rec.Meta.GetServedByMemberId(), rec.Meta.GetSource())
}
```

A runnable version is in [`examples/quickstart`](./examples/quickstart):

```sh
go run ./examples/quickstart --addr localhost:7800
```

## Command-line example (`wsctl`)

[`examples/cli`](./examples/cli) is a complete, dependency-free CLI that exercises **every** SDK
operation — it doubles as a reference for how to call each method. Run it against a node's data port:

```sh
go run ./examples/cli [global-flags] <group> <op> [args...]

# examples
go run ./examples/cli -addr=localhost:7800 kv put greeting "hello, wavespan"
go run ./examples/cli -addr=localhost:7800 kv get greeting
go run ./examples/cli -addr=localhost:7800 set add myset alice bob
go run ./examples/cli -addr=localhost:7800 -lin coll ls
go run ./examples/cli -addr=localhost:7800 graph query social "MATCH (n) RETURN n LIMIT 5"
go run ./examples/cli -addr=localhost:7800 -json budget stat quota
```

Groups: `kv`, `set`, `hash`, `zset`, `coll` (ls/tier/bulkrm), `graph`, `vec`, `budget`, `lease`,
`backup` (begin/status/list/delete/destinations). Global flags cover the endpoint/namespace/auth plus
per-op modifiers (`-lin`, `-limit`, `-ttl`, `-json`, vector/lease tuning). Human-readable output by
default; `-json` emits machine-parseable JSON.

## API surface

The SDK covers the **data-plane** services exposed on a node's data port (default `:7800`).

### Key/Value

```go
c.Put(ctx, ns, key, value, wavespan.WithTTL(60_000))      // origin+1 by default
c.Get(ctx, ns, key)                                        // Record{Found, Value, ExpiresAt, Meta}
c.MultiGet(ctx, ns, [][]byte{k1, k2})                      // one round-trip
c.Delete(ctx, ns, key, wavespan.WithoutOriginPlusOne())

scan, _ := c.Scan(ctx, ns, wavespan.WithScanMode(wavespan.ScanRoutedEventual))
fmt.Println(scan.Completeness())                           // honest header completeness
for row, err := range scan.Rows() {                        // streaming → range-over-func
	if err != nil { /* … */ }
	fmt.Printf("%s = %s\n", row.Key, row.Value)
}
fmt.Println(scan.FinalCompleteness(), scan.Warnings())     // trailer, after draining
```

Write options: `WithoutOriginPlusOne`, `WithTTL`, `WithIdempotencyKey`.
Read options: `WithoutDynamicCache`, `WithHideExpired`.
Scan options: `WithRange`, `WithLimit`, `WithScanMode`.

### Vectors (vector-as-key)

```go
v := c.Vector()
v.Put(ctx, "embeddings", queryVec, payload)
res, _ := v.Search(ctx, "embeddings", queryVec, 10,
	wavespan.WithNProbe(8), wavespan.WithRerank(), wavespan.WithPayload())
for _, n := range res.Neighbors {
	fmt.Println(n.VectorID, n.Score)
}
```

### Cypher (graph)

Parameters and row values convert to/from idiomatic Go values (`nil`, `bool`, `int64`, `float64`,
`string`, `[]byte`, `[]any`, `map[string]any`):

```go
q, _ := c.Query(ctx, "social", "MATCH (u:User {id:$id}) RETURN u.name AS name",
	map[string]any{"id": "u1"})
for row, err := range q.Rows() {
	if err != nil { /* … */ }
	fmt.Println(row["name"]) // row is map[string]any
}
fmt.Println(q.Meta().GetCompleteness())
```

### Collections (sets / hashes / sorted sets)

The strongly-consistent collections tier (sets, hash tables, sorted sets) is reached via
`c.Collections()`. Writes are linearizable; reads take a `linearizable bool` (pass `false` for the
fast bounded-stale path). The tier is enabled by default (set `WAVESPAN_COLLECTIONS_ENABLED=0` to disable).

```go
col := c.Collections()

col.SAdd(ctx, "flags", []byte("enabled"), []byte("feature-x"))
ok, _ := col.SIsMember(ctx, "flags", []byte("enabled"), []byte("feature-x"), false)

col.SAddTTL(ctx, "sessions", []byte("active"), 30*time.Minute, []byte("user-42")) // per-member TTL

col.HSet(ctx, "profile", []byte("u1"), wavespan.FieldValue{Field: []byte("name"), Value: []byte("Ada")})

col.ZAdd(ctx, "scores", []byte("game-7"), wavespan.ScoredMember{Member: []byte("ada"), Score: 99})
top, _ := col.ZRange(ctx, "scores", []byte("game-7"), 10, false) // ascending score order

// Atomic counters (exact under concurrency — no lost updates):
n, _ := col.HIncrBy(ctx, "metrics", []byte("page:home"), []byte("views"), 1)        // new int64
r, _ := col.HIncrByFloat(ctx, "metrics", []byte("page:home"), []byte("rate"), 0.5)  // new float64

// List the collections in a namespace with their element type ("set"|"hash"|"zset"):
cols, _ := col.ListCollections(ctx, "app", false) // []CollectionInfo{Name, Type}

// Bulk member removal across many collections (named list, or all when nil):
col.BulkRemove(ctx, "app", nil, [][]byte{[]byte("user-42")}) // remove user-42 from every collection

// Exactly-once write (idempotency key — important for non-idempotent ops like counters):
col.WithIdempotencyKey("req-7f3a").HIncrBy(ctx, "metrics", []byte("page:home"), []byte("views"), 1)

// Operator view: the serving node's placement, tunables, and per-shard leader status.
status, _ := col.TierInfo(ctx)
```

A mutation against a collection of the wrong datatype returns a `FailedPrecondition` error
(`WRONGTYPE`); incrementing a non-numeric field returns `InvalidArgument`. You can point the SDK at
**any** node — a non-leader transparently forwards writes to the owning shard's leader.

### Leased budgets (distributed escrow)

A **leased budget** is a pool of `int64` micro-units that clients lease out, spend against, and return —
a distributed rate/quota primitive for things like API-token or spend caps shared across many workers.
Every mutation is linearizable through the owning shard's leader and preserves the conservation
invariant:

```
cap == available + leasedOut + spent
```

In `BudgetModeStrict` (the Stage-1 mode) that invariant is enforced on every operation, so a budget can
never be over-spent even under concurrency, node failure, or retries. There are two surfaces:

**Controller** (`c.Budget()`) — define pools and manage leases (typically from a coordinator/service):

```go
b := c.Budget()

// Define a pool (nil opts = non-paced, non-expiring). Defining an existing pool is FailedPrecondition.
b.Define(ctx, "ai", []byte("gpt-tokens"), 1_000_000, wavespan.BudgetModeStrict, nil)

// Lease units to a holder under a caller-chosen lease id. Grant saturates: it returns the units
// actually granted (0 with nil error when the pool is empty), never more than available.
granted, _ := b.Grant(ctx, "ai", []byte("gpt-tokens"), []byte("worker-1"), 5_000, []byte("lease-1"))

b.Report(ctx, "ai", []byte("gpt-tokens"), []byte("lease-1"), []byte("worker-1"), 4_200) // cumulative spend
b.Return(ctx, "ai", []byte("gpt-tokens"), []byte("lease-1"), []byte("worker-1"), 4_200) // settle; credit unused back

st, _ := b.Stat(ctx, "ai", []byte("gpt-tokens"), false) // BudgetStat{Cap, Available, LeasedOut, Spent, SpentReported, Epoch}

// Reconcile against an external source of truth (e.g. a provider's Σ-acked usage), recovering units
// stranded by forced lease expiries; returns the units recovered.
recovered, _ := b.Reconcile(ctx, "ai", []byte("gpt-tokens"), 4_200)
```

`Report`/`Return` take **cumulative** spend for the lease (not deltas), so retries are idempotent.
`Stat.SpentReported` is the spend actually attested by holders (≤ `Spent`); the gap is the maximum
recoverable stranding. Use `WithIdempotencyKey` on the controller for exactly-once `Define`.

**Node-side holder** (`c.LeasedBudget()`) — a cached lease with a **zero-RPC `Spend` fast path**, for
hot request paths that must not block on the controller per call. It acquires a chunk of units up front
(and refills in the background), optionally paced by a local token bucket:

```go
lb := c.LeasedBudget()
bud, _ := lb.Acquire(ctx, wavespan.BudgetKey{Namespace: "ai", Budget: []byte("gpt-tokens")},
	wavespan.WithChunk(500),   // draw 500 units per refill
	wavespan.WithRate(1000),   // optional: local pacing at 1000 units/sec
	wavespan.WithBurst(2000))  // optional: token-bucket ceiling

switch err := bud.Spend(42); {                       // synchronous, no RPC on the hot path
case err == nil:                                     // charged
case errors.Is(err, wavespan.ErrBudgetUnavailable):  // cached lease exhausted; back off / re-check
case errors.Is(err, wavespan.ErrPacingThrottled):    // local rate limit hit; retry after tokens accrue
}
fmt.Println(bud.Remaining())  // hint: cached units still spendable without an RPC
_ = bud.Return(ctx)           // graceful settle (credits unspent units back); Budget unusable afterwards
```

Use the **controller** when you hand leases to remote workers or need exact accounting/reporting; use
the **holder** when one process makes many small charges and per-call controller round-trips would
dominate latency.

### Shard-aware write routing (advanced)

By default a collection/budget write may take one server-side forward hop to the owning shard's
leader. Opt into direct-to-leader routing by giving the SDK the cluster's core addresses:

```go
opts := wavespan.Options{Endpoint: "node-1:7800"}.
	WithShardAwareRouting([]string{"node-1:7800", "node-2:7800", "node-3:7800"}, 4) // cores, data-shard count
c, _ := wavespan.Dial(opts)
```

Reads and non-collection APIs are unaffected; `wavespan.ShardForKey(ns, coll, dataShards)` exposes the
placement hash if you want to pre-compute ownership.

### Backups (cluster snapshots to object storage)

`c.Backup()` drives consistent, point-in-time cluster backups to an object store. The node serving the
call coordinates the backup at a cluster-wide HLC frontier and fans the export out to every node; the
call returns as soon as the backup is admitted, so you poll for completion.

```go
bk := c.Backup()

// Back up everything to the node's default destination (nil spec). Returns a backup id immediately;
// the backup continues server-side.
id, _ := bk.Begin(ctx, nil)

// Or scope the backup and pick planes / destination:
id, _ = bk.Begin(ctx, &wavespan.BackupSpec{
	Selection:   &wavespan.Selection{Namespaces: []string{"users", "billing"}},
	Planes:      []wavespan.BackupPlane{wavespan.BackupPlaneLogical},   // and/or BackupPlanePhysical
	Destination: &wavespan.Destination{Bucket: "backups", Prefix: "prod/", Region: "eu-west-1"},
})

// Poll status → per-node progress, percent, and coverage gaps.
st, _ := bk.Status(ctx, id)
if st.GetStatus() == wavespan.BackupComplete {
	fmt.Printf("done: %.0f%%, %d nodes\n", st.GetOverallPct(), len(st.GetPerNode()))
}

// Catalog + lifecycle.
list, _ := bk.List(ctx) // []*BackupSummary
for _, s := range list {
	fmt.Println(s.GetBackupId(), s.GetStatus(), s.GetSizeBytes())
}
bk.Delete(ctx, id, false) // force=true cascades to dependent incremental children
```

Status is a `BackupStatusCode`: compare against `wavespan.BackupRunning`, `BackupComplete`,
`BackupPartial` (some ranges had no live holder — see `st.GetGaps()`), or `BackupFailed`. Backups go to
the node's configured default destination unless a `Destination` is given; with no bucket the default is
a local filesystem store (dev). Credentials in an ad-hoc `Destination` should use
`CredentialRef{SecretName: …}` (a server-resolved reference) rather than inline keys.

> Backup requires the server's backup coordinator to be enabled (it registers `BackupService` once the
> collections tier is up). `ListDestinations` is reserved for a future server release and returns
> `Unimplemented` until then.

## Honest consistency metadata

WaveSpan is eventually consistent, and the SDK never hides it: every read carries a `ResponseMeta`
(serving node, read source, completeness, conflict state) and scans/searches report `Completeness`
(`COMPLETE` / `PARTIAL` / `BEST_EFFORT`).

## Errors

Transport failures are returned as `*wavespan.Error` (preserving the gRPC status code). Helpers:
`wavespan.IsNotFound(err)`, `wavespan.IsUnavailable(err)`, `wavespan.CodeOf(err)`. Note: a missing KV
key is reported via `Record.Found`, not as an error.

## TLS & auth

```go
c, _ := wavespan.Dial(wavespan.Options{
	Endpoint: "node.example.com:7800",
	TLS:      &tls.Config{ /* … */ },   // secures the gRPC connection (TLS credentials)
	Token:    os.Getenv("WAVESPAN_TOKEN"), // authorization: Bearer … metadata on every RPC
})
```

The transport is **grpc-go**: all RPCs multiplex over a single HTTP/2 `*grpc.ClientConn`. With `TLS`
set the connection uses TLS credentials; otherwise it uses insecure (plaintext) credentials. Use
`Options.DialOptions` to customize the connection (keepalive, balancer, …) and `Options.Interceptors`
(`grpc.UnaryClientInterceptor`) to add unary client interceptors.

## Regenerating the stubs

The `.proto` contract under [`proto/`](./proto) is vendored from the WaveSpan server (the single
source of truth) and drives `internal/gen`. After updating the vendored `.proto` files, regenerate
from the repo root:

```sh
buf generate     # writes internal/gen (managed mode rewrites go_package to this module)
```

Requires `buf` plus `protoc-gen-go` and `protoc-gen-go-grpc` on `PATH`.

## Other languages

The same `.proto` contract drives the in-browser TypeScript client today; a Python SDK can follow the
same shape (generate stubs with a gRPC/protobuf Python plugin, wrap them ergonomically). See the
repo-level SDK notes for the cross-language plan.
