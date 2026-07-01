// Command wsctl is a complete, dependency-free example CLI over the WaveSpan Go SDK. It exercises every
// operation the SDK exposes — KV, Collections (set/hash/zset + list/tier/bulk-remove), Cypher graph
// queries, Vector search, the LeasedBudget controller, and the node-side budget holder — so it doubles
// as a reference for how to call each method.
//
// Build/run against a running node's data port (default localhost:7800):
//
//	go run ./sdk/go/examples/cli [global-flags] <group> <op> [args...]
//
// Global flags come BEFORE the group/op (the stdlib flag parser stops at the first positional):
//
//	-addr host:port   node data-port address           (default localhost:7800)
//	-ns name          namespace for KV/collections/budget (default "default")
//	-token tok        bearer token for authenticated clusters
//	-tls              dial with TLS (default plaintext, for local/dev)
//	-json             emit JSON instead of human-readable lines
//	-timeout d        per-invocation deadline           (default 15s)
//	-lin              linearizable (quorum) reads        (default bounded-stale)
//	-limit n          max rows for members/getall/range/scan (default 1000)
//	-ttl ms           TTL milliseconds for `kv put` / `set add`
//	-mode m           scan mode: cachefast|cachecomplete|routed|local (default cachefast)
//	-k n              neighbours for `vec search`        (default 5)
//	-nprobe n -ef n -rerank -payload   vector search tuning
//	-rate n -chunk n -burst n          `lease spend` node-side pacing
//
// Examples:
//
//	wsctl kv put greeting "hello, wavespan"
//	wsctl kv get greeting
//	wsctl -ns demo kv scan
//	wsctl set add myset alice bob
//	wsctl -lin set members myset
//	wsctl hash set user:100 name Carol age 30
//	wsctl zset add leaderboard 100 alice 85 bob
//	wsctl coll ls
//	wsctl graph query social "MATCH (n) RETURN n LIMIT 5"
//	wsctl vec put docs 0.1,0.2,0.3 "doc-1"
//	wsctl -k 3 vec search docs 0.1,0.2,0.3
//	wsctl budget define quota 1000 strict
//	wsctl lease spend quota 50
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	wavespan "github.com/yannick/wavespan-sdk"
)

// Global flags (parsed before the group/op positional args).
var (
	addr    = flag.String("addr", "localhost:7800", "node data-port address host:port")
	ns      = flag.String("ns", "default", "namespace for KV / collections / budget")
	token   = flag.String("token", "", "bearer token for authenticated clusters")
	useTLS  = flag.Bool("tls", false, "dial with TLS (default plaintext for local/dev)")
	jsonOut = flag.Bool("json", false, "emit JSON instead of human-readable lines")
	timeout = flag.Duration("timeout", 15*time.Second, "per-invocation deadline")
	lin     = flag.Bool("lin", false, "linearizable (quorum) reads")
	limit   = flag.Int("limit", 1000, "max rows for members/getall/range/scan")
	ttlMs   = flag.Int64("ttl", 0, "TTL milliseconds for `kv put` / `set add` (0 = none)")
	noOP1   = flag.Bool("no-origin-plus-one", false, "kv put/del: ack on the origin alone (skip the nearby-replica ack)")
	scanArg = flag.String("mode", "cachefast", "scan mode: cachefast|cachecomplete|routed|local")
	vecK    = flag.Uint("k", 5, "neighbours to return for `vec search`")
	nprobe  = flag.Uint("nprobe", 0, "vector search: coarse buckets to probe (0 = all)")
	efS     = flag.Uint("ef", 0, "vector search: HNSW beam width")
	rerank  = flag.Bool("rerank", false, "vector search: exact-rescore candidates")
	payload = flag.Bool("payload", false, "vector search: include payloads inline")
	rate    = flag.Int64("rate", 0, "lease spend: node token-bucket rate units/sec (0 = unpaced)")
	chunk   = flag.Int64("chunk", 0, "lease spend: per-refill draw size (0 = default 1000)")
	burst   = flag.Int64("burst", 0, "lease spend: token bucket ceiling (0 = chunk)")
)

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		usage()
		os.Exit(2)
	}
	group, op, rest := args[0], args[1], args[2:]

	opts := wavespan.Options{Endpoint: *addr, Token: *token}
	if *useTLS {
		opts.TLS = &tls.Config{} // server-name/roots from the system; customize for private CAs
	}
	c, err := wavespan.Dial(opts)
	if err != nil {
		die("dial: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch group {
	case "kv":
		kvCmd(ctx, c, op, rest)
	case "set":
		setCmd(ctx, c.Collections(), op, rest)
	case "hash":
		hashCmd(ctx, c.Collections(), op, rest)
	case "zset":
		zsetCmd(ctx, c.Collections(), op, rest)
	case "coll":
		collCmd(ctx, c.Collections(), op, rest)
	case "graph":
		graphCmd(ctx, c, op, rest)
	case "vec":
		vecCmd(ctx, c.Vector(), op, rest)
	case "budget":
		budgetCmd(ctx, c.Budget(), op, rest)
	case "lease":
		leaseCmd(ctx, c.LeasedBudget(), op, rest)
	case "backup":
		backupCmd(ctx, c.Backup(), op, rest)
	default:
		die("unknown group %q (try: kv set hash zset coll graph vec budget lease backup)", group)
	}
}

// ---- KV ----

func kvCmd(ctx context.Context, c *wavespan.Client, op string, a []string) {
	switch op {
	case "put":
		need(a, 2, "kv put <key> <value>")
		wo := writeOpts()
		if *ttlMs > 0 {
			wo = append(wo, wavespan.WithTTL(*ttlMs))
		}
		r, err := c.Put(ctx, *ns, []byte(a[0]), []byte(a[1]), wo...)
		check("put", err)
		emit(map[string]any{"version": fmtVersion(r.Version), "ackedNearbyReplicas": r.AckedNearbyReplicas, "geoSpillover": r.GeoSpillover},
			"PUT %s = %q  (version %s, acked %d nearby)", a[0], a[1], fmtVersion(r.Version), r.AckedNearbyReplicas)
	case "get":
		need(a, 1, "kv get <key>")
		r, err := c.Get(ctx, *ns, []byte(a[0]), readOpts()...)
		check("get", err)
		if !r.Found {
			emit(map[string]any{"found": false}, "GET %s: (not found)", a[0])
			return
		}
		emit(map[string]any{"found": true, "value": string(r.Value), "expiresAt": fmtTime(r.ExpiresAt), "servedBy": r.Meta.GetServedByMemberId()},
			"GET %s = %q  (served by %s, source %s)", a[0], r.Value, r.Meta.GetServedByMemberId(), r.Meta.GetSource())
	case "del":
		need(a, 1, "kv del <key>")
		r, err := c.Delete(ctx, *ns, []byte(a[0]), writeOpts()...)
		check("delete", err)
		emit(map[string]any{"version": fmtVersion(r.Version)}, "DEL %s  (tombstone version %s)", a[0], fmtVersion(r.Version))
	case "mget":
		need(a, 1, "kv mget <key>...")
		keys := make([][]byte, len(a))
		for i, k := range a {
			keys[i] = []byte(k)
		}
		recs, err := c.MultiGet(ctx, *ns, keys, readOpts()...)
		check("mget", err)
		type row struct {
			Key, Value string
			Found      bool
		}
		out := make([]row, len(recs))
		for i, r := range recs {
			out[i] = row{Key: a[i], Value: string(r.Value), Found: r.Found}
		}
		emitMulti(out, func() {
			for _, r := range out {
				if r.Found {
					fmt.Printf("  %s = %q\n", r.Key, r.Value)
				} else {
					fmt.Printf("  %s = (not found)\n", r.Key)
				}
			}
		})
	case "scan":
		sc, err := c.Scan(ctx, *ns, wavespan.WithLimit(uint32(*limit)), wavespan.WithScanMode(scanMode()))
		check("scan", err)
		type row struct{ Key, Value string }
		var rows []row
		for r, err := range sc.Rows() {
			check("scan row", err)
			rows = append(rows, row{Key: string(r.Key), Value: string(r.Value)})
		}
		emitMulti(map[string]any{"mode": sc.Mode().String(), "completeness": sc.FinalCompleteness().String(), "rows": rows}, func() {
			fmt.Printf("SCAN %s (mode=%s):\n", *ns, sc.Mode())
			for _, r := range rows {
				fmt.Printf("  %s = %s\n", r.Key, r.Value)
			}
			fmt.Printf("  (final completeness=%s, rows=%d)\n", sc.FinalCompleteness(), sc.RowsReturned())
		})
	default:
		die("unknown kv op %q (put get del mget scan)", op)
	}
}

// ---- Set ----

func setCmd(ctx context.Context, cc *wavespan.CollectionsClient, op string, a []string) {
	switch op {
	case "add":
		need(a, 2, "set add <coll> <member>...")
		members := toBytes(a[1:])
		var n uint64
		var err error
		if *ttlMs > 0 {
			n, err = cc.SAddTTL(ctx, *ns, []byte(a[0]), time.Duration(*ttlMs)*time.Millisecond, members...)
		} else {
			n, err = cc.SAdd(ctx, *ns, []byte(a[0]), members...)
		}
		check("sadd", err)
		emit(map[string]any{"added": n}, "SADD %s: %d newly added", a[0], n)
	case "rem":
		need(a, 2, "set rem <coll> <member>...")
		n, err := cc.SRem(ctx, *ns, []byte(a[0]), toBytes(a[1:])...)
		check("srem", err)
		emit(map[string]any{"removed": n}, "SREM %s: %d removed", a[0], n)
	case "ismember":
		need(a, 2, "set ismember <coll> <member>")
		ok, err := cc.SIsMember(ctx, *ns, []byte(a[0]), []byte(a[1]), *lin)
		check("sismember", err)
		emit(map[string]any{"member": ok}, "SISMEMBER %s %s = %t", a[0], a[1], ok)
	case "card":
		need(a, 1, "set card <coll>")
		n, err := cc.SCard(ctx, *ns, []byte(a[0]), *lin)
		check("scard", err)
		emit(map[string]any{"count": n}, "SCARD %s = %d", a[0], n)
	case "members":
		need(a, 1, "set members <coll>")
		ms, err := cc.SMembers(ctx, *ns, []byte(a[0]), *limit, *lin)
		check("smembers", err)
		out := bytesToStrs(ms)
		emitMulti(map[string]any{"members": out}, func() {
			fmt.Printf("SMEMBERS %s (%d):\n", a[0], len(out))
			for _, m := range out {
				fmt.Printf("  %s\n", m)
			}
		})
	default:
		die("unknown set op %q (add rem ismember card members)", op)
	}
}

// ---- Hash ----

func hashCmd(ctx context.Context, cc *wavespan.CollectionsClient, op string, a []string) {
	switch op {
	case "set":
		need(a, 3, "hash set <coll> <field> <value> [<field> <value>...]")
		pairs := a[1:]
		if len(pairs)%2 != 0 {
			die("hash set: field/value args must be in pairs")
		}
		fields := make([]wavespan.FieldValue, 0, len(pairs)/2)
		for i := 0; i < len(pairs); i += 2 {
			fields = append(fields, wavespan.FieldValue{Field: []byte(pairs[i]), Value: []byte(pairs[i+1])})
		}
		n, err := cc.HSet(ctx, *ns, []byte(a[0]), fields...)
		check("hset", err)
		emit(map[string]any{"newFields": n}, "HSET %s: %d new field(s)", a[0], n)
	case "del":
		need(a, 2, "hash del <coll> <field>...")
		n, err := cc.HDel(ctx, *ns, []byte(a[0]), toBytes(a[1:])...)
		check("hdel", err)
		emit(map[string]any{"removed": n}, "HDEL %s: %d removed", a[0], n)
	case "get":
		need(a, 2, "hash get <coll> <field>")
		v, ok, err := cc.HGet(ctx, *ns, []byte(a[0]), []byte(a[1]), *lin)
		check("hget", err)
		if !ok {
			emit(map[string]any{"found": false}, "HGET %s %s: (not found)", a[0], a[1])
			return
		}
		emit(map[string]any{"found": true, "value": string(v)}, "HGET %s %s = %q", a[0], a[1], v)
	case "len":
		need(a, 1, "hash len <coll>")
		n, err := cc.HLen(ctx, *ns, []byte(a[0]), *lin)
		check("hlen", err)
		emit(map[string]any{"count": n}, "HLEN %s = %d", a[0], n)
	case "getall":
		need(a, 1, "hash getall <coll>")
		fs, err := cc.HGetAll(ctx, *ns, []byte(a[0]), *limit, *lin)
		check("hgetall", err)
		type kv struct{ Field, Value string }
		out := make([]kv, len(fs))
		for i, f := range fs {
			out[i] = kv{Field: string(f.Field), Value: string(f.Value)}
		}
		emitMulti(map[string]any{"fields": out}, func() {
			fmt.Printf("HGETALL %s (%d):\n", a[0], len(out))
			for _, f := range out {
				fmt.Printf("  %s = %s\n", f.Field, f.Value)
			}
		})
	case "incrby":
		need(a, 3, "hash incrby <coll> <field> <delta>")
		n, err := cc.HIncrBy(ctx, *ns, []byte(a[0]), []byte(a[1]), mustInt(a[2]))
		check("hincrby", err)
		emit(map[string]any{"value": n}, "HINCRBY %s %s = %d", a[0], a[1], n)
	case "incrbyfloat":
		need(a, 3, "hash incrbyfloat <coll> <field> <delta>")
		f, err := cc.HIncrByFloat(ctx, *ns, []byte(a[0]), []byte(a[1]), mustFloat(a[2]))
		check("hincrbyfloat", err)
		emit(map[string]any{"value": f}, "HINCRBYFLOAT %s %s = %g", a[0], a[1], f)
	default:
		die("unknown hash op %q (set del get len getall incrby incrbyfloat)", op)
	}
}

// ---- Sorted set ----

func zsetCmd(ctx context.Context, cc *wavespan.CollectionsClient, op string, a []string) {
	switch op {
	case "add":
		need(a, 3, "zset add <coll> <score> <member> [<score> <member>...]")
		pairs := a[1:]
		if len(pairs)%2 != 0 {
			die("zset add: score/member args must be in pairs")
		}
		members := make([]wavespan.ScoredMember, 0, len(pairs)/2)
		for i := 0; i < len(pairs); i += 2 {
			members = append(members, wavespan.ScoredMember{Score: mustFloat(pairs[i]), Member: []byte(pairs[i+1])})
		}
		n, err := cc.ZAdd(ctx, *ns, []byte(a[0]), members...)
		check("zadd", err)
		emit(map[string]any{"added": n}, "ZADD %s: %d newly added", a[0], n)
	case "rem":
		need(a, 2, "zset rem <coll> <member>...")
		n, err := cc.ZRem(ctx, *ns, []byte(a[0]), toBytes(a[1:])...)
		check("zrem", err)
		emit(map[string]any{"removed": n}, "ZREM %s: %d removed", a[0], n)
	case "score":
		need(a, 2, "zset score <coll> <member>")
		sc, ok, err := cc.ZScore(ctx, *ns, []byte(a[0]), []byte(a[1]), *lin)
		check("zscore", err)
		if !ok {
			emit(map[string]any{"found": false}, "ZSCORE %s %s: (not found)", a[0], a[1])
			return
		}
		emit(map[string]any{"found": true, "score": sc}, "ZSCORE %s %s = %g", a[0], a[1], sc)
	case "card":
		need(a, 1, "zset card <coll>")
		n, err := cc.ZCard(ctx, *ns, []byte(a[0]), *lin)
		check("zcard", err)
		emit(map[string]any{"count": n}, "ZCARD %s = %d", a[0], n)
	case "range":
		need(a, 1, "zset range <coll>")
		ms, err := cc.ZRange(ctx, *ns, []byte(a[0]), *limit, *lin)
		check("zrange", err)
		type sm struct {
			Member string
			Score  float64
		}
		out := make([]sm, len(ms))
		for i, m := range ms {
			out[i] = sm{Member: string(m.Member), Score: m.Score}
		}
		emitMulti(map[string]any{"members": out}, func() {
			fmt.Printf("ZRANGE %s (%d):\n", a[0], len(out))
			for _, m := range out {
				fmt.Printf("  %-20s %g\n", m.Member, m.Score)
			}
		})
	default:
		die("unknown zset op %q (add rem score card range)", op)
	}
}

// ---- Collections (cross-type) ----

func collCmd(ctx context.Context, cc *wavespan.CollectionsClient, op string, a []string) {
	switch op {
	case "ls":
		cis, err := cc.ListCollections(ctx, *ns, *lin)
		check("listcollections", err)
		type ci struct{ Name, Type string }
		out := make([]ci, len(cis))
		for i, x := range cis {
			out[i] = ci{Name: string(x.Name), Type: x.Type}
		}
		emitMulti(map[string]any{"collections": out}, func() {
			fmt.Printf("COLLECTIONS in %s (%d):\n", *ns, len(out))
			for _, x := range out {
				fmt.Printf("  %-24s %s\n", x.Name, x.Type)
			}
		})
	case "tier":
		t, err := cc.TierInfo(ctx)
		check("tierinfo", err)
		emitMulti(t, func() {
			fmt.Printf("TIER enabled=%t node=%s raft=%s replicaID=%d voter=%t\n", t.Enabled, t.NodeHostID, t.RaftAddress, t.SelfReplicaID, t.Voter)
			fmt.Printf("  tunables: rtt=%dms electionRTT=%d heartbeatRTT=%d snapEntries=%d sweep=%dms\n", t.RTTMs, t.ElectionRTT, t.HeartbeatRTT, t.SnapshotEntries, t.SweepMs)
			for _, s := range t.Shards {
				kind := "data"
				if !s.IsData {
					kind = "meta"
				}
				fmt.Printf("  shard %d (%s): replica=%d leader=%d hasLeader=%t isLeader=%t\n", s.ShardID, kind, s.ReplicaID, s.LeaderReplicaID, s.HasLeader, s.IsLeader)
			}
		})
	case "bulkrm":
		need(a, 2, "coll bulkrm <colls-csv|''=all> <members-csv>")
		colls := toBytes(splitCSV(a[0]))
		members := toBytes(splitCSV(a[1]))
		res, err := cc.BulkRemove(ctx, *ns, colls, members)
		check("bulkremove", err)
		type be struct {
			Collection string
			Removed    uint64
			Error      string
		}
		out := make([]be, len(res))
		for i, e := range res {
			out[i] = be{Collection: string(e.Collection), Removed: e.Removed, Error: e.Error}
		}
		emitMulti(map[string]any{"results": out}, func() {
			for _, e := range out {
				if e.Error != "" {
					fmt.Printf("  %s: ERROR %s\n", e.Collection, e.Error)
				} else {
					fmt.Printf("  %s: %d removed\n", e.Collection, e.Removed)
				}
			}
		})
	default:
		die("unknown coll op %q (ls tier bulkrm)", op)
	}
}

// ---- Graph (Cypher) ----

func graphCmd(ctx context.Context, c *wavespan.Client, op string, a []string) {
	switch op {
	case "query":
		need(a, 2, "graph query <graphID> <cypher> [name=value ...]")
		params := parseParams(a[2:])
		q, err := c.Query(ctx, a[0], a[1], params)
		check("query", err)
		var rows []map[string]any
		for row, err := range q.Rows() {
			check("query row", err)
			rows = append(rows, map[string]any(row))
		}
		emitMulti(map[string]any{"rows": rows}, func() {
			fmt.Printf("CYPHER %s (%d rows):\n", a[0], len(rows))
			for _, r := range rows {
				fmt.Printf("  %v\n", r)
			}
		})
	default:
		die("unknown graph op %q (query)", op)
	}
}

// ---- Vector ----

func vecCmd(ctx context.Context, v *wavespan.VectorClient, op string, a []string) {
	switch op {
	case "put":
		need(a, 3, "vec put <coll> <csv-floats> <payload>")
		ver, err := v.Put(ctx, a[0], parseFloats(a[1]), []byte(a[2]))
		check("vec put", err)
		emit(map[string]any{"version": fmtVersion(ver)}, "VEC PUT %s  (version %s)", a[0], fmtVersion(ver))
	case "get":
		need(a, 2, "vec get <coll> <csv-floats>")
		pl, ok, err := v.Get(ctx, a[0], parseFloats(a[1]))
		check("vec get", err)
		if !ok {
			emit(map[string]any{"found": false}, "VEC GET %s: (not found)", a[0])
			return
		}
		emit(map[string]any{"found": true, "payload": string(pl)}, "VEC GET %s = %q", a[0], pl)
	case "del":
		need(a, 2, "vec del <coll> <csv-floats>")
		check("vec del", v.Delete(ctx, a[0], parseFloats(a[1])))
		emit(map[string]any{"deleted": true}, "VEC DEL %s", a[0])
	case "search":
		need(a, 2, "vec search <coll> <csv-floats>")
		var so []wavespan.SearchOption
		if *nprobe > 0 {
			so = append(so, wavespan.WithNProbe(uint32(*nprobe)))
		}
		if *efS > 0 {
			so = append(so, wavespan.WithEfSearch(uint32(*efS)))
		}
		if *rerank {
			so = append(so, wavespan.WithRerank())
		}
		if *payload {
			so = append(so, wavespan.WithPayload())
		}
		res, err := v.Search(ctx, a[0], parseFloats(a[1]), uint32(*vecK), so...)
		check("vec search", err)
		type nb struct {
			VectorID string
			Distance float64
			Score    float64
			Payload  string
		}
		out := make([]nb, len(res.Neighbors))
		for i, n := range res.Neighbors {
			out[i] = nb{VectorID: n.VectorID, Distance: n.Distance, Score: n.Score, Payload: string(n.Payload)}
		}
		emitMulti(map[string]any{"completeness": res.Completeness.String(), "neighbors": out}, func() {
			fmt.Printf("VEC SEARCH %s (k=%d, completeness=%s):\n", a[0], *vecK, res.Completeness)
			for _, n := range out {
				fmt.Printf("  %-24s dist=%.4f score=%.4f %s\n", n.VectorID, n.Distance, n.Score, n.Payload)
			}
		})
	default:
		die("unknown vec op %q (put get del search)", op)
	}
}

// ---- Budget controller ----

func budgetCmd(ctx context.Context, bc *wavespan.BudgetClient, op string, a []string) {
	switch op {
	case "define":
		need(a, 2, "budget define <budget> <cap> [strict|relaxed]")
		mode := wavespan.BudgetModeStrict
		if len(a) >= 3 && strings.EqualFold(a[2], "relaxed") {
			mode = wavespan.BudgetModeRelaxed
		}
		check("define", bc.Define(ctx, *ns, []byte(a[0]), mustInt(a[1]), mode, nil))
		emit(map[string]any{"defined": a[0], "cap": mustInt(a[1])}, "BUDGET DEFINE %s cap=%d", a[0], mustInt(a[1]))
	case "grant":
		need(a, 3, "budget grant <budget> <holder> <amount> [leaseID]")
		var leaseID []byte
		if len(a) >= 4 {
			leaseID = []byte(a[3])
		}
		granted, err := bc.Grant(ctx, *ns, []byte(a[0]), []byte(a[1]), mustInt(a[2]), leaseID)
		check("grant", err)
		emit(map[string]any{"granted": granted}, "BUDGET GRANT %s -> %s: %d units", a[0], a[1], granted)
	case "report":
		need(a, 4, "budget report <budget> <leaseID> <holder> <spentCumulative>")
		check("report", bc.Report(ctx, *ns, []byte(a[0]), []byte(a[1]), []byte(a[2]), mustInt(a[3])))
		emit(map[string]any{"reported": mustInt(a[3])}, "BUDGET REPORT %s lease=%s spent=%d", a[0], a[1], mustInt(a[3]))
	case "return":
		need(a, 4, "budget return <budget> <leaseID> <holder> <spentCumulative>")
		check("return", bc.Return(ctx, *ns, []byte(a[0]), []byte(a[1]), []byte(a[2]), mustInt(a[3])))
		emit(map[string]any{"returned": true}, "BUDGET RETURN %s lease=%s spent=%d", a[0], a[1], mustInt(a[3]))
	case "reconcile":
		need(a, 2, "budget reconcile <budget> <trueAckedUnits>")
		recovered, err := bc.Reconcile(ctx, *ns, []byte(a[0]), mustInt(a[1]))
		check("reconcile", err)
		emit(map[string]any{"recovered": recovered}, "BUDGET RECONCILE %s: %d recovered", a[0], recovered)
	case "stat":
		need(a, 1, "budget stat <budget>")
		st, err := bc.Stat(ctx, *ns, []byte(a[0]), *lin)
		check("stat", err)
		emit(st, "BUDGET STAT %s exists=%t cap=%d available=%d leasedOut=%d spent=%d (reported=%d) epoch=%d",
			a[0], st.Exists, st.CapUnits, st.AvailableUnits, st.LeasedOutUnits, st.SpentUnits, st.SpentReportedUnits, st.Epoch)
	default:
		die("unknown budget op %q (define grant report return reconcile stat)", op)
	}
}

// ---- LeasedBudget holder (node-side, zero-RPC Spend) ----

func leaseCmd(ctx context.Context, lb *wavespan.LeasedBudgetClient, op string, a []string) {
	switch op {
	case "spend":
		need(a, 2, "lease spend <budget> <n>")
		var ao []wavespan.AcquireOption
		if *rate > 0 {
			ao = append(ao, wavespan.WithRate(*rate))
		}
		if *chunk > 0 {
			ao = append(ao, wavespan.WithChunk(*chunk))
		}
		if *burst > 0 {
			ao = append(ao, wavespan.WithBurst(*burst))
		}
		b, err := lb.Acquire(ctx, wavespan.BudgetKey{Namespace: *ns, Budget: []byte(a[0])}, ao...)
		check("acquire", err)
		spendErr := b.Spend(mustInt(a[1]))
		remaining := b.Remaining()
		// Settle the lease back to the controller regardless of the spend outcome.
		retErr := b.Return(ctx)
		status := "ok"
		switch {
		case errors.Is(spendErr, wavespan.ErrBudgetUnavailable):
			status = "unavailable"
		case errors.Is(spendErr, wavespan.ErrPacingThrottled):
			status = "throttled"
		case spendErr != nil:
			check("spend", spendErr)
		}
		check("return", retErr)
		emit(map[string]any{"status": status, "spent": mustInt(a[1]), "remainingHint": remaining},
			"LEASE SPEND %s: %s (spent %d, remaining hint %d)", a[0], status, mustInt(a[1]), remaining)
	default:
		die("unknown lease op %q (spend)", op)
	}
}

// ---- Backup (cluster backups to an object store) ----

func backupCmd(ctx context.Context, b *wavespan.BackupClient, op string, a []string) {
	switch op {
	case "begin":
		// Optional first arg selects namespaces (CSV); default = everything. Logical plane by default.
		spec := &wavespan.BackupSpec{Planes: []wavespan.BackupPlane{wavespan.BackupPlaneLogical}}
		if len(a) >= 1 && a[0] != "" {
			spec.Selection = &wavespan.Selection{Namespaces: splitCSV(a[0])}
		}
		id, err := b.Begin(ctx, spec)
		check("backup begin", err)
		emit(map[string]any{"backupId": id}, "BACKUP BEGIN -> %s  (poll: backup status %s)", id, id)
	case "status":
		need(a, 1, "backup status <id>")
		st, err := b.Status(ctx, a[0])
		check("backup status", err)
		emitMulti(st, func() {
			fmt.Printf("BACKUP %s: status=%s phase=%s %.1f%% started=%s finished=%s\n",
				st.GetBackupId(), st.GetStatus(), st.GetPhase(), st.GetOverallPct(), fmtMs(st.GetStartedMs()), fmtMs(st.GetFinishedMs()))
			for _, n := range st.GetPerNode() {
				fmt.Printf("  node %-10s phase=%s objects=%d bytes=%d done=%t\n", n.GetMemberId(), n.GetPhase(), n.GetObjects(), n.GetBytes(), n.GetDone())
			}
			if g := st.GetGaps(); len(g) > 0 {
				fmt.Printf("  gaps (%d): %v\n", len(g), g)
			}
		})
	case "list":
		bs, err := b.List(ctx)
		check("backup list", err)
		type row struct {
			ID, Status, Parent string
			SizeBytes          int64
			Partial            bool
		}
		out := make([]row, len(bs))
		for i, s := range bs {
			out[i] = row{ID: s.GetBackupId(), Status: s.GetStatus().String(), Parent: s.GetParent(), SizeBytes: s.GetSizeBytes(), Partial: s.GetPartial()}
		}
		emitMulti(map[string]any{"backups": out}, func() {
			fmt.Printf("BACKUPS (%d):\n", len(out))
			for _, s := range out {
				kind := "full"
				if s.Parent != "" {
					kind = "incr<-" + s.Parent
				}
				fmt.Printf("  %-28s %-16s %-14s %d bytes\n", s.ID, s.Status, kind, s.SizeBytes)
			}
		})
	case "delete":
		need(a, 1, "backup delete <id> [force]")
		force := len(a) >= 2 && (a[1] == "force" || a[1] == "true")
		ok, err := b.Delete(ctx, a[0], force)
		check("backup delete", err)
		emit(map[string]any{"deleted": ok}, "BACKUP DELETE %s: deleted=%t", a[0], ok)
	case "destinations":
		d, err := b.ListDestinations(ctx)
		check("backup destinations", err)
		emitMulti(d, func() {
			def := d.GetDefaultDestination()
			fmt.Printf("DEFAULT destination: bucket=%q prefix=%q endpoint=%q ssl=%t (filesystem=%t)\n",
				def.GetBucket(), def.GetPrefix(), def.GetEndpoint(), def.GetUseSsl(), d.GetDefaultIsFs())
			for _, n := range d.GetNamed() {
				fmt.Printf("  named %-12s bucket=%q endpoint=%q\n", n.GetName(), n.GetBucket(), n.GetEndpoint())
			}
			fmt.Printf("  inline credentials allowed: %t\n", d.GetAllowInlineCreds())
		})
	default:
		die("unknown backup op %q (begin status list delete destinations)", op)
	}
}

// ---- helpers ----

func readOpts() []wavespan.ReadOption { return nil } // placeholder for future read tuning

func writeOpts() []wavespan.WriteOption {
	if *noOP1 {
		return []wavespan.WriteOption{wavespan.WithoutOriginPlusOne()}
	}
	return nil
}

func scanMode() wavespan.ScanMode {
	switch strings.ToLower(*scanArg) {
	case "cachecomplete":
		return wavespan.ScanCacheComplete
	case "routed":
		return wavespan.ScanRoutedEventual
	case "local":
		return wavespan.ScanLocalOnly
	default:
		return wavespan.ScanCacheFast
	}
}

// emit prints a single-result op: JSON when -json, else the formatted human line.
func emit(jsonObj any, humanFmt string, args ...any) {
	if *jsonOut {
		encodeJSON(jsonObj)
		return
	}
	fmt.Printf(humanFmt+"\n", args...)
}

// emitMulti prints a multi-line/multi-row op: JSON when -json, else runs the human closure.
func emitMulti(jsonObj any, human func()) {
	if *jsonOut {
		encodeJSON(jsonObj)
		return
	}
	human()
}

func encodeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		die("json: %v", err)
	}
}

func toBytes(ss []string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func bytesToStrs(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}

// splitCSV splits "a,b,c" into ["a","b","c"]; "" yields an empty slice (BulkRemove treats empty
// collections as "all collections in the namespace").
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func parseFloats(csv string) []float32 {
	parts := strings.Split(csv, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			die("invalid float %q in vector: %v", p, err)
		}
		out[i] = float32(f)
	}
	return out
}

// parseParams turns ["n=7", "s=hi", "ok=true"] into typed Cypher params (int, then float, then bool,
// else string).
func parseParams(kvs []string) map[string]any {
	if len(kvs) == 0 {
		return nil
	}
	out := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			die("invalid param %q (want name=value)", kv)
		}
		out[k] = inferType(v)
	}
	return out
}

func inferType(v string) any {
	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	return v
}

func mustInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		die("invalid integer %q: %v", s, err)
	}
	return i
}

func mustFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		die("invalid number %q: %v", s, err)
	}
	return f
}

func fmtVersion(v *wavespan.Version) string {
	if v == nil {
		return "—"
	}
	return fmt.Sprintf("%d.%d@%s", v.GetHlcPhysicalMs(), v.GetHlcLogical(), v.GetWriterMemberId())
}

func fmtTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}

func fmtMs(ms int64) string {
	if ms == 0 {
		return "—"
	}
	return time.UnixMilli(ms).Format(time.RFC3339)
}

func need(a []string, n int, usage string) {
	if len(a) < n {
		die("usage: %s", usage)
	}
}

func check(op string, err error) {
	if err != nil {
		die("%s: %v", op, err)
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "wsctl: "+format+"\n", args...)
	os.Exit(1)
}

func usage() {
	fmt.Fprint(os.Stderr, `wsctl — WaveSpan SDK example CLI (every SDK operation)

usage: wsctl [global-flags] <group> <op> [args...]

groups & ops:
  kv      put <k> <v> | get <k> | del <k> | mget <k>... | scan
  set     add <c> <m>... | rem <c> <m>... | ismember <c> <m> | card <c> | members <c>
  hash    set <c> <f> <v> [<f> <v>...] | del <c> <f>... | get <c> <f> | len <c> | getall <c>
          incrby <c> <f> <delta> | incrbyfloat <c> <f> <delta>
  zset    add <c> <score> <m> [<score> <m>...] | rem <c> <m>... | score <c> <m> | card <c> | range <c>
  coll    ls | tier | bulkrm <colls-csv|''> <members-csv>
  graph   query <graphID> <cypher> [name=value...]
  vec     put <c> <csv-floats> <payload> | get <c> <csv-floats> | del <c> <csv-floats> | search <c> <csv-floats>
  budget  define <b> <cap> [strict|relaxed] | grant <b> <holder> <amt> [leaseID]
          report/return <b> <leaseID> <holder> <spentCumulative> | reconcile <b> <trueAcked> | stat <b>
  lease   spend <b> <n>
  backup  begin [namespaces-csv] | status <id> | list | delete <id> [force] | destinations

global flags:
`)
	flag.PrintDefaults()
}
