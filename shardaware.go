package wavespan

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// firstDataShard is the lowest data-shard id in WaveSpan's static pre-split collection layout (data
// shard ids run [firstDataShard, firstDataShard+N)). It MUST match the server's
// internal/collections.FirstDataShard; an equivalence test in the main module pins the two together.
const firstDataShard uint64 = 2

// ShardForKey reports which data shard owns collection (ns,coll) under WaveSpan's static hash
// pre-split: firstDataShard + (fnv64a(routeKey) mod dataShards), where routeKey is len-prefixed ns
// then coll. This is the SDK's copy of the server's canonical routing function (replicated, not
// imported, because sdk/go is a standalone module that must not depend on internal/collections); a
// main-module equivalence test pins it to the server's internal/collections.ShardForKey so the two
// can never drift. Exported so applications can pre-compute placement. dataShards < 1 is clamped to 1.
func ShardForKey(ns, coll []byte, dataShards uint64) uint64 {
	if dataShards < 1 {
		dataShards = 1
	}
	h := fnv.New64a()
	_, _ = h.Write(routeKey(ns, coll))
	return firstDataShard + (h.Sum64() % dataShards)
}

// routeKey is the collection's position in the global ordered keyspace: uint32(len(ns))||ns then
// uint32(len(coll))||coll, big-endian — byte-for-byte identical to the server's routeKey.
func routeKey(ns, coll []byte) []byte {
	out := make([]byte, 0, 8+len(ns)+len(coll))
	out = appendChunk(out, ns)
	out = appendChunk(out, coll)
	return out
}

func appendChunk(dst, b []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(b)))
	dst = append(dst, l[:]...)
	return append(dst, b...)
}

// shardRouter is the opt-in shard-aware routing layer: it holds one CollectionService client per
// core (index i = replicaId i+1) and a routing table mapping each data shard to its current leader's
// core index, learned from TierInfo. Each write is sent straight to the owning shard's leader,
// eliminating the per-op forward hop a single-endpoint client incurs (client -> any node -> leader).
//
// The table is refreshed on a short interval and lazily after a write error (a leadership change
// surfaces as a not-leader/forward error); when a leader is unknown the router falls back to a
// deterministic core so progress is still made (the server forwards as usual). It is safe for
// concurrent use: the table is guarded by an RWMutex (read on the hot path, write only on refresh).
type shardRouter struct {
	conns      []*grpc.ClientConn
	clients    []wavespanv1.CollectionServiceClient
	dataShards uint64

	mu          sync.RWMutex
	shardLeader map[uint64]int
	lastRefresh time.Time

	refreshMin time.Duration
}

// shardRouterRefreshInterval is how often the routing table is proactively rebuilt and the floor on
// lazy (error-triggered) refreshes, so an error burst cannot hammer TierInfo.
const shardRouterRefreshInterval = time.Second

// newShardRouter dials one CollectionService client per core (using the same transport posture as the
// parent client's dial options) and primes the routing table once, best-effort. cores is the ordered
// list of core data-port addresses (index i = replicaId i+1); dataShards is the hash directory width
// N. dataShards < 1 is clamped to 1.
func newShardRouter(cores []string, dataShards int, dialOpts []grpc.DialOption) (*shardRouter, error) {
	if len(cores) == 0 {
		return nil, errNoCores
	}
	if dataShards < 1 {
		dataShards = 1
	}
	conns := make([]*grpc.ClientConn, len(cores))
	clients := make([]wavespanv1.CollectionServiceClient, len(cores))
	for i, addr := range cores {
		conn, err := grpc.NewClient(dialTargetFor(addr), dialOpts...)
		if err != nil {
			for j := 0; j < i; j++ { // close what we opened so far
				_ = conns[j].Close()
			}
			return nil, err
		}
		conns[i] = conn
		clients[i] = wavespanv1.NewCollectionServiceClient(conn)
	}
	r := &shardRouter{
		conns:       conns,
		clients:     clients,
		dataShards:  uint64(dataShards),
		shardLeader: make(map[uint64]int),
		refreshMin:  shardRouterRefreshInterval,
	}
	r.refresh(context.Background())
	return r, nil
}

// clientForKey returns the CollectionService client for the leader of (ns,coll)'s shard, refreshing a
// stale table first when the leader is unknown. Falls back to a deterministic core if the leader is
// still unknown so the op makes progress (the server then forwards).
func (r *shardRouter) clientForKey(ctx context.Context, ns string, coll []byte) wavespanv1.CollectionServiceClient {
	shard := ShardForKey([]byte(ns), coll, r.dataShards)

	r.mu.RLock()
	idx, known := r.shardLeader[shard]
	stale := time.Since(r.lastRefresh) >= r.refreshMin
	r.mu.RUnlock()

	if !known && stale {
		r.refresh(ctx)
		r.mu.RLock()
		idx, known = r.shardLeader[shard]
		r.mu.RUnlock()
	}
	if !known || idx < 0 || idx >= len(r.clients) {
		idx = int(shard % uint64(len(r.clients)))
	}
	return r.clients[idx]
}

// noteError triggers a rate-limited refresh after a failed write (a leadership change surfaces here),
// so the next op routes to the new leader. It returns err unchanged for call-site chaining.
func (r *shardRouter) noteError(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil {
		return err
	}
	r.mu.RLock()
	stale := time.Since(r.lastRefresh) >= r.refreshMin
	r.mu.RUnlock()
	if stale {
		r.refresh(ctx)
	}
	return err
}

// refresh rebuilds the routing table from TierInfo on the first reachable core. A voter hosts every
// data shard, so its TierInfo reports leaders for all of them; replicaId R maps to core index R-1. On
// total failure the existing table is kept (only lastRefresh advances, to rate-limit retries).
func (r *shardRouter) refresh(ctx context.Context) {
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var table map[uint64]int
	for _, cl := range r.clients {
		resp, err := cl.TierInfo(rctx, &wavespanv1.TierInfoRequest{})
		if err != nil || resp == nil {
			continue
		}
		t := make(map[uint64]int)
		for _, sh := range resp.GetShards() {
			if !sh.GetIsData() || !sh.GetHasLeader() {
				continue
			}
			leader := sh.GetLeaderReplicaId()
			if leader < 1 {
				continue
			}
			idx := int(leader - 1)
			if idx < 0 || idx >= len(r.clients) {
				continue // leader replicaId without a known core address
			}
			t[sh.GetShardId()] = idx
		}
		if len(t) > 0 {
			table = t
			break
		}
	}

	r.mu.Lock()
	r.lastRefresh = time.Now()
	if table != nil {
		r.shardLeader = table
	}
	r.mu.Unlock()
}

// close releases the per-core connections.
func (r *shardRouter) close() {
	for _, conn := range r.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

// dialOptionsFor reconstructs the grpc dial options the SDK uses for the router's per-core conns,
// mirroring Dial's transport posture (TLS or insecure + auth/user-agent interceptors).
func dialOptionsFor(opts Options) []grpc.DialOption {
	ua := opts.UserAgent
	if ua == "" {
		ua = "wavespan-go"
	}
	dialOpts := make([]grpc.DialOption, 0, 4+len(opts.DialOptions))
	if opts.TLS != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(opts.TLS)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	unary := append([]grpc.UnaryClientInterceptor{metadataInterceptor(opts.Token, ua)}, opts.Interceptors...)
	dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(unary...))
	dialOpts = append(dialOpts, grpc.WithChainStreamInterceptor(metadataStreamInterceptor(opts.Token, ua)))
	dialOpts = append(dialOpts, opts.DialOptions...)
	return dialOpts
}
