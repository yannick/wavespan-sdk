package wavespan

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
	"google.golang.org/grpc"
)

// refShardForKey is an INDEPENDENT re-implementation of WaveSpan's collection hash routing, written
// from the spec rather than calling ShardForKey: firstDataShard(2) + fnv64a(uint32(len(ns))||ns ||
// uint32(len(coll))||coll) mod dataShards. It is the contract the server's
// internal/collections.ShardForKey is itself pinned to (see that package's shardforkey_test.go).
// Asserting the SDK's ShardForKey == refShardForKey here, while the server test asserts the server ==
// the same formula, transitively guarantees SDK routing == server routing without linking both proto
// stub sets into one binary (which panics on duplicate descriptor registration).
func refShardForKey(ns, coll []byte, dataShards uint64) uint64 {
	const firstDataShard = uint64(2)
	if dataShards < 1 {
		dataShards = 1
	}
	var key []byte
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(ns)))
	key = append(key, l[:]...)
	key = append(key, ns...)
	binary.BigEndian.PutUint32(l[:], uint32(len(coll)))
	key = append(key, l[:]...)
	key = append(key, coll...)
	h := fnv.New64a()
	_, _ = h.Write(key)
	return firstDataShard + (h.Sum64() % dataShards)
}

func TestShardForKeyMatchesServerFormula(t *testing.T) {
	for _, n := range []uint64{1, 2, 3, 4, 8, 16, 64} {
		for i := 0; i < 400; i++ {
			ns := []byte(fmt.Sprintf("ns/%d", i%11))
			coll := []byte(fmt.Sprintf("col/%d", i))
			got := ShardForKey(ns, coll, n)
			want := refShardForKey(ns, coll, n)
			if got != want {
				t.Fatalf("n=%d ns=%q coll=%q: ShardForKey=%d ref=%d", n, ns, coll, got, want)
			}
			if got < 2 || got >= 2+n {
				t.Fatalf("n=%d: shard %d out of range [2,%d)", n, got, 2+n)
			}
		}
	}
}

func TestShardForKeyClampsDataShards(t *testing.T) {
	if got := ShardForKey([]byte("a"), []byte("b"), 0); got != 2 {
		t.Fatalf("dataShards=0: got %d want firstDataShard 2", got)
	}
}

// --- fake CollectionService server: serves TierInfo + records which core each SAdd lands on ---

type fakeCore struct {
	wavespanv1.UnimplementedCollectionServiceServer
	idx      int // this core's index (replicaId = idx+1)
	tier     *wavespanv1.TierInfoResult
	saddHits *int64 // per-core SAdd counter (shared slice element)
	tierHits *int64 // TierInfo call counter
}

func (f *fakeCore) TierInfo(_ context.Context, _ *wavespanv1.TierInfoRequest) (*wavespanv1.TierInfoResult, error) {
	atomic.AddInt64(f.tierHits, 1)
	return f.tier, nil
}

func (f *fakeCore) SAdd(_ context.Context, _ *wavespanv1.SAddRequest) (*wavespanv1.CountResult, error) {
	atomic.AddInt64(f.saddHits, 1)
	return &wavespanv1.CountResult{Count: 1}, nil
}

// startFakeCore boots a fake CollectionService on an ephemeral port, returning its address and a stop func.
func startFakeCore(t *testing.T, h *fakeCore) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	wavespanv1.RegisterCollectionServiceServer(srv, h)
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), srv.Stop
}

// tierWithLeaders builds a TierInfoResult that reports the given data shards each led by leaderReplicaID.
func tierWithLeaders(leaders map[uint64]uint64) *wavespanv1.TierInfoResult {
	t := &wavespanv1.TierInfoResult{Enabled: true, Voter: true, SelfReplicaId: 1}
	for shard, leader := range leaders {
		t.Shards = append(t.Shards, &wavespanv1.ShardStatus{
			ShardId: shard, LeaderReplicaId: leader, HasLeader: true, IsData: true,
		})
	}
	return t
}

// TestShardRouterRoutesToLeader brings up 3 fake cores, each leading a distinct data shard, and
// asserts that with shard-aware routing every SAdd lands on the core that leads the shard ShardForKey
// selects — i.e. the write reaches the leader directly, with no forward hop.
func TestShardRouterRoutesToLeader(t *testing.T) {
	const n = 3 // data shards 2,3,4 led by replicaIds 1,2,3 (core indexes 0,1,2)
	leaders := map[uint64]uint64{2: 1, 3: 2, 4: 3}

	saddHits := make([]int64, 3)
	tierHits := make([]int64, 3)
	cores := make([]string, 3)
	for i := 0; i < 3; i++ {
		addr, stop := startFakeCore(t, &fakeCore{
			idx: i, tier: tierWithLeaders(leaders), saddHits: &saddHits[i], tierHits: &tierHits[i],
		})
		defer stop()
		cores[i] = addr
	}

	client, err := Dial(Options{}.WithShardAwareRouting(cores, n))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	coll := client.Collections()

	ctx := context.Background()
	ns := "bench"
	for i := 0; i < 200; i++ {
		c := []byte(fmt.Sprintf("col/%d", i))
		if _, err := coll.SAdd(ctx, ns, c, []byte("m")); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
		// Every write must have gone to the leader of its shard.
		shard := ShardForKey([]byte(ns), c, n)
		wantCore := int(leaders[shard]) - 1
		if got := atomic.LoadInt64(&saddHits[wantCore]); got == 0 {
			t.Fatalf("col %d -> shard %d: leader core %d received no SAdd", i, shard, wantCore)
		}
	}

	// Sanity: each core that leads a hit shard saw at least one write, and at least one TierInfo
	// discovery call was made.
	var totalTier int64
	for i := range tierHits {
		totalTier += atomic.LoadInt64(&tierHits[i])
	}
	if totalTier == 0 {
		t.Fatalf("router never called TierInfo to discover leaders")
	}
}

// TestShardRouterConcurrent drives the shard-aware client from many goroutines to exercise the
// RWMutex-guarded routing table under -race.
func TestShardRouterConcurrent(t *testing.T) {
	const n = 4
	leaders := map[uint64]uint64{2: 1, 3: 2, 4: 1, 5: 2}
	saddHits := make([]int64, 2)
	tierHits := make([]int64, 2)
	cores := make([]string, 2)
	for i := 0; i < 2; i++ {
		addr, stop := startFakeCore(t, &fakeCore{
			idx: i, tier: tierWithLeaders(leaders), saddHits: &saddHits[i], tierHits: &tierHits[i],
		})
		defer stop()
		cores[i] = addr
	}

	client, err := Dial(Options{}.WithShardAwareRouting(cores, n))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	coll := client.Collections()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c := []byte(fmt.Sprintf("g%d/col/%d", g, i))
				if _, err := coll.SAdd(context.Background(), "bench", c, []byte("m")); err != nil {
					t.Errorf("SAdd: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if atomic.LoadInt64(&saddHits[0])+atomic.LoadInt64(&saddHits[1]) != 16*100 {
		t.Fatalf("expected %d writes, got %d", 16*100, saddHits[0]+saddHits[1])
	}
}

// TestDefaultPathUnchanged confirms that without WithShardAwareRouting no router is built and writes
// go to the single endpoint (default behavior, no per-core fan-out).
func TestDefaultPathUnchanged(t *testing.T) {
	var saddHits, tierHits int64
	addr, stop := startFakeCore(t, &fakeCore{idx: 0, tier: tierWithLeaders(nil), saddHits: &saddHits, tierHits: &tierHits})
	defer stop()

	client, err := Dial(Options{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	if client.router != nil {
		t.Fatal("default client must not build a shard router")
	}
	if _, err := client.Collections().SAdd(context.Background(), "ns", []byte("c"), []byte("m")); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if atomic.LoadInt64(&saddHits) != 1 {
		t.Fatalf("default SAdd hits = %d want 1", saddHits)
	}
	if atomic.LoadInt64(&tierHits) != 0 {
		t.Fatalf("default path must not call TierInfo, got %d", tierHits)
	}
}
