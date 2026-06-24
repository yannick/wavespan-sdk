package wavespan

import (
	"context"
	"time"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
)

// CollectionsClient is the ergonomic client for the replicated-collections API (design/30): sets, hash
// tables, and sorted sets over the CP consensus tier. Writes are linearizable; reads default to
// bounded-stale local reads — pass linearizable=true for a quorum read. Obtain one via
// [Client.Collections].
type CollectionsClient struct {
	c    *Client
	idem string
}

// Collections returns the replicated-collections sub-client.
func (c *Client) Collections() *CollectionsClient { return &CollectionsClient{c: c} }

// WithIdempotencyKey returns a sub-client whose next write carries the given idempotency key, so a
// retry (after a timeout) applies exactly once and returns the original result (design/30 §13.12). Use
// a fresh key per logical write.
func (cc *CollectionsClient) WithIdempotencyKey(key string) *CollectionsClient {
	clone := *cc
	clone.idem = key
	return &clone
}

func (cc *CollectionsClient) idemPtr() *string {
	if cc.idem == "" {
		return nil
	}
	return &cc.idem
}

// FieldValue is a hash field/value pair.
type FieldValue struct {
	Field []byte
	Value []byte
}

// ScoredMember is a sorted-set member and its score.
type ScoredMember struct {
	Member []byte
	Score  float64
}

// --- Set ---

// SAdd adds members to the set, returning the number newly added.
func (cc *CollectionsClient) SAdd(ctx context.Context, namespace string, collection []byte, members ...[]byte) (uint64, error) {
	resp, err := cc.c.collections.SAdd(ctx, &wavespanv1.SAddRequest{
		Namespace: namespace, Collection: collection, Members: members, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("SAdd", err)
	}
	return resp.GetCount(), nil
}

// SAddTTL adds members that expire after ttl, returning the number newly added.
func (cc *CollectionsClient) SAddTTL(ctx context.Context, namespace string, collection []byte, ttl time.Duration, members ...[]byte) (uint64, error) {
	ms := ttl.Milliseconds()
	resp, err := cc.c.collections.SAdd(ctx, &wavespanv1.SAddRequest{
		Namespace: namespace, Collection: collection, Members: members, TtlMs: &ms, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("SAddTTL", err)
	}
	return resp.GetCount(), nil
}

// SRem removes members from the set, returning the number removed.
func (cc *CollectionsClient) SRem(ctx context.Context, namespace string, collection []byte, members ...[]byte) (uint64, error) {
	resp, err := cc.c.collections.SRem(ctx, &wavespanv1.KeysRequest{
		Namespace: namespace, Collection: collection, Keys: members, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("SRem", err)
	}
	return resp.GetCount(), nil
}

// SIsMember reports whether member is in the set.
func (cc *CollectionsClient) SIsMember(ctx context.Context, namespace string, collection, member []byte, linearizable bool) (bool, error) {
	resp, err := cc.c.collections.SIsMember(ctx, &wavespanv1.MemberRequest{
		Namespace: namespace, Collection: collection, Member: member, Linearizable: linearizable,
	})
	if err != nil {
		return false, wrapErr("SIsMember", err)
	}
	return resp.GetValue(), nil
}

// SCard returns the set cardinality.
func (cc *CollectionsClient) SCard(ctx context.Context, namespace string, collection []byte, linearizable bool) (uint64, error) {
	resp, err := cc.c.collections.SCard(ctx, &wavespanv1.CardRequest{
		Namespace: namespace, Collection: collection, Linearizable: linearizable,
	})
	if err != nil {
		return 0, wrapErr("SCard", err)
	}
	return resp.GetCount(), nil
}

// SMembers returns up to limit set members (0 = all).
func (cc *CollectionsClient) SMembers(ctx context.Context, namespace string, collection []byte, limit int, linearizable bool) ([][]byte, error) {
	resp, err := cc.c.collections.SMembers(ctx, &wavespanv1.RangeRequest{
		Namespace: namespace, Collection: collection, Limit: int32(limit), Linearizable: linearizable,
	})
	if err != nil {
		return nil, wrapErr("SMembers", err)
	}
	return resp.GetMembers(), nil
}

// --- Hash ---

// HSet sets hash fields, returning the number of new fields.
func (cc *CollectionsClient) HSet(ctx context.Context, namespace string, collection []byte, fields ...FieldValue) (uint64, error) {
	pb := make([]*wavespanv1.FieldValue, len(fields))
	for i, f := range fields {
		pb[i] = &wavespanv1.FieldValue{Field: f.Field, Value: f.Value}
	}
	resp, err := cc.c.collections.HSet(ctx, &wavespanv1.HSetRequest{
		Namespace: namespace, Collection: collection, Fields: pb, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("HSet", err)
	}
	return resp.GetCount(), nil
}

// HDel deletes hash fields, returning the number removed.
func (cc *CollectionsClient) HDel(ctx context.Context, namespace string, collection []byte, fields ...[]byte) (uint64, error) {
	resp, err := cc.c.collections.HDel(ctx, &wavespanv1.KeysRequest{
		Namespace: namespace, Collection: collection, Keys: fields, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("HDel", err)
	}
	return resp.GetCount(), nil
}

// HGet returns a hash field value and whether it was present.
func (cc *CollectionsClient) HGet(ctx context.Context, namespace string, collection, field []byte, linearizable bool) ([]byte, bool, error) {
	resp, err := cc.c.collections.HGet(ctx, &wavespanv1.MemberRequest{
		Namespace: namespace, Collection: collection, Member: field, Linearizable: linearizable,
	})
	if err != nil {
		return nil, false, wrapErr("HGet", err)
	}
	return resp.GetValue(), resp.GetFound(), nil
}

// HLen returns the number of hash fields.
func (cc *CollectionsClient) HLen(ctx context.Context, namespace string, collection []byte, linearizable bool) (uint64, error) {
	resp, err := cc.c.collections.HLen(ctx, &wavespanv1.CardRequest{
		Namespace: namespace, Collection: collection, Linearizable: linearizable,
	})
	if err != nil {
		return 0, wrapErr("HLen", err)
	}
	return resp.GetCount(), nil
}

// HGetAll returns up to limit hash field/value pairs (0 = all).
func (cc *CollectionsClient) HGetAll(ctx context.Context, namespace string, collection []byte, limit int, linearizable bool) ([]FieldValue, error) {
	resp, err := cc.c.collections.HGetAll(ctx, &wavespanv1.RangeRequest{
		Namespace: namespace, Collection: collection, Limit: int32(limit), Linearizable: linearizable,
	})
	if err != nil {
		return nil, wrapErr("HGetAll", err)
	}
	out := make([]FieldValue, len(resp.GetFields()))
	for i, f := range resp.GetFields() {
		out[i] = FieldValue{Field: f.GetField(), Value: f.GetValue()}
	}
	return out, nil
}

// HIncrBy atomically adds delta to an integer hash field and returns the new value (exact under
// concurrency). Fails with InvalidArgument if the field's value is not an integer.
func (cc *CollectionsClient) HIncrBy(ctx context.Context, namespace string, collection, field []byte, delta int64) (int64, error) {
	resp, err := cc.c.collections.HIncrBy(ctx, &wavespanv1.HIncrByRequest{
		Namespace: namespace, Collection: collection, Field: field, Delta: delta, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("HIncrBy", err)
	}
	return resp.GetValue(), nil
}

// HIncrByFloat atomically adds delta to a float hash field and returns the new value.
func (cc *CollectionsClient) HIncrByFloat(ctx context.Context, namespace string, collection, field []byte, delta float64) (float64, error) {
	resp, err := cc.c.collections.HIncrByFloat(ctx, &wavespanv1.HIncrByFloatRequest{
		Namespace: namespace, Collection: collection, Field: field, Delta: delta, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("HIncrByFloat", err)
	}
	return resp.GetValue(), nil
}

// --- Sorted set ---

// ZAdd adds or updates sorted-set members, returning the number newly added.
func (cc *CollectionsClient) ZAdd(ctx context.Context, namespace string, collection []byte, members ...ScoredMember) (uint64, error) {
	pb := make([]*wavespanv1.ScoredMember, len(members))
	for i, m := range members {
		pb[i] = &wavespanv1.ScoredMember{Member: m.Member, Score: m.Score}
	}
	resp, err := cc.c.collections.ZAdd(ctx, &wavespanv1.ZAddRequest{
		Namespace: namespace, Collection: collection, Members: pb, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("ZAdd", err)
	}
	return resp.GetCount(), nil
}

// ZRem removes sorted-set members, returning the number removed.
func (cc *CollectionsClient) ZRem(ctx context.Context, namespace string, collection []byte, members ...[]byte) (uint64, error) {
	resp, err := cc.c.collections.ZRem(ctx, &wavespanv1.KeysRequest{
		Namespace: namespace, Collection: collection, Keys: members, IdempotencyKey: cc.idemPtr(),
	})
	if err != nil {
		return 0, wrapErr("ZRem", err)
	}
	return resp.GetCount(), nil
}

// ZScore returns a member's score and whether it was present.
func (cc *CollectionsClient) ZScore(ctx context.Context, namespace string, collection, member []byte, linearizable bool) (float64, bool, error) {
	resp, err := cc.c.collections.ZScore(ctx, &wavespanv1.MemberRequest{
		Namespace: namespace, Collection: collection, Member: member, Linearizable: linearizable,
	})
	if err != nil {
		return 0, false, wrapErr("ZScore", err)
	}
	return resp.GetScore(), resp.GetFound(), nil
}

// ZCard returns the sorted-set cardinality.
func (cc *CollectionsClient) ZCard(ctx context.Context, namespace string, collection []byte, linearizable bool) (uint64, error) {
	resp, err := cc.c.collections.ZCard(ctx, &wavespanv1.CardRequest{
		Namespace: namespace, Collection: collection, Linearizable: linearizable,
	})
	if err != nil {
		return 0, wrapErr("ZCard", err)
	}
	return resp.GetCount(), nil
}

// ZRange returns members in ascending score order (limit 0 = all).
func (cc *CollectionsClient) ZRange(ctx context.Context, namespace string, collection []byte, limit int, linearizable bool) ([]ScoredMember, error) {
	resp, err := cc.c.collections.ZRange(ctx, &wavespanv1.RangeRequest{
		Namespace: namespace, Collection: collection, Limit: int32(limit), Linearizable: linearizable,
	})
	if err != nil {
		return nil, wrapErr("ZRange", err)
	}
	out := make([]ScoredMember, len(resp.GetMembers()))
	for i, m := range resp.GetMembers() {
		out[i] = ScoredMember{Member: m.GetMember(), Score: m.GetScore()}
	}
	return out, nil
}

// --- Bulk / namespace ---

// BulkRemoveEntry is the per-collection result of BulkRemove.
type BulkRemoveEntry struct {
	Collection []byte
	Removed    uint64
	Error      string // empty = success
}

// BulkRemove removes members from many collections at once: the named collections, or (when
// collections is empty) every collection in the namespace. The removal is type-agnostic and
// best-effort across shards; per-collection results are returned (design/30 §13.7).
func (cc *CollectionsClient) BulkRemove(ctx context.Context, namespace string, collections, members [][]byte) ([]BulkRemoveEntry, error) {
	resp, err := cc.c.collections.BulkRemove(ctx, &wavespanv1.BulkRemoveRequest{
		Namespace: namespace, Collections: collections, Members: members,
	})
	if err != nil {
		return nil, wrapErr("BulkRemove", err)
	}
	out := make([]BulkRemoveEntry, len(resp.GetResults()))
	for i, e := range resp.GetResults() {
		out[i] = BulkRemoveEntry{Collection: e.GetCollection(), Removed: e.GetRemoved(), Error: e.GetError()}
	}
	return out, nil
}

// --- Tier status (operator) ---

// ShardStatus is one shard's leadership snapshot.
type ShardStatus struct {
	ShardID         uint64
	ReplicaID       uint64
	LeaderReplicaID uint64
	HasLeader       bool
	IsLeader        bool // this node currently leads the shard
	IsData          bool // true = data shard, false = meta shard
}

// TierStatus is a node's consensus-tier placement, active tunables, and per-shard leader status.
type TierStatus struct {
	Enabled            bool
	NodeHostID         string
	RaftAddress        string
	SelfReplicaID      uint64
	Voter              bool // false = spot/edge node
	RTTMs              uint64
	ElectionRTT        uint64
	HeartbeatRTT       uint64
	SnapshotEntries    uint64
	CompactionOverhead uint64
	SweepMs            uint64
	Shards             []ShardStatus
}

// TierInfo reports the consensus-tier status of the node serving this call (placement, tunables, and
// per-shard leader status) — a read-only operator view.
func (cc *CollectionsClient) TierInfo(ctx context.Context) (TierStatus, error) {
	resp, err := cc.c.collections.TierInfo(ctx, &wavespanv1.TierInfoRequest{})
	if err != nil {
		return TierStatus{}, wrapErr("TierInfo", err)
	}
	m := resp
	out := TierStatus{
		Enabled: m.GetEnabled(), NodeHostID: m.GetNodeHostId(), RaftAddress: m.GetRaftAddress(),
		SelfReplicaID: m.GetSelfReplicaId(), Voter: m.GetVoter(),
		RTTMs: m.GetRttMs(), ElectionRTT: m.GetElectionRtt(), HeartbeatRTT: m.GetHeartbeatRtt(),
		SnapshotEntries: m.GetSnapshotEntries(), CompactionOverhead: m.GetCompactionOverhead(), SweepMs: m.GetSweepMs(),
	}
	for _, s := range m.GetShards() {
		out.Shards = append(out.Shards, ShardStatus{
			ShardID: s.GetShardId(), ReplicaID: s.GetReplicaId(), LeaderReplicaID: s.GetLeaderReplicaId(),
			HasLeader: s.GetHasLeader(), IsLeader: s.GetIsLeader(), IsData: s.GetIsData(),
		})
	}
	return out, nil
}
