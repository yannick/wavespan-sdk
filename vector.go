package wavespan

import (
	"context"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
)

// VectorClient is the vector-as-key API (design/29): the embedding is the key and an opaque payload
// is the value. Obtain one via [Client.Vector]. The query/index vectors are plain float32 slices
// whose length must match the collection's configured dimensions.
type VectorClient struct{ c *Client }

// Vector returns the vector sub-client. It is cheap; the returned value shares the parent's
// connection.
func (c *Client) Vector() *VectorClient { return &VectorClient{c: c} }

// Put stores payload under the embedding vec in collection, returning the assigned version.
func (v *VectorClient) Put(ctx context.Context, collection string, vec []float32, payload []byte) (*Version, error) {
	resp, err := v.c.vector.VectorPut(ctx, &wavespanv1.VectorPutReq{
		Collection: collection,
		Vector:     vec,
		Payload:    payload,
	})
	if err != nil {
		return nil, wrapErr("VectorPut", err)
	}
	return resp.GetVersion(), nil
}

// Get returns the payload stored under the exact embedding vec. found is false when no such vector
// exists.
func (v *VectorClient) Get(ctx context.Context, collection string, vec []float32) (payload []byte, found bool, err error) {
	resp, err := v.c.vector.VectorGet(ctx, &wavespanv1.VectorGetReq{
		Collection: collection,
		Vector:     vec,
	})
	if err != nil {
		return nil, false, wrapErr("VectorGet", err)
	}
	return resp.GetPayload(), resp.GetFound(), nil
}

// Delete removes the vector keyed by the exact embedding vec.
func (v *VectorClient) Delete(ctx context.Context, collection string, vec []float32) error {
	_, err := v.c.vector.VectorDelete(ctx, &wavespanv1.VectorDeleteReq{
		Collection: collection,
		Vector:     vec,
	})
	if err != nil {
		return wrapErr("VectorDelete", err)
	}
	return nil
}

// Neighbor is one k-NN search hit. Distance is smaller-is-closer; Score is larger-is-more-similar.
type Neighbor struct {
	VectorID string
	Vector   []float32
	Payload  []byte // populated only when WithPayload() is set
	Distance float64
	Score    float64
}

// SearchResult bundles the neighbors with the honest completeness of a cluster-wide search.
type SearchResult struct {
	Neighbors    []Neighbor
	Completeness wavespanv1.Completeness
	Meta         *ResponseMeta
}

type searchOptions struct {
	nprobe         uint32
	efSearch       uint32
	rerank         bool
	includePayload bool
}

// SearchOption customizes a vector Search.
type SearchOption func(*searchOptions)

// WithNProbe sets how many coarse buckets to probe (IVF routing). 0 scatters to all holders.
func WithNProbe(n uint32) SearchOption { return func(o *searchOptions) { o.nprobe = n } }

// WithEfSearch sets the HNSW beam width for the ANN traversal.
func WithEfSearch(ef uint32) SearchOption { return func(o *searchOptions) { o.efSearch = ef } }

// WithRerank exact-rescores ANN candidates before returning, trading a little latency for accuracy.
func WithRerank() SearchOption { return func(o *searchOptions) { o.rerank = true } }

// WithPayload returns each neighbor's payload inline.
func WithPayload() SearchOption { return func(o *searchOptions) { o.includePayload = true } }

// Search runs a cluster-wide k-nearest-neighbour query for the given query vector.
func (v *VectorClient) Search(ctx context.Context, collection string, query []float32, k uint32, opts ...SearchOption) (*SearchResult, error) {
	o := searchOptions{}
	for _, fn := range opts {
		fn(&o)
	}
	msg, err := v.c.vector.VectorSearch(ctx, &wavespanv1.VectorSearchReq{
		Collection:     collection,
		Query:          query,
		K:              k,
		Nprobe:         o.nprobe,
		EfSearch:       o.efSearch,
		Rerank:         o.rerank,
		IncludePayload: o.includePayload,
	})
	if err != nil {
		return nil, wrapErr("VectorSearch", err)
	}
	neighbors := make([]Neighbor, 0, len(msg.GetNeighbors()))
	for _, n := range msg.GetNeighbors() {
		neighbors = append(neighbors, Neighbor{
			VectorID: n.GetVectorId(),
			Vector:   n.GetVector(),
			Payload:  n.GetPayload(),
			Distance: n.GetDistance(),
			Score:    n.GetScore(),
		})
	}
	return &SearchResult{
		Neighbors:    neighbors,
		Completeness: msg.GetCompleteness(),
		Meta:         msg.GetMeta(),
	}, nil
}
