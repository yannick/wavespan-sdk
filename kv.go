package wavespan

import (
	"context"
	"io"
	"iter"
	"time"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
	"google.golang.org/grpc"
)

// Version is the per-mutation hybrid-logical-clock stamp returned by writes and reads.
type Version = wavespanv1.Version

// ResponseMeta describes how a read was served (node, source, completeness, conflict state). It is
// attached to every user-visible read so the consistency mode is never hidden.
type ResponseMeta = wavespanv1.ResponseMeta

// WriteResult is the outcome of a Put or Delete.
type WriteResult struct {
	Version             *Version
	AckedNearbyReplicas uint32
	GeoSpillover        bool // Put only: the nearby replica landed in another geo
	Meta                *ResponseMeta
}

// Record is the result of a Get / MultiGet read.
type Record struct {
	Found     bool
	Value     []byte
	ExpiresAt *time.Time // nil when the record has no TTL
	Meta      *ResponseMeta
}

// Put writes value at (namespace, key). By default it requires origin+1 durability; see WriteOption.
func (c *Client) Put(ctx context.Context, namespace string, key, value []byte, opts ...WriteOption) (*WriteResult, error) {
	o := newWriteOptions()
	for _, fn := range opts {
		fn(&o)
	}
	req := &wavespanv1.PutRequest{
		Namespace:            namespace,
		Key:                  key,
		Value:                value,
		RequireOriginPlusOne: o.requireOriginPlusOne,
		TtlMs:                o.ttlMs,
	}
	if o.idempotencyKey != "" {
		req.IdempotencyKey = &o.idempotencyKey
	}
	m, err := c.kv.Put(ctx, req)
	if err != nil {
		return nil, wrapErr("Put", err)
	}
	return &WriteResult{
		Version:             m.GetVersion(),
		AckedNearbyReplicas: m.GetAckedNearbyReplicas(),
		GeoSpillover:        m.GetGeoSpillover(),
		Meta:                m.GetMeta(),
	}, nil
}

// Get reads (namespace, key). The returned Record's Found reports whether the key exists; a missing
// key is not an error.
func (c *Client) Get(ctx context.Context, namespace string, key []byte, opts ...ReadOption) (*Record, error) {
	o := newReadOptions()
	for _, fn := range opts {
		fn(&o)
	}
	resp, err := c.kv.Get(ctx, &wavespanv1.GetRequest{
		Namespace:         namespace,
		Key:               key,
		AllowDynamicCache: o.allowDynamicCache,
		HideExpiredOnRead: o.hideExpired,
	})
	if err != nil {
		return nil, wrapErr("Get", err)
	}
	return recordFromGet(resp), nil
}

// MultiGet reads many keys of one namespace in a single round-trip. Results are returned in request
// order, one Record per requested key.
func (c *Client) MultiGet(ctx context.Context, namespace string, keys [][]byte, opts ...ReadOption) ([]*Record, error) {
	o := newReadOptions()
	for _, fn := range opts {
		fn(&o)
	}
	resp, err := c.kv.MultiGet(ctx, &wavespanv1.MultiGetRequest{
		Namespace:         namespace,
		Keys:              keys,
		HideExpiredOnRead: o.hideExpired,
	})
	if err != nil {
		return nil, wrapErr("MultiGet", err)
	}
	out := make([]*Record, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		out = append(out, recordFromGet(r))
	}
	return out, nil
}

// Delete tombstones (namespace, key). Like Put it requires origin+1 by default.
func (c *Client) Delete(ctx context.Context, namespace string, key []byte, opts ...WriteOption) (*WriteResult, error) {
	o := newWriteOptions()
	for _, fn := range opts {
		fn(&o)
	}
	req := &wavespanv1.DeleteRequest{
		Namespace:            namespace,
		Key:                  key,
		RequireOriginPlusOne: o.requireOriginPlusOne,
	}
	if o.idempotencyKey != "" {
		req.IdempotencyKey = &o.idempotencyKey
	}
	m, err := c.kv.Delete(ctx, req)
	if err != nil {
		return nil, wrapErr("Delete", err)
	}
	return &WriteResult{
		Version:             m.GetVersion(),
		AckedNearbyReplicas: m.GetAckedNearbyReplicas(),
		Meta:                m.GetMeta(),
	}, nil
}

// ScanRow is one row of a range scan.
type ScanRow struct {
	Key       []byte
	Value     []byte
	Version   *Version
	ExpiresAt *time.Time
}

// Scan opens a range scan over namespace and returns a [ScanResult]. The stream header is consumed
// eagerly so [ScanResult.Mode] and [ScanResult.Completeness] are known before iterating rows. Always
// drain [ScanResult.Rows] (or cancel ctx) to release the stream.
func (c *Client) Scan(ctx context.Context, namespace string, opts ...ScanOption) (*ScanResult, error) {
	o := scanOptions{mode: ScanCacheFast}
	for _, fn := range opts {
		fn(&o)
	}
	stream, err := c.kv.Scan(ctx, &wavespanv1.ScanRequest{
		Namespace: namespace,
		StartKey:  o.start,
		EndKey:    o.end,
		Limit:     o.limit,
		Mode:      o.mode,
	})
	if err != nil {
		return nil, wrapErr("Scan", err)
	}
	sr := &ScanResult{stream: stream}
	// Eagerly read the header (the server always sends it first) so completeness is visible up front.
	first, err := stream.Recv()
	switch {
	case err == io.EOF:
		sr.exhausted = true
	case err != nil:
		return nil, wrapErr("Scan", err)
	default:
		switch m := first.Msg.(type) {
		case *wavespanv1.ScanResponse_Header:
			sr.mode = m.Header.GetMode()
			sr.completeness = m.Header.GetCompleteness()
			sr.meta = m.Header.GetMeta()
		default:
			sr.pending = first // unexpected first frame; replay it during Rows()
		}
	}
	return sr, nil
}

// ScanResult is a live range-scan stream. Iterate rows with [ScanResult.Rows]; read completeness and
// warnings before (header) and after (trailer) iteration.
type ScanResult struct {
	stream  grpc.ServerStreamingClient[wavespanv1.ScanResponse]
	pending *wavespanv1.ScanResponse
	meta    *ResponseMeta

	mode         ScanMode
	completeness wavespanv1.Completeness
	exhausted    bool

	finalCompleteness wavespanv1.Completeness
	warnings          []string
	rowsReturned      uint64
}

// Mode reports the scan strategy the server actually used.
func (s *ScanResult) Mode() ScanMode { return s.mode }

// Completeness reports the header completeness declared at the start of the scan.
func (s *ScanResult) Completeness() wavespanv1.Completeness { return s.completeness }

// Meta reports the response metadata from the scan header, if any.
func (s *ScanResult) Meta() *ResponseMeta { return s.meta }

// FinalCompleteness reports the trailer completeness; valid only after Rows() is fully drained.
func (s *ScanResult) FinalCompleteness() wavespanv1.Completeness { return s.finalCompleteness }

// Warnings reports trailer warnings; valid only after Rows() is fully drained.
func (s *ScanResult) Warnings() []string { return s.warnings }

// RowsReturned reports the server's trailer row count; valid only after Rows() is fully drained.
func (s *ScanResult) RowsReturned() uint64 { return s.rowsReturned }

// Rows returns an iterator over the scan's rows. A transport error surfaces as a non-nil error in the
// final iteration step. The trailer (final completeness, warnings, row count) is captured when the
// stream ends. Break early to stop; the underlying stream is closed when iteration finishes.
func (s *ScanResult) Rows() iter.Seq2[*ScanRow, error] {
	return func(yield func(*ScanRow, error) bool) {
		// Replay any non-header frame read while consuming the header.
		if s.pending != nil {
			p := s.pending
			s.pending = nil
			if !s.dispatch(p, yield) {
				return
			}
		}
		if s.exhausted {
			return
		}
		for {
			msg, err := s.stream.Recv()
			if err == io.EOF {
				s.exhausted = true
				return
			}
			if err != nil {
				yield(nil, wrapErr("Scan", err))
				return
			}
			if !s.dispatch(msg, yield) {
				return
			}
		}
	}
}

// dispatch yields a row, or records trailer state, for a single stream frame. It returns false when
// iteration should stop (consumer broke, or the trailer was reached).
func (s *ScanResult) dispatch(resp *wavespanv1.ScanResponse, yield func(*ScanRow, error) bool) bool {
	switch m := resp.Msg.(type) {
	case *wavespanv1.ScanResponse_Row:
		return yield(scanRow(m.Row), nil)
	case *wavespanv1.ScanResponse_Trailer:
		s.finalCompleteness = m.Trailer.GetFinalCompleteness()
		s.warnings = m.Trailer.GetWarnings()
		s.rowsReturned = m.Trailer.GetRowsReturned()
		s.exhausted = true
		return false
	default: // a stray header — ignore
		return true
	}
}

func scanRow(r *wavespanv1.ScanRow) *ScanRow {
	return &ScanRow{
		Key:       r.GetKey(),
		Value:     r.GetValue(),
		Version:   r.GetVersion(),
		ExpiresAt: unixMsPtr(r.ExpiresAtUnixMs),
	}
}

func recordFromGet(r *wavespanv1.GetResult) *Record {
	return &Record{
		Found:     r.GetFound(),
		Value:     r.GetValue(),
		ExpiresAt: unixMsPtr(r.ExpiresAtUnixMs),
		Meta:      r.GetMeta(),
	}
}

// unixMsPtr converts an optional unix-millis field to a *time.Time.
func unixMsPtr(ms *int64) *time.Time {
	if ms == nil {
		return nil
	}
	t := time.UnixMilli(*ms)
	return &t
}
