package wavespan

import (
	"context"
	"io"
	"iter"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
	"google.golang.org/grpc"
)

// QueryMeta is the honest metadata terminating every Cypher result stream: which members
// participated, the completeness, and whether partial graph state could have been observed.
type QueryMeta = wavespanv1.QueryMeta

// Query runs a Cypher query against graphID and returns a streaming [QueryResult]. params values may
// be any Go value supported by [GoToValue] (nil, bool, integers, floats, string, []byte, slices,
// maps); pass nil for no parameters.
func (c *Client) Query(ctx context.Context, graphID, query string, params map[string]any) (*QueryResult, error) {
	pb, err := paramsToProto(params)
	if err != nil {
		return nil, err
	}
	stream, err := c.cypher.Query(ctx, &wavespanv1.CypherRequest{
		GraphId:    graphID,
		Query:      query,
		Parameters: pb,
	})
	if err != nil {
		return nil, wrapErr("Query", err)
	}
	return &QueryResult{stream: stream}, nil
}

// QueryResult is a live Cypher result stream. Iterate result rows with [QueryResult.Rows]; read
// [QueryResult.Meta] after the stream is fully drained.
type QueryResult struct {
	stream grpc.ServerStreamingClient[wavespanv1.CypherResult]
	meta   *QueryMeta
}

// Meta reports the terminal query metadata; valid only after Rows() is fully drained.
func (q *QueryResult) Meta() *QueryMeta { return q.meta }

// Row is one Cypher result row: column name → Go-native value (see [ValueToGo]).
type Row map[string]any

// Rows returns an iterator over result rows. The terminal QueryMeta frame is captured into Meta()
// when the stream ends; a transport error surfaces as a non-nil error in the final step.
func (q *QueryResult) Rows() iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		for {
			msg, err := q.stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				yield(nil, wrapErr("Query", err))
				return
			}
			switch m := msg.Msg.(type) {
			case *wavespanv1.CypherResult_Row:
				if !yield(rowToGo(m.Row), nil) {
					return
				}
			case *wavespanv1.CypherResult_Meta:
				q.meta = m.Meta
				return
			}
		}
	}
}

func rowToGo(r *wavespanv1.CypherRow) Row {
	out := make(Row, len(r.GetColumns()))
	for k, v := range r.GetColumns() {
		out[k] = ValueToGo(v)
	}
	return out
}

func paramsToProto(params map[string]any) (map[string]*wavespanv1.Value, error) {
	if len(params) == 0 {
		return nil, nil
	}
	out := make(map[string]*wavespanv1.Value, len(params))
	for k, v := range params {
		pv, err := GoToValue(v)
		if err != nil {
			return nil, &Error{Op: "Query", Message: "parameter " + k + ": " + err.Error()}
		}
		out[k] = pv
	}
	return out, nil
}
