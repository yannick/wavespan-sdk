// Package wavespan is the official Go client SDK for WaveSpan, a Kubernetes-native,
// eventually-consistent distributed KV / graph / vector store.
//
// It wraps the generated grpc-go stubs with typed, ergonomic methods so callers never touch
// gRPC request/stream plumbing or transport setup directly. The SDK covers the data-plane services
// exposed on a node's data port (default :7800):
//
//   - Key/Value:  [Client.Put], [Client.Get], [Client.MultiGet], [Client.Delete], [Client.Scan]
//   - Vectors:    [Client.Vector] → [VectorClient.Put]/[VectorClient.Get]/[VectorClient.Search]/…
//   - Cypher:     [Client.Query] (streaming graph queries with Go-native parameter/row values)
//
// # Quick start
//
//	c, err := wavespan.Dial(wavespan.Options{Endpoint: "localhost:7800"})
//	if err != nil { log.Fatal(err) }
//	defer c.Close()
//
//	if _, err := c.Put(ctx, "users", []byte("u1"), []byte("alice")); err != nil { … }
//	rec, err := c.Get(ctx, "users", []byte("u1"))
//	if err == nil && rec.Found { fmt.Printf("%s\n", rec.Value) }
//
// Every read carries a [ResponseMeta] (served-by node, read source, completeness) so the consistency
// mode of a result is never hidden — WaveSpan is eventually consistent and the SDK surfaces that
// honestly rather than papering over it.
//
// The SDK is safe for concurrent use by multiple goroutines and multiplexes all RPCs over a single
// gRPC connection (*grpc.ClientConn).
package wavespan
