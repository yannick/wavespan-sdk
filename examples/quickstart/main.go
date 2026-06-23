// Command quickstart demonstrates the WaveSpan Go SDK against a running node (default data port
// localhost:7800). Start a node (e.g. `make container-single` or a local dev node) then run:
//
//	go run ./examples/quickstart --addr localhost:7800
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	wavespan "github.com/yannick/wavespan-sdk"
)

func main() {
	addr := flag.String("addr", "localhost:7800", "node data-port address host:port")
	flag.Parse()

	c, err := wavespan.Dial(wavespan.Options{Endpoint: *addr})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// --- Key/Value ---
	if _, err := c.Put(ctx, "demo", []byte("greeting"), []byte("hello, wavespan")); err != nil {
		log.Fatalf("put: %v", err)
	}
	rec, err := c.Get(ctx, "demo", []byte("greeting"))
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	if rec.Found {
		fmt.Printf("GET greeting = %q  (served by %s, source %s)\n",
			rec.Value, rec.Meta.GetServedByMemberId(), rec.Meta.GetSource())
	}

	// --- Range scan (streaming → iterator) ---
	_, _ = c.Put(ctx, "demo", []byte("a"), []byte("1"))
	_, _ = c.Put(ctx, "demo", []byte("b"), []byte("2"))
	scan, err := c.Scan(ctx, "demo", wavespan.WithScanMode(wavespan.ScanRoutedEventual))
	if err != nil {
		log.Fatalf("scan: %v", err)
	}
	fmt.Printf("SCAN demo (mode=%s, header completeness=%s):\n", scan.Mode(), scan.Completeness())
	for row, err := range scan.Rows() {
		if err != nil {
			log.Fatalf("scan row: %v", err)
		}
		fmt.Printf("  %s = %s\n", row.Key, row.Value)
	}
	fmt.Printf("  (final completeness=%s, rows=%d)\n", scan.FinalCompleteness(), scan.RowsReturned())

	// --- Cypher (Go-native parameters and row values) ---
	q, err := c.Query(ctx, "demo", "RETURN $n AS n, $s AS s", map[string]any{"n": 7, "s": "ok"})
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	for row, err := range q.Rows() {
		if err != nil {
			log.Fatalf("query row: %v", err)
		}
		fmt.Printf("CYPHER row: %v\n", map[string]any(row))
	}
}
