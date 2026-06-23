// Module github.com/yannick/wavespan-sdk is the ergonomic Go client SDK for WaveSpan. It wraps the
// generated Connect stubs (vendored under internal/gen, regenerated from ./proto via `buf generate`)
// with typed methods, transport/auth management, and stream→iterator adapters.
//
// It depends ONLY on protobuf + Connect — never on the server — so `go get
// github.com/yannick/wavespan-sdk` stays clean and self-contained.
module github.com/yannick/wavespan-sdk

go 1.26.4

require (
	connectrpc.com/connect v1.20.0
	golang.org/x/net v0.56.0
	google.golang.org/protobuf v1.36.11
)

require golang.org/x/text v0.38.0 // indirect
