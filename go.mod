// Module github.com/yannick/wavespan-sdk is the ergonomic Go client SDK for WaveSpan. It wraps the
// generated gRPC stubs (vendored under internal/gen, regenerated from ./proto via `buf generate`)
// with typed methods, transport/auth management, and stream→iterator adapters.
//
// It depends ONLY on protobuf + gRPC — never on the server — so `go get
// github.com/yannick/wavespan-sdk` stays clean and self-contained.
module github.com/yannick/wavespan-sdk

go 1.26.4

require (
	golang.org/x/sys v0.46.0
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
)
