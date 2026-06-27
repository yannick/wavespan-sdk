package wavespan

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Options configures a [Client]. The zero value is invalid; at least one endpoint is required.
type Options struct {
	// Endpoint is a single node's data-port address ("host:port", e.g. "localhost:7800"). Either
	// Endpoint or Endpoints must be set; Endpoint is shorthand for a one-element Endpoints.
	Endpoint string

	// Endpoints lists node data-port addresses. The first is used for all calls; the remainder are
	// reserved for future client-side failover/load-balancing. A full "http://host:port" or
	// "https://host:port" URL is also accepted; the scheme is stripped (gRPC dials host:port).
	Endpoints []string

	// TLS, when non-nil, secures the gRPC connection with these credentials. Leave nil for plaintext
	// (the default for local/dev clusters), which uses insecure transport credentials.
	TLS *tls.Config

	// Token, when non-empty, is sent as "authorization: Bearer <token>" metadata on every RPC.
	Token string

	// DialOptions are appended after the SDK's own grpc.DialOptions (credentials + auth/user-agent
	// interceptor). Set this to customize the underlying *grpc.ClientConn (keepalive, balancer, …).
	DialOptions []grpc.DialOption

	// UserAgent overrides the user-agent metadata (default: "wavespan-go").
	UserAgent string

	// Interceptors are gRPC unary client interceptors applied (in order) after the SDK's own
	// auth/user-agent interceptor.
	//
	// NOTE: the SDK transport migrated from Connect to grpc-go. This field previously held
	// connect.Interceptor values; it now holds grpc.UnaryClientInterceptor. Callers that customized
	// interceptors must migrate. For full control over the connection (including stream
	// interceptors), use DialOptions instead.
	Interceptors []grpc.UnaryClientInterceptor
}

// Client is a connection to a WaveSpan cluster. It is safe for concurrent use and should be reused;
// create one per cluster and call [Client.Close] when done.
type Client struct {
	target string
	conn   *grpc.ClientConn

	kv          wavespanv1.KvServiceClient
	vector      wavespanv1.VectorServiceClient
	cypher      wavespanv1.CypherClient
	collections wavespanv1.CollectionServiceClient
	budget      wavespanv1.BudgetServiceClient
}

// Dial constructs a [Client] from Options. With grpc.NewClient the connection is established lazily,
// so a non-nil error here means the options (or dial settings) were invalid rather than a network
// failure — the first RPC surfaces connectivity problems.
func Dial(opts Options) (*Client, error) {
	endpoints := opts.Endpoints
	if opts.Endpoint != "" {
		endpoints = append([]string{opts.Endpoint}, endpoints...)
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("wavespan: Dial requires Options.Endpoint or Options.Endpoints")
	}
	target := normalizeTarget(endpoints[0])

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
	// The SDK's own metadata interceptor (auth bearer token + user-agent) runs first, then any
	// caller-supplied unary interceptors, chained in order.
	unary := append([]grpc.UnaryClientInterceptor{metadataInterceptor(opts.Token, ua)}, opts.Interceptors...)
	dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(unary...))
	dialOpts = append(dialOpts, grpc.WithChainStreamInterceptor(metadataStreamInterceptor(opts.Token, ua)))
	dialOpts = append(dialOpts, opts.DialOptions...)

	// Use the passthrough resolver for a plain host:port so grpc dials the address via the OS
	// resolver (correct IPv4/IPv6 happy-eyeballs for "localhost") instead of the dns resolver that
	// grpc.NewClient defaults to — the dns resolver can stall indefinitely on localhost's dual-stack
	// records. A target the caller gave with its own scheme (dns:///, unix:, …) is left untouched.
	dialTarget := target
	if !strings.Contains(dialTarget, "://") {
		dialTarget = "passthrough:///" + dialTarget
	}
	conn, err := grpc.NewClient(dialTarget, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("wavespan: Dial: %w", err)
	}

	return &Client{
		target:      target,
		conn:        conn,
		kv:          wavespanv1.NewKvServiceClient(conn),
		vector:      wavespanv1.NewVectorServiceClient(conn),
		cypher:      wavespanv1.NewCypherClient(conn),
		collections: wavespanv1.NewCollectionServiceClient(conn),
		budget:      wavespanv1.NewBudgetServiceClient(conn),
	}, nil
}

// Close closes the underlying *grpc.ClientConn. The Client must not be used after Close.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Endpoint reports the gRPC target the client is bound to (useful for logging).
func (c *Client) Endpoint() string { return c.target }

// normalizeTarget reduces a configured endpoint to the "host:port" target gRPC dials, stripping any
// "http://" / "https://" scheme a caller may have supplied (gRPC selects TLS via dial credentials,
// not a URL scheme).
func normalizeTarget(endpoint string) string {
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")
	return endpoint
}

// metadataInterceptor injects the user-agent (and bearer token, if set) as request metadata on every
// unary RPC, so individual call sites never deal with auth headers.
func metadataInterceptor(token, userAgent string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(authContext(ctx, token, userAgent), method, req, reply, cc, opts...)
	}
}

// metadataStreamInterceptor is the streaming counterpart of metadataInterceptor (used for Scan/Query).
func metadataStreamInterceptor(token, userAgent string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(authContext(ctx, token, userAgent), desc, cc, method, opts...)
	}
}

// authContext appends the SDK's user-agent and (optional) bearer-token metadata to ctx.
func authContext(ctx context.Context, token, userAgent string) context.Context {
	kv := make([]string, 0, 4)
	if userAgent != "" {
		kv = append(kv, "user-agent", userAgent)
	}
	if token != "" {
		kv = append(kv, "authorization", "Bearer "+token)
	}
	if len(kv) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, kv...)
}
