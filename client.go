package wavespan

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
	"google.golang.org/grpc"
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

	// ShardAwareCores / ShardAwareDataShards opt into shard-aware collection-write routing (off by
	// default). When ShardAwareCores is non-empty, the Collections() client routes each write straight
	// to the owning data shard's current leader (no per-op server forward hop), discovering leaders via
	// TierInfo. ShardAwareCores is the ordered list of core data-port addresses (index i = replicaId
	// i+1, matching WAVESPAN_COLLECTIONS_VOTERS order); ShardAwareDataShards is the hash directory
	// width N (the number of data shards, server default 4). Set both. Reads and non-collection APIs
	// are unaffected; the default single-endpoint path is unchanged. Prefer the [WithShardAwareRouting]
	// helper over setting these directly.
	ShardAwareCores      []string
	ShardAwareDataShards int
}

// WithShardAwareRouting sets opts to route collection writes directly to each shard's current leader,
// eliminating the per-op server forward hop. cores is the ordered list of core data-port addresses
// (index i = replicaId i+1, in WAVESPAN_COLLECTIONS_VOTERS order); dataShards is the hash directory
// width N (server default 4). This is opt-in; without it the SDK uses the single-endpoint path
// unchanged. Returns opts for chaining.
func (o Options) WithShardAwareRouting(cores []string, dataShards int) Options {
	o.ShardAwareCores = cores
	o.ShardAwareDataShards = dataShards
	return o
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
	backup      wavespanv1.BackupServiceClient

	// router is non-nil only when shard-aware collection-write routing is enabled (Options.ShardAwareCores).
	router *shardRouter
}

// Dial constructs a [Client] from Options. With grpc.NewClient the connection is established lazily,
// so a non-nil error here means the options (or dial settings) were invalid rather than a network
// failure — the first RPC surfaces connectivity problems.
func Dial(opts Options) (*Client, error) {
	endpoints := opts.Endpoints
	if opts.Endpoint != "" {
		endpoints = append([]string{opts.Endpoint}, endpoints...)
	}
	// Shard-aware routing supplies the cluster's core addresses; when no explicit Endpoint/Endpoints
	// are given, the first core also serves as the default (non-routed) endpoint, so callers can dial
	// with cores alone.
	if len(endpoints) == 0 && len(opts.ShardAwareCores) > 0 {
		endpoints = opts.ShardAwareCores
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("wavespan: Dial requires Options.Endpoint, Options.Endpoints, or Options.ShardAwareCores")
	}
	target := normalizeTarget(endpoints[0])

	// dialOptionsFor builds the SDK's transport posture (credentials + auth/user-agent interceptors +
	// caller dial options); the shard-aware router reuses it for its per-core conns.
	conn, err := grpc.NewClient(dialTargetFor(target), dialOptionsFor(opts)...)
	if err != nil {
		return nil, fmt.Errorf("wavespan: Dial: %w", err)
	}

	c := &Client{
		target:      target,
		conn:        conn,
		kv:          wavespanv1.NewKvServiceClient(conn),
		vector:      wavespanv1.NewVectorServiceClient(conn),
		cypher:      wavespanv1.NewCypherClient(conn),
		collections: wavespanv1.NewCollectionServiceClient(conn),
		budget:      wavespanv1.NewBudgetServiceClient(conn),
		backup:      wavespanv1.NewBackupServiceClient(conn),
	}

	if len(opts.ShardAwareCores) > 0 {
		router, rerr := newShardRouter(opts.ShardAwareCores, opts.ShardAwareDataShards, dialOptionsFor(opts))
		if rerr != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("wavespan: Dial: shard-aware routing: %w", rerr)
		}
		c.router = router
	}

	return c, nil
}

// dialTargetFor wraps a plain host:port in the passthrough resolver so grpc dials it via the OS
// resolver (correct IPv4/IPv6 happy-eyeballs for "localhost") instead of grpc.NewClient's default dns
// resolver, which can stall on localhost's dual-stack records. A target with its own scheme
// (dns:///, unix:, …) is left untouched. It also strips http(s):// via normalizeTarget.
func dialTargetFor(addr string) string {
	target := normalizeTarget(addr)
	if !strings.Contains(target, "://") {
		target = "passthrough:///" + target
	}
	return target
}

// Close closes the underlying *grpc.ClientConn (and the per-core conns of the shard-aware router, if
// enabled). The Client must not be used after Close.
func (c *Client) Close() error {
	if c.router != nil {
		c.router.close()
	}
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
