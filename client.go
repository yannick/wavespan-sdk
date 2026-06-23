package wavespan

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1/wavespanv1connect"
	"golang.org/x/net/http2"
)

// defaultDialTimeout bounds connection establishment for the SDK's default HTTP client. It does NOT
// cap per-call duration — that is governed by the context passed to each method, so long-lived
// server streams (Scan, Query) are not truncated.
const defaultDialTimeout = 10 * time.Second

// Options configures a [Client]. The zero value is invalid; at least one endpoint is required.
type Options struct {
	// Endpoint is a single node's data-port address ("host:port", e.g. "localhost:7800"). Either
	// Endpoint or Endpoints must be set; Endpoint is shorthand for a one-element Endpoints.
	Endpoint string

	// Endpoints lists node data-port addresses. The first is used for all calls; the remainder are
	// reserved for future client-side failover/load-balancing. A full "http://host:port" or
	// "https://host:port" URL is also accepted (its scheme overrides TLS inference).
	Endpoints []string

	// TLS, when non-nil, switches the transport to https and uses this config for the handshake.
	// Leave nil for plaintext (the default for local/dev clusters). Ignored for endpoints given as
	// explicit "http://"/"https://" URLs.
	TLS *tls.Config

	// Token, when non-empty, is sent as "Authorization: Bearer <token>" on every request.
	Token string

	// HTTPClient overrides the transport. The default already uses HTTP/2 (ALPN over TLS, h2c over
	// plaintext); set this only to customize it (e.g. force HTTP/1.1, or use a shared client). When
	// set, TLS is assumed to be handled by this client.
	HTTPClient connect.HTTPClient

	// UserAgent overrides the User-Agent header (default: "wavespan-go").
	UserAgent string

	// Interceptors are appended after the SDK's own (auth/user-agent) interceptors.
	Interceptors []connect.Interceptor
}

// Client is a connection to a WaveSpan cluster. It is safe for concurrent use and should be reused;
// create one per cluster and call [Client.Close] when done.
type Client struct {
	baseURL string
	httpc   connect.HTTPClient
	owned   *http.Client // non-nil when the SDK created the client (so Close can idle it)

	kv          wavespanv1connect.KvServiceClient
	vector      wavespanv1connect.VectorServiceClient
	cypher      wavespanv1connect.CypherClient
	collections wavespanv1connect.CollectionServiceClient
}

// Dial constructs a [Client] from Options. It does not perform any network I/O — the first RPC
// establishes the connection — so a non-nil error means the options were invalid.
func Dial(opts Options) (*Client, error) {
	endpoints := opts.Endpoints
	if opts.Endpoint != "" {
		endpoints = append([]string{opts.Endpoint}, endpoints...)
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("wavespan: Dial requires Options.Endpoint or Options.Endpoints")
	}
	baseURL := normalizeBaseURL(endpoints[0], opts.TLS != nil)

	httpc := opts.HTTPClient
	var owned *http.Client
	if httpc == nil {
		owned = newHTTPClient(opts.TLS)
		httpc = owned
	}

	ua := opts.UserAgent
	if ua == "" {
		ua = "wavespan-go"
	}
	clientOpts := []connect.ClientOption{
		connect.WithInterceptors(headerInterceptor(opts.Token, ua)),
	}
	if len(opts.Interceptors) > 0 {
		clientOpts = append(clientOpts, connect.WithInterceptors(opts.Interceptors...))
	}

	return &Client{
		baseURL:     baseURL,
		httpc:       httpc,
		owned:       owned,
		kv:          wavespanv1connect.NewKvServiceClient(httpc, baseURL, clientOpts...),
		vector:      wavespanv1connect.NewVectorServiceClient(httpc, baseURL, clientOpts...),
		cypher:      wavespanv1connect.NewCypherClient(httpc, baseURL, clientOpts...),
		collections: wavespanv1connect.NewCollectionServiceClient(httpc, baseURL, clientOpts...),
	}, nil
}

// Close releases idle connections held by the SDK-owned HTTP client. It is a no-op when a custom
// HTTPClient was supplied (the caller owns that one). The Client must not be used after Close.
func (c *Client) Close() error {
	if c.owned != nil {
		c.owned.CloseIdleConnections()
	}
	return nil
}

// Endpoint reports the base URL the client is bound to (useful for logging).
func (c *Client) Endpoint() string { return c.baseURL }

// normalizeBaseURL turns "host:port" into a scheme-qualified URL, honoring an explicit scheme if the
// caller already provided one. tlsEnabled selects https when no scheme is present.
func normalizeBaseURL(endpoint string, tlsEnabled bool) string {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	scheme := "http"
	if tlsEnabled {
		scheme = "https"
	}
	return scheme + "://" + endpoint
}

// newHTTPClient builds the default transport. Both paths use HTTP/2, so concurrent calls multiplex
// over one connection instead of serializing per-connection on HTTP/1.1:
//   - TLS: the standard transport negotiates HTTP/2 via ALPN (ForceAttemptHTTP2).
//   - plaintext: an h2c (HTTP/2 cleartext) transport, since Go's stdlib won't do h2c automatically.
//
// Connect also works over HTTP/1.1; supply Options.HTTPClient to override either default.
func newHTTPClient(tlsCfg *tls.Config) *http.Client {
	if tlsCfg == nil {
		// h2c: speak HTTP/2 over a plain TCP connection (the "http" scheme). Mirrors the server's
		// H2CHandler so plaintext dev/cluster traffic is multiplexed, not HTTP/1.1.
		return &http.Client{Transport: &http2.Transport{
			AllowHTTP: true, // permit the "http" scheme
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr) // plain TCP, no TLS
			},
			ReadIdleTimeout: 30 * time.Second,
			PingTimeout:     10 * time.Second,
		}}
	}
	tr := &http.Transport{
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   defaultDialTimeout,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       tlsCfg,
	}
	// No client-level Timeout: it would also cap streaming reads. Per-call deadlines come from the
	// context handed to each method.
	return &http.Client{Transport: tr}
}

// headerInterceptor injects the User-Agent (and bearer token, if set) on every unary and streaming
// request. Applied uniformly so individual call sites never deal with auth headers.
func headerInterceptor(token, userAgent string) connect.Interceptor {
	return interceptorFunc{token: token, userAgent: userAgent}
}

type interceptorFunc struct {
	token     string
	userAgent string
}

func (i interceptorFunc) apply(h http.Header) {
	if i.userAgent != "" {
		h.Set("User-Agent", i.userAgent)
	}
	if i.token != "" {
		h.Set("Authorization", "Bearer "+i.token)
	}
}

func (i interceptorFunc) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		i.apply(req.Header())
		return next(ctx, req)
	}
}

func (i interceptorFunc) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		i.apply(conn.RequestHeader())
		return conn
	}
}

func (i interceptorFunc) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next // client SDK: no server-side handlers
}
