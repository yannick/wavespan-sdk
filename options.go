package wavespan

import wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"

// --- write options (Put / Delete) ---

type writeOptions struct {
	requireOriginPlusOne bool
	ttlMs                *int64
	idempotencyKey       string
}

func newWriteOptions() writeOptions {
	// require_origin_plus_one defaults true to match the server's durability contract: a write ACKs
	// only after a nearby durable replica also has it (design/03).
	return writeOptions{requireOriginPlusOne: true}
}

// WriteOption customizes a Put or Delete.
type WriteOption func(*writeOptions)

// WithoutOriginPlusOne ACKs as soon as the coordinating node has the write durably, without waiting
// for a nearby replica. Faster, weaker durability. The default is to require origin+1.
func WithoutOriginPlusOne() WriteOption {
	return func(o *writeOptions) { o.requireOriginPlusOne = false }
}

// WithTTL sets a time-to-live in milliseconds on a Put. Zero or negative removes any TTL intent.
func WithTTL(ms int64) WriteOption {
	return func(o *writeOptions) {
		if ms > 0 {
			o.ttlMs = &ms
		} else {
			o.ttlMs = nil
		}
	}
}

// WithIdempotencyKey collapses retries carrying the same key into a single logical mutation.
func WithIdempotencyKey(key string) WriteOption {
	return func(o *writeOptions) { o.idempotencyKey = key }
}

// --- read options (Get) ---

type readOptions struct {
	allowDynamicCache bool
	hideExpired       bool
}

func newReadOptions() readOptions {
	return readOptions{allowDynamicCache: true}
}

// ReadOption customizes a Get.
type ReadOption func(*readOptions)

// WithoutDynamicCache forces the read past any dynamic cache to a durable source.
func WithoutDynamicCache() ReadOption {
	return func(o *readOptions) { o.allowDynamicCache = false }
}

// WithHideExpired suppresses values whose TTL has elapsed but that have not yet been swept.
func WithHideExpired() ReadOption {
	return func(o *readOptions) { o.hideExpired = true }
}

// --- scan options ---

// ScanMode selects the range-scan strategy. See the package constants.
type ScanMode = wavespanv1.ScanMode

const (
	// ScanCacheFast reads local cache/durable only — fast, may be incomplete, never reports COMPLETE.
	ScanCacheFast ScanMode = wavespanv1.ScanMode_CACHE_FAST
	// ScanCacheComplete reads local cache and reports COMPLETE only with a valid coverage certificate.
	ScanCacheComplete ScanMode = wavespanv1.ScanMode_CACHE_COMPLETE
	// ScanRoutedEventual contacts known holders per subrange and merges — the most complete option.
	ScanRoutedEventual ScanMode = wavespanv1.ScanMode_ROUTED_EVENTUAL
	// ScanLocalOnly reads the local store only (debugging/analytics).
	ScanLocalOnly ScanMode = wavespanv1.ScanMode_LOCAL_ONLY
)

type scanOptions struct {
	start, end []byte
	limit      uint32
	mode       ScanMode
}

// ScanOption customizes a Scan.
type ScanOption func(*scanOptions)

// WithRange restricts the scan to [start, end). Either bound may be nil/empty for open-ended.
func WithRange(start, end []byte) ScanOption {
	return func(o *scanOptions) { o.start, o.end = start, end }
}

// WithLimit caps the number of rows returned (0 = unlimited).
func WithLimit(n uint32) ScanOption {
	return func(o *scanOptions) { o.limit = n }
}

// WithScanMode selects the scan strategy (default ScanCacheFast).
func WithScanMode(m ScanMode) ScanOption {
	return func(o *scanOptions) { o.mode = m }
}
