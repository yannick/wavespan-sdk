package wavespan

import (
	"crypto/tls"
	"testing"
)

func TestDialValidation(t *testing.T) {
	if _, err := Dial(Options{}); err == nil {
		t.Fatal("Dial with no endpoint should error")
	}
	c, err := Dial(Options{Endpoint: "localhost:7800"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	if got, want := c.Endpoint(), "localhost:7800"; got != want {
		t.Errorf("Endpoint = %q, want %q", got, want)
	}
}

func TestDialTLSTarget(t *testing.T) {
	c, err := Dial(Options{Endpoint: "node:7800", TLS: &tls.Config{}})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = c.Close() }()
	// gRPC dials host:port; the TLS choice is in the dial credentials, not a URL scheme.
	if got, want := c.Endpoint(), "node:7800"; got != want {
		t.Errorf("Endpoint = %q, want %q", got, want)
	}
}

func TestNormalizeTarget(t *testing.T) {
	cases := []struct {
		endpoint string
		want     string
	}{
		{"localhost:7800", "localhost:7800"},
		{"http://node:7800", "node:7800"},  // scheme stripped
		{"https://node:7800", "node:7800"}, // scheme stripped
		{"10.0.0.1:9000", "10.0.0.1:9000"},
	}
	for _, tc := range cases {
		if got := normalizeTarget(tc.endpoint); got != tc.want {
			t.Errorf("normalizeTarget(%q) = %q, want %q", tc.endpoint, got, tc.want)
		}
	}
}

func TestWriteOptionDefaults(t *testing.T) {
	o := newWriteOptions()
	if !o.requireOriginPlusOne {
		t.Error("require_origin_plus_one should default true")
	}
	WithoutOriginPlusOne()(&o)
	if o.requireOriginPlusOne {
		t.Error("WithoutOriginPlusOne should clear the flag")
	}
	WithTTL(1500)(&o)
	if o.ttlMs == nil || *o.ttlMs != 1500 {
		t.Errorf("WithTTL not applied: %v", o.ttlMs)
	}
	WithTTL(0)(&o)
	if o.ttlMs != nil {
		t.Error("WithTTL(0) should clear TTL")
	}
}

func TestReadOptionDefaults(t *testing.T) {
	o := newReadOptions()
	if !o.allowDynamicCache {
		t.Error("allow_dynamic_cache should default true")
	}
	WithoutDynamicCache()(&o)
	if o.allowDynamicCache {
		t.Error("WithoutDynamicCache should clear the flag")
	}
}
