package wavespan

import (
	"reflect"
	"testing"
)

func TestValueRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want any // expected after GoToValue→ValueToGo (note integer widening to int64)
	}{
		{"nil", nil, nil},
		{"bool", true, true},
		{"int", 42, int64(42)},
		{"int64", int64(-7), int64(-7)},
		{"uint32", uint32(9), int64(9)},
		{"float64", 3.5, 3.5},
		{"float32", float32(2.0), float64(2.0)},
		{"string", "hi", "hi"},
		{"bytes", []byte{1, 2, 3}, []byte{1, 2, 3}},
		{"list", []any{int64(1), "x", true}, []any{int64(1), "x", true}},
		{"map", map[string]any{"a": int64(1), "b": "y"}, map[string]any{"a": int64(1), "b": "y"}},
		{"nested", []any{map[string]any{"k": []any{int64(2)}}}, []any{map[string]any{"k": []any{int64(2)}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pv, err := GoToValue(tc.in)
			if err != nil {
				t.Fatalf("GoToValue(%v): %v", tc.in, err)
			}
			got := ValueToGo(pv)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("round-trip = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestGoToValueUnsupported(t *testing.T) {
	if _, err := GoToValue(struct{ X int }{1}); err == nil {
		t.Error("expected error for unsupported type")
	}
}

func TestValueToGoNil(t *testing.T) {
	if got := ValueToGo(nil); got != nil {
		t.Errorf("ValueToGo(nil) = %v, want nil", got)
	}
}

func TestExplicitNull(t *testing.T) {
	pv, err := GoToValue(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := ValueToGo(pv); got != nil {
		t.Errorf("explicit null round-trips to %v, want nil", got)
	}
}
