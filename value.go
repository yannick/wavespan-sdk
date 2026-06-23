package wavespan

import (
	"fmt"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
)

// Value is a Cypher property value as it appears on the wire.
type Value = wavespanv1.Value

// ValueToGo converts a Cypher [Value] to an idiomatic Go value:
//
//	null         → nil
//	bool         → bool
//	int          → int64
//	double       → float64
//	string       → string
//	bytes        → []byte
//	list         → []any
//	map          → map[string]any
//
// A nil Value (or an unset oneof) returns nil.
func ValueToGo(v *Value) any {
	if v == nil {
		return nil
	}
	switch b := v.GetValue().(type) {
	case *wavespanv1.Value_Null:
		return nil
	case *wavespanv1.Value_BoolValue:
		return b.BoolValue
	case *wavespanv1.Value_IntValue:
		return b.IntValue
	case *wavespanv1.Value_DoubleValue:
		return b.DoubleValue
	case *wavespanv1.Value_StringValue:
		return b.StringValue
	case *wavespanv1.Value_BytesValue:
		return b.BytesValue
	case *wavespanv1.Value_ListValue:
		items := b.ListValue.GetValues()
		out := make([]any, len(items))
		for i, it := range items {
			out[i] = ValueToGo(it)
		}
		return out
	case *wavespanv1.Value_MapValue:
		entries := b.MapValue.GetEntries()
		out := make(map[string]any, len(entries))
		for k, it := range entries {
			out[k] = ValueToGo(it)
		}
		return out
	default:
		return nil
	}
}

// GoToValue converts an idiomatic Go value to a Cypher [Value] for use as a query parameter. It
// accepts nil, bool, all signed/unsigned integer kinds, float32/float64, string, []byte, []any (and
// typed slices via the generic path), and map[string]any. Unsupported types return an error.
func GoToValue(v any) (*Value, error) {
	switch x := v.(type) {
	case nil:
		return &Value{Value: &wavespanv1.Value_Null{Null: true}}, nil
	case bool:
		return &Value{Value: &wavespanv1.Value_BoolValue{BoolValue: x}}, nil
	case int:
		return intValue(int64(x)), nil
	case int8:
		return intValue(int64(x)), nil
	case int16:
		return intValue(int64(x)), nil
	case int32:
		return intValue(int64(x)), nil
	case int64:
		return intValue(x), nil
	case uint:
		return intValue(int64(x)), nil
	case uint8:
		return intValue(int64(x)), nil
	case uint16:
		return intValue(int64(x)), nil
	case uint32:
		return intValue(int64(x)), nil
	case uint64:
		return intValue(int64(x)), nil
	case float32:
		return &Value{Value: &wavespanv1.Value_DoubleValue{DoubleValue: float64(x)}}, nil
	case float64:
		return &Value{Value: &wavespanv1.Value_DoubleValue{DoubleValue: x}}, nil
	case string:
		return &Value{Value: &wavespanv1.Value_StringValue{StringValue: x}}, nil
	case []byte:
		return &Value{Value: &wavespanv1.Value_BytesValue{BytesValue: x}}, nil
	case []any:
		list := &wavespanv1.ValueList{Values: make([]*Value, len(x))}
		for i, it := range x {
			pv, err := GoToValue(it)
			if err != nil {
				return nil, err
			}
			list.Values[i] = pv
		}
		return &Value{Value: &wavespanv1.Value_ListValue{ListValue: list}}, nil
	case map[string]any:
		m := &wavespanv1.ValueMap{Entries: make(map[string]*Value, len(x))}
		for k, it := range x {
			pv, err := GoToValue(it)
			if err != nil {
				return nil, err
			}
			m.Entries[k] = pv
		}
		return &Value{Value: &wavespanv1.Value_MapValue{MapValue: m}}, nil
	default:
		return nil, fmt.Errorf("unsupported parameter type %T", v)
	}
}

func intValue(i int64) *Value {
	return &Value{Value: &wavespanv1.Value_IntValue{IntValue: i}}
}
