package wavespan

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Error is the SDK's error type. It names the logical operation that failed and preserves the
// underlying gRPC status code (via [Error.Code] / [CodeOf]).
type Error struct {
	Op      string     // logical operation, e.g. "Put", "VectorSearch"
	Code    codes.Code // gRPC status code; codes.OK when not a gRPC status error
	Message string
	err     error // wrapped cause (often carries a *status.Status)
}

func (e *Error) Error() string {
	if e.Op != "" {
		return fmt.Sprintf("wavespan: %s: %s", e.Op, e.Message)
	}
	return "wavespan: " + e.Message
}

// Unwrap exposes the underlying cause so errors.Is/As reach the wrapped gRPC error.
func (e *Error) Unwrap() error { return e.err }

// IsNotFound reports whether err is a "not found" condition (gRPC codes.NotFound). Note that a
// missing KV key is reported via Record.Found, not as an error; this helps with services that do
// signal absence through a status code.
func IsNotFound(err error) bool { return CodeOf(err) == codes.NotFound }

// IsUnavailable reports whether err indicates the node was unreachable/unavailable.
func IsUnavailable(err error) bool { return CodeOf(err) == codes.Unavailable }

// CodeOf extracts the gRPC status code from err, returning codes.Unknown if there is none.
func CodeOf(err error) codes.Code {
	return status.Code(err)
}

// wrapErr converts a transport/gRPC error into a *Error, preserving the status code and cause.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	e := &Error{Op: op, Message: err.Error(), err: err}
	if st, ok := status.FromError(err); ok {
		e.Code = st.Code()
		e.Message = st.Message()
	}
	return e
}
