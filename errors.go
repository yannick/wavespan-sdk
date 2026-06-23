package wavespan

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
)

// Error is the SDK's error type. It names the logical operation that failed and preserves the
// underlying Connect status code (via [Error.Code] / errors.Is on a [connect.Error]).
type Error struct {
	Op      string       // logical operation, e.g. "Put", "VectorSearch"
	Code    connect.Code // Connect status code; 0 when not a Connect error
	Message string
	err     error // wrapped cause (often a *connect.Error)
}

func (e *Error) Error() string {
	if e.Op != "" {
		return fmt.Sprintf("wavespan: %s: %s", e.Op, e.Message)
	}
	return "wavespan: " + e.Message
}

// Unwrap exposes the underlying cause so errors.Is/As reach the wrapped *connect.Error.
func (e *Error) Unwrap() error { return e.err }

// IsNotFound reports whether err is a "not found" condition (Connect CodeNotFound). Note that a
// missing KV key is reported via Record.Found, not as an error; this helps with services that do
// signal absence through a status code.
func IsNotFound(err error) bool { return CodeOf(err) == connect.CodeNotFound }

// IsUnavailable reports whether err indicates the node was unreachable/unavailable.
func IsUnavailable(err error) bool { return CodeOf(err) == connect.CodeUnavailable }

// CodeOf extracts the Connect status code from err, returning connect.CodeUnknown if there is none.
func CodeOf(err error) connect.Code {
	var ce *connect.Error
	if errors.As(err, &ce) {
		return ce.Code()
	}
	return connect.CodeUnknown
}

// wrapErr converts a transport/Connect error into a *Error, preserving the status code and cause.
func wrapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	e := &Error{Op: op, Message: err.Error(), err: err}
	var ce *connect.Error
	if errors.As(err, &ce) {
		e.Code = ce.Code()
		e.Message = ce.Message()
	}
	return e
}
