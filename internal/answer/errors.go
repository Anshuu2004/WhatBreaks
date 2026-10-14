package answer

import (
	"errors"
	"fmt"
)

var (
	// ErrCoreUnavailable is returned when the Rust core engine is unreachable or not connected.
	ErrCoreUnavailable = errors.New("core engine is currently unavailable")

	// ErrWorkspaceNotMaterialized is returned when the target workspace has not been materialized in the core engine.
	ErrWorkspaceNotMaterialized = errors.New("workspace state is not materialized")

	// ErrCoreTimeout is returned when the Rust core engine call exceeds the deadline.
	ErrCoreTimeout = errors.New("core engine request timed out")

	// ErrRequestCanceled is returned when the caller cancels the request context.
	ErrRequestCanceled = errors.New("request was cancelled")

	// ErrMalformedCoreResponse is returned when the core engine returns an incomplete or corrupt response.
	ErrMalformedCoreResponse = errors.New("core engine returned a malformed response")

	// ErrIdentityIntegrityRejected is returned when a query targets or occurs within a workspace
	// with incompatible legacy/ambiguous Kubernetes state.
	ErrIdentityIntegrityRejected = errors.New("identity integrity validation rejected: legacy or ambiguous kubernetes identity detected")
)

// IdentityIntegrityError indicates that a query was rejected because the target identity
// or workspace contains legacy or ambiguous Kubernetes identities that cannot safely determine impact.
type IdentityIntegrityError struct {
	WorkspaceID string
	Reason      string
	Err         error
}

func (e *IdentityIntegrityError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("identity integrity rejected: %s", e.Reason)
	}
	if e.Err != nil {
		return fmt.Sprintf("identity integrity rejected: %v", e.Err)
	}
	return "identity integrity rejected"
}

func (e *IdentityIntegrityError) Unwrap() error {
	return e.Err
}

// ValidationError indicates a client-provided parameter is invalid.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("validation failed on %s: %s", e.Field, e.Message)
	}
	return fmt.Sprintf("validation failed: %s", e.Message)
}

// ErrorResponse defines the standard JSON error structure matching the platform API conventions.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// Standard ErrorCode values matching platform conventions.
const (
	CodeBadRequest                = "BAD_REQUEST"
	CodeValidationError           = "VALIDATION_ERROR"
	CodeUnauthorized              = "UNAUTHORIZED"
	CodeForbidden                 = "FORBIDDEN"
	CodeNotFound                  = "NOT_FOUND"
	CodeCoreUnavailable           = "CORE_UNAVAILABLE"
	CodeWorkspaceNotMaterialized  = "WORKSPACE_NOT_MATERIALIZED"
	CodeCoreTimeout               = "CORE_TIMEOUT"
	CodeRequestCanceled           = "REQUEST_CANCELED"
	CodeInternalError             = "INTERNAL_ERROR"
	CodeIdentityIntegrityRejected = "IDENTITY_INTEGRITY_REJECTED"
)
