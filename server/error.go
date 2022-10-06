package server

import (
	"fmt"
	"net/http"

	"github.com/goccy/go-json"
)

// ServerError represents BigQuery errors.
// documentation is here.
// https://cloud.google.com/bigquery/docs/error-messages
type ServerError struct {
	Status    int         `json:"-"`
	Reason    ErrorReason `json:"reason"`
	Location  string      `json:"location"`
	DebugInfo string      `json:"debugInfo"`
	Message   string      `json:"message"`
}

type ResponseError struct {
	Error *ErrorFormat `json:"error"`
}

type ErrorFormat struct {
	Errors  []*ServerError `json:"errors"`
	Code    int            `json:"code"`
	Message string         `json:"message"`
}

func (e *ServerError) Response() []byte {
	b, _ := json.Marshal(&ResponseError{
		Error: &ErrorFormat{
			Errors:  []*ServerError{e},
			Code:    e.Status,
			Message: e.Message,
		},
	})
	return b
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("%s: %s", e.Reason, e.Message)
}

type ErrorReason string

const (
	AccessDenied             ErrorReason = "accessDenied"
	BackendError             ErrorReason = "backendError"
	BillingNotEnabled        ErrorReason = "billingNotEnabled"
	BillingTierLimitExceeded ErrorReason = "billingTierLimitExceeded"
	Blocked                  ErrorReason = "blocked"
	Duplicate                ErrorReason = "duplicate"
	InternalError            ErrorReason = "internalError"
	Invalid                  ErrorReason = "invalid"
	InvalidQuery             ErrorReason = "invalidQuery"
	InvalidUser              ErrorReason = "invalidUser"
	JobBackendError          ErrorReason = "jobBackendError"
	JobInternalError         ErrorReason = "jobInternalError"
	NotFound                 ErrorReason = "notFound"
	NotImplemented           ErrorReason = "notImplemented"
	QuotaExceeded            ErrorReason = "quotaExceeded"
	RateLimitExceeded        ErrorReason = "rateLimitExceeded"
	ResourceInUse            ErrorReason = "resourceInUse"
	ResourcesExceeded        ErrorReason = "resourcesExceeded"
	ResponseTooLarge         ErrorReason = "responseTooLarge"
	Stopped                  ErrorReason = "stopped"
	TableUnavailable         ErrorReason = "tableUnavailable"
	Timeout                  ErrorReason = "timeout"
)

func errAccessDenied(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  AccessDenied,
		Message: msg,
	}
}

func errBackendError(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusInternalServerError,
		Reason:  BackendError,
		Message: msg,
	}
}

func errBillingNotEnabled(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  BillingNotEnabled,
		Message: msg,
	}
}

func errBillingTierLimitExceeded(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  BillingTierLimitExceeded,
		Message: msg,
	}
}

func errBlocked(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  Blocked,
		Message: msg,
	}
}

func errDuplicate(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusConflict,
		Reason:  Duplicate,
		Message: msg,
	}
}

func errInternalError(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusInternalServerError,
		Reason:  InternalError,
		Message: msg,
	}
}

func errInvalid(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  Invalid,
		Message: msg,
	}
}

func errInvalidQuery(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  InvalidQuery,
		Message: msg,
	}
}

func errInvalidUser(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  InvalidUser,
		Message: msg,
	}
}

func errJobBackendError(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  JobBackendError,
		Message: msg,
	}
}

func errJobInternalError(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  JobInternalError,
		Message: msg,
	}
}

func errNotFound(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusNotFound,
		Reason:  NotFound,
		Message: msg,
	}
}

func errNotImplemented(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusNotImplemented,
		Reason:  NotImplemented,
		Message: msg,
	}
}

func errQuotaExceeded(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  QuotaExceeded,
		Message: msg,
	}
}

func errRateLimitExceeded(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  RateLimitExceeded,
		Message: msg,
	}
}

func errResourceInUse(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  ResourceInUse,
		Message: msg,
	}
}

func errResourcesExceeded(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  ResourcesExceeded,
		Message: msg,
	}
}

func errResponseTooLarge(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusForbidden,
		Reason:  ResponseTooLarge,
		Message: msg,
	}
}

func errStopped(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusOK,
		Reason:  Stopped,
		Message: msg,
	}
}

func errTableUnavailable(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  TableUnavailable,
		Message: msg,
	}
}

func errTimeout(msg string) *ServerError {
	return &ServerError{
		Status:  http.StatusBadRequest,
		Reason:  Timeout,
		Message: msg,
	}
}
