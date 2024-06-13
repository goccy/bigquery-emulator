package contentdata

type ContentDataError struct {
	Cause       error
	Message     string
	ErrorReason ContentDataErrorReason
}

type ContentDataErrorReason string

func Error(errorReason ContentDataErrorReason) *ContentDataError {
	return &ContentDataError{
		Cause:       nil,
		Message:     "",
		ErrorReason: errorReason,
	}
}

func ErrorWithMessage(errorReason ContentDataErrorReason, message string) *ContentDataError {
	return &ContentDataError{
		Message:     message,
		ErrorReason: errorReason,
	}
}

func ErrorWithCause(errorReason ContentDataErrorReason, cause error) *ContentDataError {
	return &ContentDataError{
		Cause:       cause,
		ErrorReason: errorReason,
	}
}

const (
	AlterTableExistingFieldRemoved ContentDataErrorReason = "alterTableExistingFieldRemoved"
	AlterTableExistingFieldChanged ContentDataErrorReason = "alterTableExistingFieldChanged"
	Unknown                        ContentDataErrorReason = "unknown"
)
