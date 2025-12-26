// Package diagnostic provides types for reporting errors, warnings, and hints.
package diagnostic

import (
	"fmt"

	"github.com/cloudygreybeard/kqlparser/token"
)

// Severity indicates the importance of a diagnostic.
type Severity int

const (
	SeverityError Severity = iota
	SeverityWarning
	SeverityInfo
	SeverityHint
)

func (s Severity) String() string {
	switch s {
	case SeverityError:
		return "error"
	case SeverityWarning:
		return "warning"
	case SeverityInfo:
		return "info"
	case SeverityHint:
		return "hint"
	default:
		return "unknown"
	}
}

// Diagnostic represents a single diagnostic message.
type Diagnostic struct {
	Pos      token.Position // Start position
	End      token.Position // End position (may be zero)
	Severity Severity
	Code     Code   // Machine-readable code
	Message  string // Human-readable message
}

// Error implements the error interface.
func (d Diagnostic) Error() string {
	return fmt.Sprintf("%s: %s: %s", d.Pos, d.Severity, d.Message)
}

// String returns a human-readable representation.
func (d Diagnostic) String() string {
	return d.Error()
}

// Code is a machine-readable diagnostic code.
type Code string

// Diagnostic codes
const (
	// Name resolution errors
	CodeUnresolvedName     Code = "KQL001"
	CodeUnresolvedColumn   Code = "KQL002"
	CodeUnresolvedTable    Code = "KQL003"
	CodeUnresolvedFunction Code = "KQL004"

	// Type errors
	CodeTypeMismatch    Code = "KQL010"
	CodeInvalidOperand  Code = "KQL011"
	CodeInvalidArgument Code = "KQL012"
	CodeWrongArgCount   Code = "KQL013"
	CodeNotCallable     Code = "KQL014"
	CodeNotIndexable    Code = "KQL015"
	CodeInvalidOperator Code = "KQL016"

	// Semantic errors
	CodeDuplicateColumn         Code = "KQL020"
	CodeDuplicateVariable       Code = "KQL021"
	CodeInvalidAggregateContext Code = "KQL022"

	// Warnings
	CodeUnusedVariable Code = "KQL100"
	CodeDeprecated     Code = "KQL101"
)

// List is a collection of diagnostics.
type List []Diagnostic

// Err returns an error if the list contains errors, nil otherwise.
func (l List) Err() error {
	for _, d := range l {
		if d.Severity == SeverityError {
			return l
		}
	}
	return nil
}

// Error implements the error interface.
func (l List) Error() string {
	switch len(l) {
	case 0:
		return "no diagnostics"
	case 1:
		return l[0].Error()
	default:
		return fmt.Sprintf("%s (and %d more)", l[0].Error(), len(l)-1)
	}
}

// HasErrors returns true if the list contains any errors.
func (l List) HasErrors() bool {
	for _, d := range l {
		if d.Severity == SeverityError {
			return true
		}
	}
	return false
}

// Errors returns only the error-level diagnostics.
func (l List) Errors() List {
	var errs List
	for _, d := range l {
		if d.Severity == SeverityError {
			errs = append(errs, d)
		}
	}
	return errs
}

// Warnings returns only the warning-level diagnostics.
func (l List) Warnings() List {
	var warns List
	for _, d := range l {
		if d.Severity == SeverityWarning {
			warns = append(warns, d)
		}
	}
	return warns
}
