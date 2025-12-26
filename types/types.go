// Package types defines the KQL type system.
package types

import "fmt"

// Type represents a KQL type.
type Type interface {
	String() string
	Underlying() Type
	isType()
}

// Kind represents the kind of a scalar type.
type Kind int

const (
	Invalid Kind = iota
	Bool
	Int
	Long
	Real
	Decimal
	String
	DateTime
	TimeSpan
	Guid
	Dynamic
)

var kindNames = [...]string{
	Invalid:  "invalid",
	Bool:     "bool",
	Int:      "int",
	Long:     "long",
	Real:     "real",
	Decimal:  "decimal",
	String:   "string",
	DateTime: "datetime",
	TimeSpan: "timespan",
	Guid:     "guid",
	Dynamic:  "dynamic",
}

func (k Kind) String() string {
	if k >= 0 && int(k) < len(kindNames) {
		return kindNames[k]
	}
	return fmt.Sprintf("Kind(%d)", k)
}

// Scalar represents a scalar (primitive) type.
type Scalar struct {
	kind Kind
}

func (t *Scalar) String() string   { return t.kind.String() }
func (t *Scalar) Underlying() Type { return t }
func (t *Scalar) isType()          {}
func (t *Scalar) Kind() Kind       { return t.kind }

// Predefined scalar types
var (
	Typ_Invalid  = &Scalar{Invalid}
	Typ_Bool     = &Scalar{Bool}
	Typ_Int      = &Scalar{Int}
	Typ_Long     = &Scalar{Long}
	Typ_Real     = &Scalar{Real}
	Typ_Decimal  = &Scalar{Decimal}
	Typ_String   = &Scalar{String}
	Typ_DateTime = &Scalar{DateTime}
	Typ_TimeSpan = &Scalar{TimeSpan}
	Typ_Guid     = &Scalar{Guid}
	Typ_Dynamic  = &Scalar{Dynamic}
)

// Tabular represents a tabular type (table schema).
type Tabular struct {
	Columns []*Column
}

func (t *Tabular) String() string {
	if len(t.Columns) == 0 {
		return "()"
	}
	s := "("
	for i, col := range t.Columns {
		if i > 0 {
			s += ", "
		}
		s += col.Name + ": " + col.Type.String()
	}
	return s + ")"
}

func (t *Tabular) Underlying() Type { return t }
func (t *Tabular) isType()          {}

// Column returns the column with the given name, or nil if not found.
func (t *Tabular) Column(name string) *Column {
	for _, col := range t.Columns {
		if col.Name == name {
			return col
		}
	}
	return nil
}

// HasColumn reports whether the tabular type has a column with the given name.
func (t *Tabular) HasColumn(name string) bool {
	return t.Column(name) != nil
}

// Column represents a column in a tabular type.
type Column struct {
	Name string
	Type Type
}

// NewTabular creates a new tabular type with the given columns.
func NewTabular(columns ...*Column) *Tabular {
	return &Tabular{Columns: columns}
}

// NewColumn creates a new column.
func NewColumn(name string, typ Type) *Column {
	return &Column{Name: name, Type: typ}
}

// EmptyTabular is an empty tabular type.
var EmptyTabular = &Tabular{}

// Unknown represents an unknown or error type.
type Unknown struct {
	reason string
}

func (t *Unknown) String() string   { return "unknown" }
func (t *Unknown) Underlying() Type { return t }
func (t *Unknown) isType()          {}
func (t *Unknown) Reason() string   { return t.reason }

// Typ_Unknown is the singleton unknown type.
var Typ_Unknown = &Unknown{reason: "unknown"}

// NewUnknown creates an unknown type with a reason.
func NewUnknown(reason string) *Unknown {
	return &Unknown{reason: reason}
}

// IsScalar reports whether the type is a scalar type.
func IsScalar(t Type) bool {
	_, ok := t.(*Scalar)
	return ok
}

// IsTabular reports whether the type is a tabular type.
func IsTabular(t Type) bool {
	_, ok := t.(*Tabular)
	return ok
}

// IsNumeric reports whether the type is numeric (int, long, real, decimal).
func IsNumeric(t Type) bool {
	if s, ok := t.(*Scalar); ok {
		switch s.kind {
		case Int, Long, Real, Decimal:
			return true
		}
	}
	return false
}

// IsString reports whether the type is string.
func IsString(t Type) bool {
	if s, ok := t.(*Scalar); ok {
		return s.kind == String
	}
	return false
}

// IsBool reports whether the type is bool.
func IsBool(t Type) bool {
	if s, ok := t.(*Scalar); ok {
		return s.kind == Bool
	}
	return false
}

// IsTemporal reports whether the type is datetime or timespan.
func IsTemporal(t Type) bool {
	if s, ok := t.(*Scalar); ok {
		return s.kind == DateTime || s.kind == TimeSpan
	}
	return false
}

// Compatible reports whether two types are compatible for comparison/assignment.
func Compatible(a, b Type) bool {
	// Dynamic is compatible with everything
	if IsDynamic(a) || IsDynamic(b) {
		return true
	}

	// Same type is always compatible
	if Equal(a, b) {
		return true
	}

	// Numeric types are compatible with each other
	if IsNumeric(a) && IsNumeric(b) {
		return true
	}

	return false
}

// IsDynamic reports whether the type is dynamic.
func IsDynamic(t Type) bool {
	if s, ok := t.(*Scalar); ok {
		return s.kind == Dynamic
	}
	return false
}

// Equal reports whether two types are equal.
func Equal(a, b Type) bool {
	switch at := a.(type) {
	case *Scalar:
		if bt, ok := b.(*Scalar); ok {
			return at.kind == bt.kind
		}
	case *Tabular:
		if bt, ok := b.(*Tabular); ok {
			if len(at.Columns) != len(bt.Columns) {
				return false
			}
			for i, col := range at.Columns {
				if col.Name != bt.Columns[i].Name || !Equal(col.Type, bt.Columns[i].Type) {
					return false
				}
			}
			return true
		}
	case *Unknown:
		_, ok := b.(*Unknown)
		return ok
	}
	return false
}

// CommonType returns the common type of two types, for binary operations.
func CommonType(a, b Type) Type {
	if Equal(a, b) {
		return a
	}

	// Dynamic absorbs everything
	if IsDynamic(a) || IsDynamic(b) {
		return Typ_Dynamic
	}

	// Numeric promotion
	if IsNumeric(a) && IsNumeric(b) {
		// Promote to the "larger" type
		as, bs := a.(*Scalar), b.(*Scalar)
		if as.kind == Decimal || bs.kind == Decimal {
			return Typ_Decimal
		}
		if as.kind == Real || bs.kind == Real {
			return Typ_Real
		}
		if as.kind == Long || bs.kind == Long {
			return Typ_Long
		}
		return Typ_Int
	}

	return Typ_Unknown
}
