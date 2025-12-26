// Package symbol defines the symbol types used in semantic analysis.
package symbol

import (
	"github.com/cloudygreybeard/kqlparser/types"
)

// Symbol represents a named entity in KQL (table, column, function, etc.).
type Symbol interface {
	Name() string
	Kind() SymbolKind
	Type() types.Type
	isSymbol()
}

// SymbolKind represents the kind of symbol.
type SymbolKind int

const (
	SymbolInvalid SymbolKind = iota
	SymbolColumn
	SymbolTable
	SymbolFunction
	SymbolAggregate
	SymbolVariable
	SymbolParameter
	SymbolDatabase
	SymbolCluster
)

var symbolKindNames = [...]string{
	SymbolInvalid:   "invalid",
	SymbolColumn:    "column",
	SymbolTable:     "table",
	SymbolFunction:  "function",
	SymbolAggregate: "aggregate",
	SymbolVariable:  "variable",
	SymbolParameter: "parameter",
	SymbolDatabase:  "database",
	SymbolCluster:   "cluster",
}

func (k SymbolKind) String() string {
	if k >= 0 && int(k) < len(symbolKindNames) {
		return symbolKindNames[k]
	}
	return "unknown"
}

// baseSymbol provides common fields for all symbols.
type baseSymbol struct {
	name string
}

func (s *baseSymbol) Name() string { return s.name }

// ColumnSymbol represents a column in a table or result set.
type ColumnSymbol struct {
	baseSymbol
	typ             types.Type
	OriginalColumns []*ColumnSymbol // For computed columns that reference others
}

func NewColumn(name string, typ types.Type) *ColumnSymbol {
	return &ColumnSymbol{
		baseSymbol: baseSymbol{name: name},
		typ:        typ,
	}
}

func (s *ColumnSymbol) Kind() SymbolKind { return SymbolColumn }
func (s *ColumnSymbol) Type() types.Type { return s.typ }
func (s *ColumnSymbol) isSymbol()        {}

// TableSymbol represents a database table.
type TableSymbol struct {
	baseSymbol
	Schema *types.Tabular
}

func NewTable(name string, columns ...*types.Column) *TableSymbol {
	return &TableSymbol{
		baseSymbol: baseSymbol{name: name},
		Schema:     types.NewTabular(columns...),
	}
}

func NewTableWithSchema(name string, schema *types.Tabular) *TableSymbol {
	return &TableSymbol{
		baseSymbol: baseSymbol{name: name},
		Schema:     schema,
	}
}

func (s *TableSymbol) Kind() SymbolKind { return SymbolTable }
func (s *TableSymbol) Type() types.Type { return s.Schema }
func (s *TableSymbol) isSymbol()        {}

// Column returns the column with the given name.
func (s *TableSymbol) Column(name string) *types.Column {
	return s.Schema.Column(name)
}

// Columns returns all columns.
func (s *TableSymbol) Columns() []*types.Column {
	return s.Schema.Columns
}

// FunctionSymbol represents a scalar or tabular function.
type FunctionSymbol struct {
	baseSymbol
	Signatures  []*FunctionSignature
	Description string
}

// FunctionSignature represents one overload of a function.
type FunctionSignature struct {
	Parameters []*Parameter
	ReturnType types.Type
	MinArgs    int // Minimum required arguments
	MaxArgs    int // Maximum arguments (-1 for variadic)
}

// Parameter represents a function parameter.
type Parameter struct {
	Name         string
	Type         types.Type
	DefaultValue string // Optional default value
	IsOptional   bool
}

func NewFunction(name string, sigs ...*FunctionSignature) *FunctionSymbol {
	return &FunctionSymbol{
		baseSymbol: baseSymbol{name: name},
		Signatures: sigs,
	}
}

func NewScalarFunction(name string, returnType types.Type, params ...*Parameter) *FunctionSymbol {
	minArgs := 0
	for _, p := range params {
		if !p.IsOptional {
			minArgs++
		}
	}
	sig := &FunctionSignature{
		Parameters: params,
		ReturnType: returnType,
		MinArgs:    minArgs,
		MaxArgs:    len(params),
	}
	return NewFunction(name, sig)
}

func NewVariadicFunction(name string, returnType types.Type, params ...*Parameter) *FunctionSymbol {
	minArgs := 0
	for _, p := range params {
		if !p.IsOptional {
			minArgs++
		}
	}
	sig := &FunctionSignature{
		Parameters: params,
		ReturnType: returnType,
		MinArgs:    minArgs,
		MaxArgs:    -1, // Variadic
	}
	return NewFunction(name, sig)
}

func (s *FunctionSymbol) Kind() SymbolKind { return SymbolFunction }
func (s *FunctionSymbol) Type() types.Type {
	if len(s.Signatures) > 0 {
		return s.Signatures[0].ReturnType
	}
	return types.Typ_Unknown
}
func (s *FunctionSymbol) isSymbol() {}

// AggregateSymbol represents an aggregate function (count, sum, etc.).
type AggregateSymbol struct {
	FunctionSymbol
}

func NewAggregate(name string, returnType types.Type, params ...*Parameter) *AggregateSymbol {
	fn := NewScalarFunction(name, returnType, params...)
	return &AggregateSymbol{FunctionSymbol: *fn}
}

func (s *AggregateSymbol) Kind() SymbolKind { return SymbolAggregate }

// VariableSymbol represents a local variable (from let statement).
type VariableSymbol struct {
	baseSymbol
	typ types.Type
}

func NewVariable(name string, typ types.Type) *VariableSymbol {
	return &VariableSymbol{
		baseSymbol: baseSymbol{name: name},
		typ:        typ,
	}
}

func (s *VariableSymbol) Kind() SymbolKind { return SymbolVariable }
func (s *VariableSymbol) Type() types.Type { return s.typ }
func (s *VariableSymbol) isSymbol()        {}

// ParameterSymbol represents a function parameter.
type ParameterSymbol struct {
	baseSymbol
	typ types.Type
}

func NewParameter(name string, typ types.Type) *ParameterSymbol {
	return &ParameterSymbol{
		baseSymbol: baseSymbol{name: name},
		typ:        typ,
	}
}

func (s *ParameterSymbol) Kind() SymbolKind { return SymbolParameter }
func (s *ParameterSymbol) Type() types.Type { return s.typ }
func (s *ParameterSymbol) isSymbol()        {}

// DatabaseSymbol represents a database.
type DatabaseSymbol struct {
	baseSymbol
	Tables    []*TableSymbol
	Functions []*FunctionSymbol
}

func NewDatabase(name string) *DatabaseSymbol {
	return &DatabaseSymbol{
		baseSymbol: baseSymbol{name: name},
	}
}

func (s *DatabaseSymbol) Kind() SymbolKind { return SymbolDatabase }
func (s *DatabaseSymbol) Type() types.Type { return types.Typ_Unknown }
func (s *DatabaseSymbol) isSymbol()        {}

// AddTable adds a table to the database.
func (s *DatabaseSymbol) AddTable(table *TableSymbol) {
	s.Tables = append(s.Tables, table)
}

// AddFunction adds a function to the database.
func (s *DatabaseSymbol) AddFunction(fn *FunctionSymbol) {
	s.Functions = append(s.Functions, fn)
}

// Table returns the table with the given name.
func (s *DatabaseSymbol) Table(name string) *TableSymbol {
	for _, t := range s.Tables {
		if t.Name() == name {
			return t
		}
	}
	return nil
}

// Function returns the function with the given name.
func (s *DatabaseSymbol) Function(name string) *FunctionSymbol {
	for _, f := range s.Functions {
		if f.Name() == name {
			return f
		}
	}
	return nil
}

// ClusterSymbol represents a Kusto cluster.
type ClusterSymbol struct {
	baseSymbol
	Databases []*DatabaseSymbol
}

func NewCluster(name string) *ClusterSymbol {
	return &ClusterSymbol{
		baseSymbol: baseSymbol{name: name},
	}
}

func (s *ClusterSymbol) Kind() SymbolKind { return SymbolCluster }
func (s *ClusterSymbol) Type() types.Type { return types.Typ_Unknown }
func (s *ClusterSymbol) isSymbol()        {}

// AddDatabase adds a database to the cluster.
func (s *ClusterSymbol) AddDatabase(db *DatabaseSymbol) {
	s.Databases = append(s.Databases, db)
}

// Database returns the database with the given name.
func (s *ClusterSymbol) Database(name string) *DatabaseSymbol {
	for _, db := range s.Databases {
		if db.Name() == name {
			return db
		}
	}
	return nil
}

