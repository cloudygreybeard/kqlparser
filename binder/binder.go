// Package binder implements semantic analysis for KQL.
package binder

import (
	"fmt"

	"github.com/cloudygreybeard/kqlparser/ast"
	"github.com/cloudygreybeard/kqlparser/builtin"
	"github.com/cloudygreybeard/kqlparser/diagnostic"
	"github.com/cloudygreybeard/kqlparser/symbol"
	"github.com/cloudygreybeard/kqlparser/token"
	"github.com/cloudygreybeard/kqlparser/types"
)

// Result contains the results of semantic analysis.
type Result struct {
	// Types maps expressions to their resolved types.
	Types map[ast.Expr]types.Type

	// Symbols maps identifier references to their resolved symbols.
	Symbols map[*ast.Ident]symbol.Symbol

	// ResultType is the type of the overall query result.
	ResultType types.Type

	// Diagnostics contains any errors or warnings.
	Diagnostics diagnostic.List
}

// GlobalState represents the schema context for binding.
type GlobalState struct {
	Database   *symbol.DatabaseSymbol
	Cluster    *symbol.ClusterSymbol
	Functions  map[string]*symbol.FunctionSymbol
	Aggregates map[string]*symbol.AggregateSymbol
}

// DefaultGlobals returns a GlobalState with built-in functions.
func DefaultGlobals() *GlobalState {
	g := &GlobalState{
		Functions:  make(map[string]*symbol.FunctionSymbol),
		Aggregates: make(map[string]*symbol.AggregateSymbol),
	}
	for _, fn := range builtin.ScalarFunctions {
		g.Functions[fn.Name()] = fn
	}
	for _, agg := range builtin.Aggregates {
		g.Aggregates[agg.Name()] = agg
	}
	return g
}

// Binder performs semantic analysis on an AST.
type Binder struct {
	globals *GlobalState
	file    *token.File

	// Results
	types   map[ast.Expr]types.Type
	symbols map[*ast.Ident]symbol.Symbol
	diags   diagnostic.List

	// Current context
	scope    *symbol.Scope    // Lexical scope for variables
	rowScope *symbol.RowScope // Current row context (columns)
}

// Bind performs semantic analysis on a script.
func Bind(script *ast.Script, globals *GlobalState, file *token.File) *Result {
	if globals == nil {
		globals = DefaultGlobals()
	}

	b := &Binder{
		globals: globals,
		file:    file,
		types:   make(map[ast.Expr]types.Type),
		symbols: make(map[*ast.Ident]symbol.Symbol),
		scope:   symbol.NewScope(nil),
	}

	var resultType types.Type
	for _, stmt := range script.Stmts {
		resultType = b.bindStmt(stmt)
	}

	return &Result{
		Types:       b.types,
		Symbols:     b.symbols,
		ResultType:  resultType,
		Diagnostics: b.diags,
	}
}

// error adds an error diagnostic.
func (b *Binder) error(pos token.Pos, code diagnostic.Code, msg string) {
	var position token.Position
	if b.file != nil {
		position = b.file.Position(pos)
	}
	b.diags = append(b.diags, diagnostic.Diagnostic{
		Pos:      position,
		Severity: diagnostic.SeverityError,
		Code:     code,
		Message:  msg,
	})
}

// errorf adds a formatted error diagnostic.
func (b *Binder) errorf(pos token.Pos, code diagnostic.Code, format string, args ...any) {
	b.error(pos, code, fmt.Sprintf(format, args...))
}

// recordType records the type of an expression.
func (b *Binder) recordType(expr ast.Expr, typ types.Type) types.Type {
	b.types[expr] = typ
	return typ
}

// recordSymbol records the resolved symbol for an identifier.
func (b *Binder) recordSymbol(ident *ast.Ident, sym symbol.Symbol) {
	b.symbols[ident] = sym
}

// bindStmt binds a statement.
func (b *Binder) bindStmt(stmt ast.Stmt) types.Type {
	switch s := stmt.(type) {
	case *ast.LetStmt:
		return b.bindLetStmt(s)
	case *ast.ExprStmt:
		return b.bindExpr(s.X)
	default:
		return types.Typ_Unknown
	}
}

// bindLetStmt binds a let statement.
func (b *Binder) bindLetStmt(stmt *ast.LetStmt) types.Type {
	// Bind the value expression
	valueType := b.bindExpr(stmt.Value)

	// Create a variable symbol
	varSym := symbol.NewVariable(stmt.Name.Name, valueType)
	b.scope.Define(varSym)
	b.recordSymbol(stmt.Name, varSym)

	return valueType
}

// bindExpr binds an expression and returns its type.
func (b *Binder) bindExpr(expr ast.Expr) types.Type {
	if expr == nil {
		return types.Typ_Unknown
	}

	switch e := expr.(type) {
	case *ast.Ident:
		return b.bindIdent(e)
	case *ast.BasicLit:
		return b.bindLiteral(e)
	case *ast.BinaryExpr:
		return b.bindBinaryExpr(e)
	case *ast.UnaryExpr:
		return b.bindUnaryExpr(e)
	case *ast.CallExpr:
		return b.bindCallExpr(e)
	case *ast.SelectorExpr:
		return b.bindSelectorExpr(e)
	case *ast.IndexExpr:
		return b.bindIndexExpr(e)
	case *ast.ParenExpr:
		return b.bindExpr(e.X)
	case *ast.PipeExpr:
		return b.bindPipeExpr(e)
	case *ast.NamedExpr:
		return b.bindNamedExpr(e)
	case *ast.BetweenExpr:
		return b.bindBetweenExpr(e)
	case *ast.StarExpr:
		return types.Typ_Unknown // Star is context-dependent
	case *ast.ListExpr:
		return b.bindListExpr(e)
	case *ast.DynamicLit:
		return b.recordType(e, types.Typ_Dynamic)
	case *ast.BadExpr:
		return types.Typ_Unknown
	default:
		return types.Typ_Unknown
	}
}

// bindIdent binds an identifier reference.
func (b *Binder) bindIdent(ident *ast.Ident) types.Type {
	name := ident.Name

	// Handle boolean literals
	if name == "true" || name == "false" {
		return b.recordType(ident, types.Typ_Bool)
	}

	// Handle null literal
	if name == "null" {
		return b.recordType(ident, types.Typ_Dynamic)
	}

	// Check lexical scope (variables from let)
	if sym := b.scope.Resolve(name); sym != nil {
		b.recordSymbol(ident, sym)
		return b.recordType(ident, sym.Type())
	}

	// Check row scope (columns)
	if b.rowScope != nil {
		if col := b.rowScope.Column(name); col != nil {
			b.recordSymbol(ident, col)
			return b.recordType(ident, col.Type())
		}
	}

	// Check tables in database
	if b.globals.Database != nil {
		if tbl := b.globals.Database.Table(name); tbl != nil {
			b.recordSymbol(ident, tbl)
			return b.recordType(ident, tbl.Type())
		}
	}

	// Unresolved - could be a column we don't know about
	// In permissive mode, we'd return Unknown; in strict mode, error
	return b.recordType(ident, types.Typ_Unknown)
}

// bindLiteral binds a literal value.
func (b *Binder) bindLiteral(lit *ast.BasicLit) types.Type {
	var typ types.Type
	switch lit.Kind {
	case token.INT:
		typ = types.Typ_Long
	case token.REAL:
		typ = types.Typ_Real
	case token.STRING:
		typ = types.Typ_String
	case token.BOOL:
		typ = types.Typ_Bool
	case token.DATETIME:
		typ = types.Typ_DateTime
	case token.TIMESPAN:
		typ = types.Typ_TimeSpan
	case token.GUID:
		typ = types.Typ_Guid
	case token.DYNAMIC:
		typ = types.Typ_Dynamic
	default:
		typ = types.Typ_Unknown
	}
	return b.recordType(lit, typ)
}

// bindBinaryExpr binds a binary expression.
func (b *Binder) bindBinaryExpr(expr *ast.BinaryExpr) types.Type {
	leftType := b.bindExpr(expr.X)
	rightType := b.bindExpr(expr.Y)

	var resultType types.Type

	switch expr.Op {
	// Comparison operators -> bool
	case token.EQL, token.NEQ, token.LSS, token.GTR, token.LEQ, token.GEQ,
		token.CONTAINS, token.CONTAINSCS, token.STARTSWITH, token.STARTSWITHCS,
		token.ENDSWITH, token.ENDSWITHCS, token.HAS, token.HASCS,
		token.HASALL, token.HASANY, token.LIKE, token.MATCHESREGEX:
		resultType = types.Typ_Bool

	// Logical operators -> bool
	case token.AND, token.OR:
		resultType = types.Typ_Bool

	// Arithmetic operators
	case token.ADD, token.SUB, token.MUL, token.QUO, token.REM:
		resultType = b.resolveArithmeticType(leftType, rightType)

	// In operator -> bool
	case token.IN:
		resultType = types.Typ_Bool

	default:
		resultType = types.Typ_Unknown
	}

	return b.recordType(expr, resultType)
}

// resolveArithmeticType determines the result type of an arithmetic operation.
func (b *Binder) resolveArithmeticType(left, right types.Type) types.Type {
	// If either is unknown, result is unknown
	if _, ok := left.(*types.Unknown); ok {
		return types.Typ_Unknown
	}
	if _, ok := right.(*types.Unknown); ok {
		return types.Typ_Unknown
	}

	leftScalar, leftOk := left.(*types.Scalar)
	rightScalar, rightOk := right.(*types.Scalar)

	if !leftOk || !rightOk {
		return types.Typ_Unknown
	}

	// DateTime +/- TimeSpan = DateTime
	if leftScalar.Kind() == types.DateTime && rightScalar.Kind() == types.TimeSpan {
		return types.Typ_DateTime
	}
	if leftScalar.Kind() == types.TimeSpan && rightScalar.Kind() == types.DateTime {
		return types.Typ_DateTime
	}

	// DateTime - DateTime = TimeSpan
	if leftScalar.Kind() == types.DateTime && rightScalar.Kind() == types.DateTime {
		return types.Typ_TimeSpan
	}

	// Numeric promotion
	return types.CommonType(left, right)
}

// bindUnaryExpr binds a unary expression.
func (b *Binder) bindUnaryExpr(expr *ast.UnaryExpr) types.Type {
	operandType := b.bindExpr(expr.X)

	switch expr.Op {
	case token.NOT:
		return b.recordType(expr, types.Typ_Bool)
	case token.ADD, token.SUB:
		return b.recordType(expr, operandType)
	default:
		return b.recordType(expr, types.Typ_Unknown)
	}
}

// bindCallExpr binds a function call.
func (b *Binder) bindCallExpr(expr *ast.CallExpr) types.Type {
	// Bind arguments first
	for _, arg := range expr.Args {
		b.bindExpr(arg)
	}

	// Get function name
	var funcName string
	switch fn := expr.Fun.(type) {
	case *ast.Ident:
		funcName = fn.Name
	default:
		// Could be a method call or other expression
		b.bindExpr(expr.Fun)
		return b.recordType(expr, types.Typ_Unknown)
	}

	// Look up function
	if fn := b.globals.Functions[funcName]; fn != nil {
		b.recordSymbol(expr.Fun.(*ast.Ident), fn)
		return b.recordType(expr, fn.Type())
	}

	// Look up aggregate
	if agg := b.globals.Aggregates[funcName]; agg != nil {
		b.recordSymbol(expr.Fun.(*ast.Ident), agg)
		return b.recordType(expr, agg.Type())
	}

	// Unknown function - don't error, might be user-defined
	return b.recordType(expr, types.Typ_Unknown)
}

// bindSelectorExpr binds a selector expression (e.g., table.column).
func (b *Binder) bindSelectorExpr(expr *ast.SelectorExpr) types.Type {
	baseType := b.bindExpr(expr.X)

	// If base is a table, look up column
	if tab, ok := baseType.(*types.Tabular); ok {
		if col := tab.Column(expr.Sel.Name); col != nil {
			return b.recordType(expr, col.Type)
		}
	}

	// Dynamic access always returns dynamic
	if baseType == types.Typ_Dynamic {
		return b.recordType(expr, types.Typ_Dynamic)
	}

	return b.recordType(expr, types.Typ_Unknown)
}

// bindIndexExpr binds an index expression (e.g., arr[0]).
func (b *Binder) bindIndexExpr(expr *ast.IndexExpr) types.Type {
	baseType := b.bindExpr(expr.X)
	b.bindExpr(expr.Index)

	// Array/dynamic indexing returns dynamic
	if baseType == types.Typ_Dynamic {
		return b.recordType(expr, types.Typ_Dynamic)
	}

	return b.recordType(expr, types.Typ_Unknown)
}

// bindPipeExpr binds a pipe expression (query with operators).
func (b *Binder) bindPipeExpr(expr *ast.PipeExpr) types.Type {
	// Bind the source
	sourceType := b.bindExpr(expr.Source)

	// Set up row scope from source if it's tabular
	if tab, ok := sourceType.(*types.Tabular); ok {
		b.rowScope = symbol.RowScopeFromTabular(
			symbol.NewTableWithSchema("", tab),
			nil,
		)
	}

	// Bind each operator, updating row scope as we go
	var resultType types.Type = sourceType
	for _, op := range expr.Operators {
		resultType = b.bindOperator(op, resultType)
	}

	return b.recordType(expr, resultType)
}

// bindNamedExpr binds a named expression (name = value).
func (b *Binder) bindNamedExpr(expr *ast.NamedExpr) types.Type {
	return b.bindExpr(expr.Expr)
}

// bindBetweenExpr binds a between expression.
func (b *Binder) bindBetweenExpr(expr *ast.BetweenExpr) types.Type {
	b.bindExpr(expr.X)
	b.bindExpr(expr.Low)
	b.bindExpr(expr.High)
	return b.recordType(expr, types.Typ_Bool)
}

// bindListExpr binds a list expression.
func (b *Binder) bindListExpr(expr *ast.ListExpr) types.Type {
	for _, elem := range expr.Elems {
		b.bindExpr(elem)
	}
	return b.recordType(expr, types.Typ_Dynamic)
}
