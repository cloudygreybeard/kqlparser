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

// Options controls binder behavior.
type Options struct {
	// StrictMode reports errors for unresolved names.
	// When false (permissive), unknown names are allowed (useful with incomplete schema).
	StrictMode bool
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
	opts    *Options
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
	return BindWithOptions(script, globals, file, nil)
}

// BindWithOptions performs semantic analysis with custom options.
func BindWithOptions(script *ast.Script, globals *GlobalState, file *token.File, opts *Options) *Result {
	if globals == nil {
		globals = DefaultGlobals()
	}
	if opts == nil {
		opts = &Options{StrictMode: false}
	}

	b := &Binder{
		globals: globals,
		opts:    opts,
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

// warning adds a warning diagnostic.
func (b *Binder) warning(pos token.Pos, code diagnostic.Code, msg string) {
	var position token.Position
	if b.file != nil {
		position = b.file.Position(pos)
	}
	b.diags = append(b.diags, diagnostic.Diagnostic{
		Pos:      position,
		Severity: diagnostic.SeverityWarning,
		Code:     code,
		Message:  msg,
	})
}

// warningf adds a formatted warning diagnostic.
func (b *Binder) warningf(pos token.Pos, code diagnostic.Code, format string, args ...any) {
	b.warning(pos, code, fmt.Sprintf(format, args...))
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

	// Unresolved identifier
	if b.opts.StrictMode {
		// Determine the most helpful error message
		if b.rowScope != nil {
			// We're in a row context, so this is likely a column reference
			b.errorf(ident.Pos(), diagnostic.CodeUnresolvedColumn,
				"column '%s' not found in current scope", name)
		} else if b.globals.Database != nil {
			// We have a database, so this might be a table reference
			b.errorf(ident.Pos(), diagnostic.CodeUnresolvedTable,
				"table '%s' not found in database '%s'", name, b.globals.Database.Name())
		} else {
			b.errorf(ident.Pos(), diagnostic.CodeUnresolvedName,
				"name '%s' not found", name)
		}
	}

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
	case token.EQL, token.NEQ, token.LSS, token.GTR, token.LEQ, token.GEQ:
		b.checkComparableTypes(expr, leftType, rightType)
		resultType = types.Typ_Bool

	// String operators -> bool
	case token.CONTAINS, token.CONTAINSCS, token.STARTSWITH, token.STARTSWITHCS,
		token.ENDSWITH, token.ENDSWITHCS, token.HAS, token.HASCS,
		token.HASALL, token.HASANY, token.LIKE, token.MATCHESREGEX:
		b.checkStringOperands(expr, leftType, rightType)
		resultType = types.Typ_Bool

	// Logical operators -> bool
	case token.AND, token.OR:
		b.checkBoolOperands(expr, leftType, rightType)
		resultType = types.Typ_Bool

	// Arithmetic operators
	case token.ADD, token.SUB, token.MUL, token.QUO, token.REM:
		resultType = b.resolveArithmeticType(leftType, rightType)
		if resultType == types.Typ_Unknown && b.opts.StrictMode {
			b.errorf(expr.OpPos, diagnostic.CodeInvalidOperand,
				"operator '%s' cannot be applied to %s and %s",
				expr.Op, leftType, rightType)
		}

	// In operator -> bool
	case token.IN:
		resultType = types.Typ_Bool

	default:
		resultType = types.Typ_Unknown
	}

	return b.recordType(expr, resultType)
}

// checkComparableTypes checks that two types can be compared.
func (b *Binder) checkComparableTypes(expr *ast.BinaryExpr, left, right types.Type) {
	if !b.opts.StrictMode {
		return
	}
	// Skip check if either side is unknown or dynamic
	if _, ok := left.(*types.Unknown); ok {
		return
	}
	if _, ok := right.(*types.Unknown); ok {
		return
	}
	if types.IsDynamic(left) || types.IsDynamic(right) {
		return
	}

	if !types.Compatible(left, right) {
		b.errorf(expr.OpPos, diagnostic.CodeTypeMismatch,
			"cannot compare %s with %s", left, right)
	}
}

// checkStringOperands checks that string operators have appropriate operands.
func (b *Binder) checkStringOperands(expr *ast.BinaryExpr, left, right types.Type) {
	if !b.opts.StrictMode {
		return
	}
	// Left side should typically be string or dynamic
	if !types.IsString(left) && !types.IsDynamic(left) {
		if _, ok := left.(*types.Unknown); !ok {
			b.errorf(expr.X.Pos(), diagnostic.CodeInvalidOperand,
				"operator '%s' requires string operand, got %s", expr.Op, left)
		}
	}
}

// checkBoolOperands checks that logical operators have bool operands.
func (b *Binder) checkBoolOperands(expr *ast.BinaryExpr, left, right types.Type) {
	if !b.opts.StrictMode {
		return
	}
	if !types.IsBool(left) && !types.IsDynamic(left) {
		if _, ok := left.(*types.Unknown); !ok {
			b.errorf(expr.X.Pos(), diagnostic.CodeInvalidOperand,
				"operator '%s' requires bool operand, got %s", expr.Op, left)
		}
	}
	if !types.IsBool(right) && !types.IsDynamic(right) {
		if _, ok := right.(*types.Unknown); !ok {
			b.errorf(expr.Y.Pos(), diagnostic.CodeInvalidOperand,
				"operator '%s' requires bool operand, got %s", expr.Op, right)
		}
	}
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
	var argTypes []types.Type
	for _, arg := range expr.Args {
		argTypes = append(argTypes, b.bindExpr(arg))
	}

	// Get function name
	var funcName string
	var funcIdent *ast.Ident
	switch fn := expr.Fun.(type) {
	case *ast.Ident:
		funcName = fn.Name
		funcIdent = fn
	default:
		// Could be a method call or other expression
		b.bindExpr(expr.Fun)
		return b.recordType(expr, types.Typ_Unknown)
	}

	// Look up function
	if fn := b.globals.Functions[funcName]; fn != nil {
		b.recordSymbol(funcIdent, fn)
		b.checkFunctionArgs(expr, fn, argTypes)
		return b.recordType(expr, fn.Type())
	}

	// Look up aggregate
	if agg := b.globals.Aggregates[funcName]; agg != nil {
		b.recordSymbol(funcIdent, agg)
		b.checkFunctionArgs(expr, &agg.FunctionSymbol, argTypes)
		return b.recordType(expr, agg.Type())
	}

	// Check if it's a user-defined function in the database
	if b.globals.Database != nil {
		if fn := b.globals.Database.Function(funcName); fn != nil {
			b.recordSymbol(funcIdent, fn)
			return b.recordType(expr, fn.Type())
		}
	}

	// Unknown function
	if b.opts.StrictMode {
		b.errorf(funcIdent.Pos(), diagnostic.CodeUnresolvedFunction,
			"function '%s' not found", funcName)
	}
	return b.recordType(expr, types.Typ_Unknown)
}

// checkFunctionArgs validates function arguments.
func (b *Binder) checkFunctionArgs(expr *ast.CallExpr, fn *symbol.FunctionSymbol, argTypes []types.Type) {
	if len(fn.Signatures) == 0 {
		return
	}

	// Check first signature (simplified - full impl would check all overloads)
	sig := fn.Signatures[0]
	argCount := len(argTypes)

	// Check argument count
	if argCount < sig.MinArgs {
		b.errorf(expr.Lparen, diagnostic.CodeWrongArgCount,
			"function '%s' requires at least %d argument(s), got %d",
			fn.Name(), sig.MinArgs, argCount)
		return
	}

	if sig.MaxArgs >= 0 && argCount > sig.MaxArgs {
		b.errorf(expr.Lparen, diagnostic.CodeWrongArgCount,
			"function '%s' accepts at most %d argument(s), got %d",
			fn.Name(), sig.MaxArgs, argCount)
		return
	}

	// Check argument types
	for i, argType := range argTypes {
		if i >= len(sig.Parameters) {
			break // Variadic or extra args
		}
		param := sig.Parameters[i]
		if !b.isTypeCompatible(argType, param.Type) {
			b.errorf(expr.Args[i].Pos(), diagnostic.CodeInvalidArgument,
				"argument %d of '%s': expected %s, got %s",
				i+1, fn.Name(), param.Type.String(), argType.String())
		}
	}
}

// isTypeCompatible checks if a value type can be used where an expected type is required.
func (b *Binder) isTypeCompatible(value, expected types.Type) bool {
	// Unknown is compatible with everything (permissive)
	if _, ok := value.(*types.Unknown); ok {
		return true
	}
	if _, ok := expected.(*types.Unknown); ok {
		return true
	}

	// Dynamic accepts anything
	if types.IsDynamic(expected) || types.IsDynamic(value) {
		return true
	}

	// Use the types package compatibility check
	return types.Compatible(value, expected)
}

// bindSelectorExpr binds a selector expression (e.g., table.column).
func (b *Binder) bindSelectorExpr(expr *ast.SelectorExpr) types.Type {
	baseType := b.bindExpr(expr.X)

	// If base is a table, look up column
	if tab, ok := baseType.(*types.Tabular); ok {
		if col := tab.Column(expr.Sel.Name); col != nil {
			return b.recordType(expr, col.Type)
		}
		// Column not found in table
		if b.opts.StrictMode {
			b.errorf(expr.Sel.Pos(), diagnostic.CodeUnresolvedColumn,
				"column '%s' not found in table", expr.Sel.Name)
		}
		return b.recordType(expr, types.Typ_Unknown)
	}

	// Dynamic access always returns dynamic
	if types.IsDynamic(baseType) {
		return b.recordType(expr, types.Typ_Dynamic)
	}

	// Accessing property on non-tabular, non-dynamic type
	if b.opts.StrictMode {
		if _, ok := baseType.(*types.Unknown); !ok {
			b.errorf(expr.Dot, diagnostic.CodeInvalidOperator,
				"cannot access property '%s' on %s", expr.Sel.Name, baseType)
		}
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
