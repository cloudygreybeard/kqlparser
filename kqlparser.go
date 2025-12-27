// Package kqlparser provides a parser and semantic analyzer for
// Kusto Query Language (KQL).
//
// # Quick Start
//
// Parse KQL source into an AST:
//
//	result := kqlparser.Parse("query.kql", `StormEvents | take 10`)
//	if result.HasErrors() {
//	    for _, err := range result.Errors {
//	        fmt.Println(err)
//	    }
//	}
//
// Parse and analyze with semantic checking:
//
//	result := kqlparser.ParseAndAnalyze("query.kql", src, nil)
//	if result.HasErrors() {
//	    // Handle errors
//	}
//	// result.AST contains the parsed tree
//	// result.Types maps expressions to their types
//	// result.Symbols maps identifiers to their definitions
//
// # Architecture
//
// The parser is organized into several packages:
//
//   - token: Token types and source positions
//   - lexer: Tokenizer (source → tokens)
//   - ast: Abstract syntax tree node types
//   - parser: Parser (tokens → AST)
//   - types: KQL type system (scalar, tabular)
//   - symbol: Symbol table (tables, columns, functions)
//   - binder: Semantic analyzer (type checking, name resolution)
//   - diagnostic: Error and warning types
//   - builtin: Built-in function definitions
package kqlparser

import (
	"github.com/cloudygreybeard/kqlparser/ast"
	"github.com/cloudygreybeard/kqlparser/binder"
	"github.com/cloudygreybeard/kqlparser/diagnostic"
	"github.com/cloudygreybeard/kqlparser/parser"
	"github.com/cloudygreybeard/kqlparser/symbol"
	"github.com/cloudygreybeard/kqlparser/token"
	"github.com/cloudygreybeard/kqlparser/types"
)

// ParseResult contains the results of parsing KQL source.
type ParseResult struct {
	// AST is the parsed abstract syntax tree.
	AST *ast.Script

	// Errors contains any parse errors encountered.
	Errors []error

	// File provides position information for error reporting.
	File *token.File
}

// HasErrors returns true if parsing produced errors.
func (r *ParseResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// AnalyzeResult contains the results of semantic analysis.
type AnalyzeResult struct {
	// AST is the parsed abstract syntax tree.
	AST *ast.Script

	// Types maps expressions to their inferred types.
	Types map[ast.Expr]types.Type

	// Symbols maps identifier references to their resolved symbols.
	Symbols map[*ast.Ident]symbol.Symbol

	// ResultType is the type of the overall query result.
	ResultType types.Type

	// Diagnostics contains errors, warnings, and other messages.
	Diagnostics diagnostic.List

	// File provides position information.
	File *token.File
}

// HasErrors returns true if analysis produced errors.
func (r *AnalyzeResult) HasErrors() bool {
	return r.Diagnostics.HasErrors()
}

// Errors returns only the error diagnostics.
func (r *AnalyzeResult) Errors() []diagnostic.Diagnostic {
	var errs []diagnostic.Diagnostic
	for _, d := range r.Diagnostics {
		if d.Severity == diagnostic.SeverityError {
			errs = append(errs, d)
		}
	}
	return errs
}

// Warnings returns only the warning diagnostics.
func (r *AnalyzeResult) Warnings() []diagnostic.Diagnostic {
	warns := []diagnostic.Diagnostic{}
	for _, d := range r.Diagnostics {
		if d.Severity == diagnostic.SeverityWarning {
			warns = append(warns, d)
		}
	}
	return warns
}

// Parse parses KQL source code and returns the AST.
//
// The filename is used for error messages and can be any descriptive string.
// The src parameter contains the KQL source code to parse.
//
// Example:
//
//	result := kqlparser.Parse("inline", `StormEvents | where State == "TEXAS" | take 10`)
//	if result.HasErrors() {
//	    for _, err := range result.Errors {
//	        fmt.Println(err)
//	    }
//	    return
//	}
//	// Use result.AST
func Parse(filename, src string) *ParseResult {
	p := parser.New(filename, src)
	script := p.Parse()

	// Convert parser.ErrorList to []error
	parseErrs := p.Errors()
	var errs []error
	for _, e := range parseErrs {
		errs = append(errs, e)
	}

	return &ParseResult{
		AST:    script,
		Errors: errs,
		File:   p.File(),
	}
}

// ParseAndAnalyze parses KQL source and performs semantic analysis.
//
// This is the recommended entry point for most use cases. It parses the source,
// then performs name resolution, type inference, and semantic validation.
//
// The globals parameter provides schema context (database, tables, functions).
// Pass nil to use only built-in functions without any table schema.
//
// Example:
//
//	// Without schema context (built-ins only)
//	result := kqlparser.ParseAndAnalyze("query.kql", src, nil)
//
//	// With database schema
//	globals := kqlparser.NewGlobals()
//	globals.Database = symbol.NewDatabase("MyDB")
//	globals.Database.AddTable(symbol.NewTable("Events",
//	    types.NewColumn("Timestamp", types.Typ_DateTime),
//	    types.NewColumn("Message", types.Typ_String),
//	))
//	result := kqlparser.ParseAndAnalyze("query.kql", src, globals)
func ParseAndAnalyze(filename, src string, globals *Globals) *AnalyzeResult {
	return ParseAndAnalyzeWithOptions(filename, src, globals, nil)
}

// ParseAndAnalyzeWithOptions parses and analyzes with custom options.
//
// Options control analyzer behavior such as strict mode.
func ParseAndAnalyzeWithOptions(filename, src string, globals *Globals, opts *Options) *AnalyzeResult {
	// Parse
	p := parser.New(filename, src)
	script := p.Parse()

	// Check for parse errors
	parseErrors := p.Errors()
	if len(parseErrors) > 0 {
		// Convert parse errors to diagnostics
		var diags diagnostic.List
		for _, err := range parseErrors {
			diags = append(diags, diagnostic.Diagnostic{
				Severity: diagnostic.SeverityError,
				Code:     diagnostic.CodeSyntaxError,
				Message:  err.Error(),
			})
		}
		return &AnalyzeResult{
			AST:         script,
			Diagnostics: diags,
			File:        p.File(),
		}
	}

	// Convert globals
	var binderGlobals *binder.GlobalState
	if globals != nil {
		binderGlobals = &binder.GlobalState{
			Database:   globals.Database,
			Cluster:    globals.Cluster,
			Functions:  globals.Functions,
			Aggregates: globals.Aggregates,
		}
	}

	// Convert options
	var binderOpts *binder.Options
	if opts != nil {
		binderOpts = &binder.Options{
			StrictMode: opts.StrictMode,
		}
	}

	// Analyze
	bindResult := binder.BindWithOptions(script, binderGlobals, p.File(), binderOpts)

	return &AnalyzeResult{
		AST:         script,
		Types:       bindResult.Types,
		Symbols:     bindResult.Symbols,
		ResultType:  bindResult.ResultType,
		Diagnostics: bindResult.Diagnostics,
		File:        p.File(),
	}
}

// Globals represents the schema context for semantic analysis.
type Globals struct {
	Database   *symbol.DatabaseSymbol
	Cluster    *symbol.ClusterSymbol
	Functions  map[string]*symbol.FunctionSymbol
	Aggregates map[string]*symbol.AggregateSymbol
}

// NewGlobals creates a new Globals with built-in functions loaded.
func NewGlobals() *Globals {
	bg := binder.DefaultGlobals()
	return &Globals{
		Functions:  bg.Functions,
		Aggregates: bg.Aggregates,
	}
}

// Options controls parser and analyzer behavior.
type Options struct {
	// StrictMode reports errors for unresolved names.
	// When false (default), unknown names are allowed.
	StrictMode bool
}

// MustParse parses KQL source and panics on error.
// Useful for tests and examples with known-good input.
func MustParse(filename, src string) *ast.Script {
	result := Parse(filename, src)
	if result.HasErrors() {
		panic(result.Errors[0])
	}
	return result.AST
}

// MustParseAndAnalyze parses and analyzes, panicking on error.
// Useful for tests and examples with known-good input.
func MustParseAndAnalyze(filename, src string, globals *Globals) *AnalyzeResult {
	result := ParseAndAnalyze(filename, src, globals)
	if result.HasErrors() {
		panic(result.Errors()[0].Message)
	}
	return result
}
