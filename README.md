# kqlparser

[![Go Reference](https://pkg.go.dev/badge/github.com/cloudygreybeard/kqlparser.svg)](https://pkg.go.dev/github.com/cloudygreybeard/kqlparser)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A pure Go parser and semantic analyzer for [Kusto Query Language (KQL)](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/).

## Features

- **Full KQL Parser** — Lexer, parser, and AST for the complete KQL syntax
- **Semantic Analysis** — Name resolution, type inference, and schema flow tracking
- **Rich Diagnostics** — Actionable error messages with position information
- **80+ Built-in Functions** — `strlen`, `ago`, `datetime`, `tolower`, and more
- **30+ Aggregates** — `count`, `sum`, `avg`, `percentile`, `make_list`, etc.
- **33 Operators** — `where`, `project`, `summarize`, `join`, `union`, and more
- **No Dependencies** — Pure Go, no CGO, no external libraries
- **No Network Required** — Parse and analyze KQL completely offline

## What This Is (and Isn't)

| Need to... | Use |
|------------|-----|
| Parse, validate, or analyze KQL locally | **`kqlparser`** (this library) |
| Execute queries against Azure Data Explorer | [`azure-kusto-go`](https://github.com/Azure/azure-kusto-go) |

These libraries are **complementary**: validate locally with `kqlparser`, then execute with the Azure SDK.

## Installation

```bash
go get github.com/cloudygreybeard/kqlparser
```

Requires Go 1.21 or later.

## Quick Start

### Parse KQL

```go
import "github.com/cloudygreybeard/kqlparser"

result := kqlparser.Parse("query.kql", `StormEvents | take 10`)
if result.HasErrors() {
    for _, err := range result.Errors {
        fmt.Println(err)
    }
    return
}
// Use result.AST
```

### Parse with Semantic Analysis

```go
import (
    "github.com/cloudygreybeard/kqlparser"
    "github.com/cloudygreybeard/kqlparser/symbol"
    "github.com/cloudygreybeard/kqlparser/types"
)

// Define your schema
globals := kqlparser.NewGlobals()
globals.Database = symbol.NewDatabase("Samples")
globals.Database.AddTable(symbol.NewTable("StormEvents",
    types.NewColumn("StartTime", types.Typ_DateTime),
    types.NewColumn("State", types.Typ_String),
    types.NewColumn("EventType", types.Typ_String),
    types.NewColumn("DamageProperty", types.Typ_Long),
))

// Parse and analyze
src := `
StormEvents
| where StartTime >= datetime("2007-01-01")
| summarize TotalDamage = sum(DamageProperty) by State
| top 10 by TotalDamage desc
`

result := kqlparser.ParseAndAnalyze("query.kql", src, globals)
if result.HasErrors() {
    for _, err := range result.Errors() {
        fmt.Printf("%s\n", err)
    }
    return
}

// Inspect result type
if tab, ok := result.ResultType.(*types.Tabular); ok {
    for _, col := range tab.Columns {
        fmt.Printf("  %s: %s\n", col.Name, col.Type)
    }
}
// Output:
//   State: string
//   TotalDamage: long
```

### Strict Mode

Enable strict mode to catch all unresolved names:

```go
opts := &kqlparser.Options{StrictMode: true}
result := kqlparser.ParseAndAnalyzeWithOptions("query.kql", src, globals, opts)
```

## Use Cases

- **IDE Integration** — Syntax highlighting, autocomplete, error diagnostics
- **CI/CD Validation** — Validate `.kql` files without cluster access
- **Linting** — Enforce style rules, detect common errors
- **Refactoring Tools** — Rename columns, find usages
- **Code Generation** — Generate typed structs from query schemas
- **Offline Development** — Write and validate KQL anywhere

## API Reference

### Top-Level Functions

| Function | Description |
|----------|-------------|
| `Parse(filename, src)` | Parse KQL to AST |
| `ParseAndAnalyze(filename, src, globals)` | Parse + semantic analysis |
| `ParseAndAnalyzeWithOptions(...)` | With strict mode |
| `MustParse(filename, src)` | Parse, panic on error |
| `NewGlobals()` | Create globals with built-in functions |

### Result Types

**ParseResult**
- `AST` — Parsed syntax tree
- `Errors` — Parse errors
- `HasErrors()` — Check for errors

**AnalyzeResult**
- `AST` — Parsed syntax tree
- `Types` — Map of expressions to types
- `Symbols` — Map of identifiers to symbols
- `ResultType` — Type of query result
- `Diagnostics` — Errors and warnings
- `HasErrors()`, `Errors()`, `Warnings()`

## Package Structure

| Package | Description |
|---------|-------------|
| `kqlparser` | Top-level API |
| `token` | Token types and source positions |
| `lexer` | Lexical scanner |
| `ast` | Abstract Syntax Tree nodes |
| `parser` | Recursive descent parser |
| `types` | Type system (scalar, tabular) |
| `symbol` | Symbols (tables, columns, functions, scopes) |
| `builtin` | Built-in function definitions |
| `binder` | Semantic analyzer |
| `diagnostic` | Error and warning types |

## Supported Operators

| Category | Operators |
|----------|-----------|
| **Filter** | `where`, `search` |
| **Project** | `project`, `project-away`, `project-rename`, `project-reorder`, `extend` |
| **Aggregate** | `summarize`, `count`, `distinct` |
| **Sort/Limit** | `sort`, `order`, `top`, `take`, `limit` |
| **Join** | `join` (inner, leftouter, rightouter, fullouter, leftsemi, rightsemi, leftanti, rightanti), `lookup`, `union` |
| **Transform** | `mv-expand`, `parse`, `serialize`, `scan` |
| **Time Series** | `make-series` |
| **Sampling** | `sample`, `sample-distinct` |
| **Other** | `as`, `getschema`, `invoke`, `evaluate`, `reduce`, `fork`, `facet`, `render`, `consume` |

## Diagnostic Codes

| Code | Description |
|------|-------------|
| `KQL000` | Syntax error |
| `KQL001` | Unresolved name |
| `KQL002` | Unresolved column |
| `KQL003` | Unresolved table |
| `KQL004` | Unresolved function |
| `KQL010` | Type mismatch |
| `KQL011` | Invalid operand |
| `KQL012` | Invalid argument |
| `KQL013` | Wrong argument count |

## Development

```bash
make help          # Show all targets
make check         # Build, lint, test
make test-cover    # Run tests with coverage
make test-bench    # Run benchmarks
```

## References

- [KQL Documentation](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/)
- [Kusto.Language](https://github.com/microsoft/Kusto-Query-Language) — Microsoft's C# parser
- [azure-kusto-go](https://github.com/Azure/azure-kusto-go) — Azure SDK for query execution

## License

Apache 2.0 — see [LICENSE](LICENSE)
