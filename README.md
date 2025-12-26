# kqlparser

A pure Go implementation of a Kusto Query Language (KQL) parser and semantic analyzer.

> ⚠️ **Work in Progress**: This library is under active development and not yet ready for production use.

## What This Is (and Isn't)

**This is a parser library** — it analyzes KQL syntax and semantics locally, without any network connection.

**This is not a query client** — if you need to *execute* KQL queries against Azure Data Explorer, use the official [Azure SDK for Go](https://github.com/Azure/azure-kusto-go).

| Need to... | Use |
|------------|-----|
| Execute queries against ADX clusters | [`azure-kusto-go`](https://github.com/Azure/azure-kusto-go) |
| Parse, validate, or analyze KQL locally | `kqlparser` (this library) |

These libraries are **complementary**: validate locally with `kqlparser`, then execute with the Azure SDK.

## Use Cases

- **IDE integration** — Syntax highlighting, autocomplete, error squiggles without cluster connection
- **CI/CD validation** — Validate `.kql` files in pipelines without cluster access
- **Linting & formatting** — Enforce style guides, auto-format queries
- **Refactoring** — Rename columns, find usages, extract variables
- **Code generation** — Generate typed Go structs from query result schemas
- **Offline development** — Write and validate KQL on a plane

## Features

- **Lexer**: Tokenizes KQL source into a stream of tokens
- **Parser**: Builds an Abstract Syntax Tree (AST) from tokens
- **Type System**: Scalar and tabular types with coercion rules
- **Symbol System**: Tables, columns, functions, scopes
- **Semantic Analyzer**: Resolves names, checks types, tracks schema flow *(in progress)*
- **Diagnostics**: Rich, actionable error messages *(in progress)*

## Installation

```bash
go get github.com/cloudygreybeard/kqlparser
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/cloudygreybeard/kqlparser/parser"
)

func main() {
    query := `
        StormEvents
        | where StartTime > ago(7d)
        | summarize count() by State
        | top 10 by count_
    `
    
    p := parser.New("query.kql", query)
    script := p.Parse()
    
    if errs := p.Errors(); len(errs) > 0 {
        for _, e := range errs {
            fmt.Println(e)
        }
        return
    }
    
    // script.Stmts contains the parsed AST
    fmt.Printf("Parsed %d statements\n", len(script.Stmts))
}
```

## Package Structure

| Package | Description |
|---------|-------------|
| `token` | Token types and source positions |
| `lexer` | Lexical scanner |
| `ast` | Abstract Syntax Tree node definitions |
| `parser` | Recursive descent parser |
| `types` | KQL type system (scalar, tabular) |
| `symbol` | Symbol definitions (tables, columns, functions) |
| `builtin` | Built-in functions and aggregates (~100 defined) |
| `binder` | Semantic analysis *(planned)* |
| `diagnostic` | Error and warning reporting *(planned)* |

## Development Status

### ✅ Completed
- Token types and positions
- Lexer with all KQL tokens
- AST node types (expressions, operators, statements)
- Recursive descent parser
- Type system (scalar and tabular types)
- Symbol system (tables, columns, functions, scopes)
- Built-in function definitions

### 🚧 In Progress
- Semantic analyzer (binder)
- Name resolution
- Type checking
- Schema flow through operators

### 📋 Planned
- Rich diagnostics with error codes
- Incremental parsing for IDE use
- Query formatting/pretty-printing

## References

- [KQL Documentation](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/)
- [Kusto.Language](https://github.com/microsoft/Kusto-Query-Language) — Microsoft's C# parser (our reference implementation)
- [azure-kusto-go](https://github.com/Azure/azure-kusto-go) — Azure SDK for executing queries (complementary, not competing)

## License

Apache 2.0 — see [LICENSE](LICENSE)
