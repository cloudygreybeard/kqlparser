# kqlparser

A pure Go implementation of a Kusto Query Language (KQL) parser and semantic analyzer.

> ⚠️ **Work in Progress**: This library is under active development and not yet ready for production use.

## Features

- **Lexer**: Tokenizes KQL source into a stream of tokens
- **Parser**: Builds an Abstract Syntax Tree (AST) from tokens
- **Semantic Analyzer**: Resolves names, checks types, tracks schema flow
- **Diagnostics**: Rich, actionable error messages

## Installation

```bash
go get github.com/cloudygreybeard/kqlparser
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/cloudygreybeard/kqlparser"
)

func main() {
    query := `
        StormEvents
        | where StartTime > ago(7d)
        | summarize count() by State
        | top 10 by count_
    `
    
    result := kqlparser.Parse(query)
    if len(result.Diagnostics) > 0 {
        for _, d := range result.Diagnostics {
            fmt.Println(d)
        }
    }
    
    // Walk the AST
    // ...
}
```

## Package Structure

| Package | Description |
|---------|-------------|
| `token` | Token types and source positions |
| `lexer` | Lexical scanner |
| `ast` | Abstract Syntax Tree node definitions |
| `parser` | Recursive descent parser |
| `types` | KQL type system |
| `symbol` | Symbol definitions (tables, columns, functions) |
| `binder` | Semantic analysis |
| `diagnostic` | Error and warning reporting |
| `builtin` | Built-in functions and operators |

## Development Status

### Phase 1: Foundation ✅
- [x] Token types and positions
- [x] Lexer with all KQL tokens
- [ ] Basic AST node types
- [ ] Expression parser

### Phase 2: Query Operators
- [ ] Pipe expression parsing
- [ ] Common operators (where, project, extend, summarize, etc.)
- [ ] Join and union operators

### Phase 3: Type System
- [ ] Scalar types
- [ ] Tabular types
- [ ] Type coercion

### Phase 4: Symbol System
- [ ] Symbol hierarchy
- [ ] Built-in function definitions
- [ ] Scope management

### Phase 5: Semantic Analysis
- [ ] Name resolution
- [ ] Type checking
- [ ] Schema flow
- [ ] Rich diagnostics

## References

- [KQL Documentation](https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/)
- [Microsoft Kusto-Query-Language](https://github.com/microsoft/Kusto-Query-Language) (C# reference implementation)

## License

Apache 2.0 - see [LICENSE](LICENSE)

