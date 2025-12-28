# Grammar-Derived Test Cases

Test cases extracted from Microsoft's official ANTLR grammar files in the
[Kusto-Query-Language](https://github.com/microsoft/Kusto-Query-Language) repository.

## Source Files

- `Kql.g4` - Query grammar (syntax rules)
- `KqlTokens.g4` - Lexical tokens

## Test Files

| File | Description |
|------|-------------|
| `literals.kql` | Literal expressions (strings, numbers, datetime, etc.) |
| `statements.kql` | Statement types (let, set, queries) |
| `operators.kql` | Query operators (where, project, join, etc.) |
| `expressions.kql` | Expression syntax (logical, arithmetic, path) |

## Format

Each file contains one KQL query per line, separated by blank lines.
Lines starting with `//` are comments and are ignored.
Unsupported syntax is commented out with `// TODO:` notes.

## Test Runner

The `grammar_test.go` file in `parser/` runs all `.kql` files against the parser.

```go
go test ./parser -run TestGrammarDerivedCases
```

## Coverage

These tests validate syntax support against the official grammar.
See `KQLPARSER_GRAMMAR_GAPS.md` in the workspace root for a detailed
gap analysis.
