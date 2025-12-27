package kqlparser

import (
	"testing"

	"github.com/cloudygreybeard/kqlparser/symbol"
	"github.com/cloudygreybeard/kqlparser/types"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		wantErr bool
	}{
		{
			name:    "simple query",
			src:     `StormEvents | take 10`,
			wantErr: false,
		},
		{
			name:    "filter and project",
			src:     `T | where x > 10 | project x, y`,
			wantErr: false,
		},
		{
			name:    "summarize",
			src:     `T | summarize count() by State`,
			wantErr: false,
		},
		{
			name:    "let statement",
			src:     `let x = 10; T | where value > x`,
			wantErr: false,
		},
		{
			name:    "syntax error - unbalanced parens",
			src:     `T | where (x > 10`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Parse("test", tt.src)
			if tt.wantErr && !result.HasErrors() {
				t.Error("expected error, got none")
			}
			if !tt.wantErr && result.HasErrors() {
				t.Errorf("unexpected error: %v", result.Errors)
			}
			if !tt.wantErr && result.AST == nil {
				t.Error("expected AST, got nil")
			}
		})
	}
}

func TestParseAndAnalyze(t *testing.T) {
	// Create a database with a table
	globals := NewGlobals()
	globals.Database = symbol.NewDatabase("TestDB")
	globals.Database.AddTable(symbol.NewTable("Events",
		types.NewColumn("Timestamp", types.Typ_DateTime),
		types.NewColumn("Message", types.Typ_String),
		types.NewColumn("Level", types.Typ_Long),
	))

	tests := []struct {
		name     string
		src      string
		wantErr  bool
		wantType string // expected result type kind
	}{
		{
			name:     "simple take",
			src:      `Events | take 10`,
			wantErr:  false,
			wantType: "tabular",
		},
		{
			name:     "filter",
			src:      `Events | where Level > 3`,
			wantErr:  false,
			wantType: "tabular",
		},
		{
			name:     "summarize",
			src:      `Events | summarize count() by Level`,
			wantErr:  false,
			wantType: "tabular",
		},
		{
			name:     "project",
			src:      `Events | project Message, Level`,
			wantErr:  false,
			wantType: "tabular",
		},
		{
			name:     "extend",
			src:      `Events | extend DoubleLevel = Level * 2`,
			wantErr:  false,
			wantType: "tabular",
		},
		{
			name:     "count",
			src:      `Events | count`,
			wantErr:  false,
			wantType: "tabular",
		},
		{
			name:    "syntax error - unbalanced parens",
			src:     `Events | where (Level > 3`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseAndAnalyze("test", tt.src, globals)
			if tt.wantErr && !result.HasErrors() {
				t.Error("expected error, got none")
			}
			if !tt.wantErr && result.HasErrors() {
				t.Errorf("unexpected errors: %v", result.Errors())
			}
			if !tt.wantErr && result.AST == nil {
				t.Error("expected AST, got nil")
			}
			if !tt.wantErr && tt.wantType == "tabular" {
				if _, ok := result.ResultType.(*types.Tabular); !ok {
					t.Errorf("expected tabular result, got %T", result.ResultType)
				}
			}
		})
	}
}

func TestParseAndAnalyzeStrict(t *testing.T) {
	globals := NewGlobals()
	globals.Database = symbol.NewDatabase("TestDB")
	globals.Database.AddTable(symbol.NewTable("Events",
		types.NewColumn("Level", types.Typ_Long),
	))

	opts := &Options{StrictMode: true}

	// Unknown column should error in strict mode
	result := ParseAndAnalyzeWithOptions("test", `Events | where Unknown > 10`, globals, opts)
	if !result.HasErrors() {
		t.Error("expected error for unknown column in strict mode")
	}

	// Known column should work
	result = ParseAndAnalyzeWithOptions("test", `Events | where Level > 10`, globals, opts)
	if result.HasErrors() {
		t.Errorf("unexpected error: %v", result.Errors())
	}
}

func TestParseAndAnalyzeWithoutGlobals(t *testing.T) {
	// Should work with just built-in functions
	result := ParseAndAnalyze("test", `print strlen("hello")`, nil)
	if result.HasErrors() {
		t.Errorf("unexpected error: %v", result.Errors())
	}
}

func TestNewGlobals(t *testing.T) {
	globals := NewGlobals()

	// Should have built-in functions
	if globals.Functions == nil {
		t.Error("expected Functions map")
	}
	if len(globals.Functions) == 0 {
		t.Error("expected built-in functions")
	}

	// Check for a known function
	if _, ok := globals.Functions["strlen"]; !ok {
		t.Error("expected strlen function")
	}

	// Should have built-in aggregates
	if globals.Aggregates == nil {
		t.Error("expected Aggregates map")
	}
	if _, ok := globals.Aggregates["count"]; !ok {
		t.Error("expected count aggregate")
	}
}

func TestMustParse(t *testing.T) {
	// Should not panic with valid input
	script := MustParse("test", `T | take 10`)
	if script == nil {
		t.Error("expected script")
	}

	// Should panic with invalid input
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid input")
		}
	}()
	MustParse("test", `||| invalid`)
}

func TestAnalyzeResultMethods(t *testing.T) {
	result := ParseAndAnalyze("test", `T | take 10`, nil)

	// HasErrors should return false for valid query
	if result.HasErrors() {
		t.Errorf("unexpected errors: %v", result.Errors())
	}

	// Errors() should return empty slice
	if len(result.Errors()) != 0 {
		t.Error("expected no errors")
	}

	// Warnings() should return empty slice (no warnings for this query)
	if result.Warnings() == nil {
		t.Error("Warnings() should not return nil")
	}
}

func TestComplexQuery(t *testing.T) {
	globals := NewGlobals()
	globals.Database = symbol.NewDatabase("Samples")
	globals.Database.AddTable(symbol.NewTable("StormEvents",
		types.NewColumn("StartTime", types.Typ_DateTime),
		types.NewColumn("State", types.Typ_String),
		types.NewColumn("EventType", types.Typ_String),
		types.NewColumn("DamageProperty", types.Typ_Long),
	))

	// Use string literals for datetime() to match function signature
	src := `
StormEvents
| where StartTime >= datetime("2007-01-01") and StartTime < datetime("2008-01-01")
| summarize TotalDamage = sum(DamageProperty), EventCount = count() by State
| top 10 by TotalDamage desc
`

	result := ParseAndAnalyze("complex.kql", src, globals)
	if result.HasErrors() {
		t.Errorf("unexpected errors: %v", result.Errors())
	}

	// Check result has expected columns
	tab, ok := result.ResultType.(*types.Tabular)
	if !ok {
		t.Fatalf("expected tabular result, got %T", result.ResultType)
	}

	// Should have State, TotalDamage, EventCount
	if tab.Column("State") == nil {
		t.Error("expected State column")
	}
	if tab.Column("TotalDamage") == nil {
		t.Error("expected TotalDamage column")
	}
	if tab.Column("EventCount") == nil {
		t.Error("expected EventCount column")
	}
}
