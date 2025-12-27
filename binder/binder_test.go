package binder

import (
	"testing"

	"github.com/cloudygreybeard/kqlparser/diagnostic"
	"github.com/cloudygreybeard/kqlparser/parser"
	"github.com/cloudygreybeard/kqlparser/symbol"
	"github.com/cloudygreybeard/kqlparser/types"
)

func TestBindLiterals(t *testing.T) {
	tests := []struct {
		src      string
		wantType types.Type
	}{
		{`123`, types.Typ_Long},
		{`1.5`, types.Typ_Real},
		{`"hello"`, types.Typ_String},
		{`true`, types.Typ_Bool},
		{`1d`, types.Typ_TimeSpan},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			p := parser.New("test", tt.src)
			script := p.Parse()
			if errs := p.Errors(); len(errs) > 0 {
				t.Fatalf("parse errors: %v", errs)
			}

			result := Bind(script, nil, p.File())
			if result.ResultType != tt.wantType {
				t.Errorf("result type: got %v, want %v", result.ResultType, tt.wantType)
			}
		})
	}
}

func TestBindBinaryExpr(t *testing.T) {
	tests := []struct {
		src      string
		wantType types.Type
	}{
		{`1 + 2`, types.Typ_Long},
		{`1.0 + 2`, types.Typ_Real},
		{`1 == 2`, types.Typ_Bool},
		{`"a" contains "b"`, types.Typ_Bool},
		{`true and false`, types.Typ_Bool},
		{`1 > 0 or 2 > 0`, types.Typ_Bool},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			p := parser.New("test", tt.src)
			script := p.Parse()
			if errs := p.Errors(); len(errs) > 0 {
				t.Fatalf("parse errors: %v", errs)
			}

			result := Bind(script, nil, p.File())
			if result.ResultType != tt.wantType {
				t.Errorf("result type: got %v, want %v", result.ResultType, tt.wantType)
			}
		})
	}
}

func TestBindFunctionCall(t *testing.T) {
	tests := []struct {
		src      string
		wantType types.Type
	}{
		{`strlen("hello")`, types.Typ_Long},
		{`toupper("hello")`, types.Typ_String},
		{`now()`, types.Typ_DateTime},
		{`ago(1d)`, types.Typ_DateTime},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			p := parser.New("test", tt.src)
			script := p.Parse()
			if errs := p.Errors(); len(errs) > 0 {
				t.Fatalf("parse errors: %v", errs)
			}

			result := Bind(script, nil, p.File())
			if result.ResultType != tt.wantType {
				t.Errorf("result type: got %v, want %v", result.ResultType, tt.wantType)
			}
		})
	}
}

func TestBindLetStatement(t *testing.T) {
	src := `let x = 42; x + 1`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	result := Bind(script, nil, p.File())
	if result.ResultType != types.Typ_Long {
		t.Errorf("result type: got %v, want long", result.ResultType)
	}
}

func TestBindPipeExpr(t *testing.T) {
	// Create a test schema
	globals := DefaultGlobals()
	globals.Database = symbol.NewDatabase("TestDB")
	globals.Database.AddTable(symbol.NewTable("Events",
		types.NewColumn("Timestamp", types.Typ_DateTime),
		types.NewColumn("Message", types.Typ_String),
		types.NewColumn("Level", types.Typ_Long),
	))

	src := `Events | where Level > 3 | project Timestamp, Message`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	result := Bind(script, globals, p.File())
	if result.Diagnostics.HasErrors() {
		t.Fatalf("bind errors: %v", result.Diagnostics)
	}

	// Result should be tabular with 2 columns
	tab, ok := result.ResultType.(*types.Tabular)
	if !ok {
		t.Fatalf("result type: got %T, want *types.Tabular", result.ResultType)
	}
	if len(tab.Columns) != 2 {
		t.Errorf("column count: got %d, want 2", len(tab.Columns))
	}
}

func TestBindSummarize(t *testing.T) {
	globals := DefaultGlobals()
	globals.Database = symbol.NewDatabase("TestDB")
	globals.Database.AddTable(symbol.NewTable("Events",
		types.NewColumn("Timestamp", types.Typ_DateTime),
		types.NewColumn("Message", types.Typ_String),
		types.NewColumn("Level", types.Typ_Long),
	))

	src := `Events | summarize count() by Level`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	result := Bind(script, globals, p.File())
	if result.Diagnostics.HasErrors() {
		t.Fatalf("bind errors: %v", result.Diagnostics)
	}

	// Result should be tabular with Level and count columns
	tab, ok := result.ResultType.(*types.Tabular)
	if !ok {
		t.Fatalf("result type: got %T, want *types.Tabular", result.ResultType)
	}
	if len(tab.Columns) != 2 {
		t.Errorf("column count: got %d, want 2", len(tab.Columns))
	}
}

func TestBindExtend(t *testing.T) {
	globals := DefaultGlobals()
	globals.Database = symbol.NewDatabase("TestDB")
	globals.Database.AddTable(symbol.NewTable("Events",
		types.NewColumn("Value", types.Typ_Long),
	))

	src := `Events | extend DoubleValue = Value * 2`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	result := Bind(script, globals, p.File())
	if result.Diagnostics.HasErrors() {
		t.Fatalf("bind errors: %v", result.Diagnostics)
	}

	// Result should have Value and DoubleValue columns
	tab, ok := result.ResultType.(*types.Tabular)
	if !ok {
		t.Fatalf("result type: got %T, want *types.Tabular", result.ResultType)
	}
	if len(tab.Columns) != 2 {
		t.Errorf("column count: got %d, want 2", len(tab.Columns))
	}
}

func TestBindCount(t *testing.T) {
	globals := DefaultGlobals()
	globals.Database = symbol.NewDatabase("TestDB")
	globals.Database.AddTable(symbol.NewTable("Events",
		types.NewColumn("Value", types.Typ_Long),
	))

	src := `Events | count`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	result := Bind(script, globals, p.File())
	if result.Diagnostics.HasErrors() {
		t.Fatalf("bind errors: %v", result.Diagnostics)
	}

	// Result should have single Count column
	tab, ok := result.ResultType.(*types.Tabular)
	if !ok {
		t.Fatalf("result type: got %T, want *types.Tabular", result.ResultType)
	}
	if len(tab.Columns) != 1 {
		t.Errorf("column count: got %d, want 1", len(tab.Columns))
	}
	if tab.Columns[0].Name != "Count" {
		t.Errorf("column name: got %s, want Count", tab.Columns[0].Name)
	}
}

func TestSymbolResolution(t *testing.T) {
	globals := DefaultGlobals()
	globals.Database = symbol.NewDatabase("TestDB")
	globals.Database.AddTable(symbol.NewTable("Events",
		types.NewColumn("Value", types.Typ_Long),
	))

	src := `Events | where Value > 10`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	result := Bind(script, globals, p.File())
	
	// Check that symbols were resolved
	if len(result.Symbols) == 0 {
		t.Error("expected symbols to be resolved")
	}
}

// Diagnostic tests

func TestDiagnosticUnresolvedColumn(t *testing.T) {
	globals := DefaultGlobals()
	globals.Database = symbol.NewDatabase("TestDB")
	globals.Database.AddTable(symbol.NewTable("Events",
		types.NewColumn("Value", types.Typ_Long),
	))

	src := `Events | where UnknownColumn > 10`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	opts := &Options{StrictMode: true}
	result := BindWithOptions(script, globals, p.File(), opts)
	
	if !result.Diagnostics.HasErrors() {
		t.Error("expected error for unresolved column")
	}

	// Check the error message
	found := false
	for _, d := range result.Diagnostics {
		if d.Code == diagnostic.CodeUnresolvedColumn {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected CodeUnresolvedColumn diagnostic")
	}
}

func TestDiagnosticUnresolvedTable(t *testing.T) {
	globals := DefaultGlobals()
	globals.Database = symbol.NewDatabase("TestDB")

	src := `UnknownTable | take 10`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	opts := &Options{StrictMode: true}
	result := BindWithOptions(script, globals, p.File(), opts)
	
	if !result.Diagnostics.HasErrors() {
		t.Error("expected error for unresolved table")
	}
}

func TestDiagnosticUnresolvedFunction(t *testing.T) {
	src := `unknownFunc(123)`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	opts := &Options{StrictMode: true}
	result := BindWithOptions(script, nil, p.File(), opts)
	
	if !result.Diagnostics.HasErrors() {
		t.Error("expected error for unresolved function")
	}

	found := false
	for _, d := range result.Diagnostics {
		if d.Code == diagnostic.CodeUnresolvedFunction {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected CodeUnresolvedFunction diagnostic")
	}
}

func TestDiagnosticWrongArgCount(t *testing.T) {
	// strlen requires 1 argument
	src := `strlen()`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	opts := &Options{StrictMode: true}
	result := BindWithOptions(script, nil, p.File(), opts)
	
	if !result.Diagnostics.HasErrors() {
		t.Error("expected error for wrong argument count")
	}

	found := false
	for _, d := range result.Diagnostics {
		if d.Code == diagnostic.CodeWrongArgCount {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected CodeWrongArgCount diagnostic, got: %v", result.Diagnostics)
	}
}

func TestDiagnosticTypeMismatch(t *testing.T) {
	// Comparing string with number
	src := `"hello" == 123`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	opts := &Options{StrictMode: true}
	result := BindWithOptions(script, nil, p.File(), opts)
	
	if !result.Diagnostics.HasErrors() {
		t.Error("expected error for type mismatch")
	}

	found := false
	for _, d := range result.Diagnostics {
		if d.Code == diagnostic.CodeTypeMismatch {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected CodeTypeMismatch diagnostic, got: %v", result.Diagnostics)
	}
}

func TestDiagnosticInvalidOperand(t *testing.T) {
	// Using 'and' with non-boolean operand
	src := `123 and 456`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	opts := &Options{StrictMode: true}
	result := BindWithOptions(script, nil, p.File(), opts)
	
	if !result.Diagnostics.HasErrors() {
		t.Error("expected error for invalid operand")
	}

	found := false
	for _, d := range result.Diagnostics {
		if d.Code == diagnostic.CodeInvalidOperand {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected CodeInvalidOperand diagnostic, got: %v", result.Diagnostics)
	}
}

func TestPermissiveModeNoDiagnostics(t *testing.T) {
	// In permissive mode (default), unknown names don't produce errors
	src := `UnknownTable | where UnknownColumn > 10`
	p := parser.New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}

	// Permissive mode (default)
	result := Bind(script, nil, p.File())
	
	if result.Diagnostics.HasErrors() {
		t.Errorf("expected no errors in permissive mode, got: %v", result.Diagnostics)
	}
}
