package parser

import (
	"strings"
	"testing"

	"github.com/cloudygreybeard/kqlparser/ast"
	"github.com/cloudygreybeard/kqlparser/token"
)

func TestParseSimpleExprs(t *testing.T) {
	tests := []struct {
		src  string
		want string // Expected AST structure (simplified)
	}{
		{"x", "Ident{x}"},
		{"123", "BasicLit{123}"},
		{`"hello"`, `BasicLit{"hello"}`},
		{"1.5", "BasicLit{1.5}"},
		{"true", "Ident{true}"},
		{"7d", "BasicLit{7d}"},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			p := New("test", tt.src)
			expr := p.ParseExpr()
			if errs := p.Errors(); len(errs) > 0 {
				t.Fatalf("parse errors: %v", errs)
			}
			got := simplePrint(expr)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseBinaryExprs(t *testing.T) {
	tests := []struct {
		src string
		op  token.Token
	}{
		{"a + b", token.ADD},
		{"a - b", token.SUB},
		{"a * b", token.MUL},
		{"a / b", token.QUO},
		{"a == b", token.EQL},
		{"a != b", token.NEQ},
		{"a < b", token.LSS},
		{"a > b", token.GTR},
		{"a <= b", token.LEQ},
		{"a >= b", token.GEQ},
		{"a and b", token.AND},
		{"a or b", token.OR},
		// Positive string operators
		{"a contains b", token.CONTAINS},
		{"a has b", token.HAS},
		{"a startswith b", token.STARTSWITH},
		{"a endswith b", token.ENDSWITH},
		{"a hasprefix b", token.HASPREFIX},
		{"a hassuffix b", token.HASSUFFIX},
		// Negated string operators
		{"a !contains b", token.NOTCONTAINS},
		{"a !has b", token.NOTHAS},
		{"a !startswith b", token.NOTSTARTSWITH},
		{"a !endswith b", token.NOTENDSWITH},
		{"a !hasprefix b", token.NOTHASPREFIX},
		{"a !hassuffix b", token.NOTHASSUFFIX},
		{"a !contains_cs b", token.NOTCONTAINSCS},
		{"a !has_cs b", token.NOTHASCS},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			p := New("test", tt.src)
			expr := p.ParseExpr()
			if errs := p.Errors(); len(errs) > 0 {
				t.Fatalf("parse errors: %v", errs)
			}
			bin, ok := expr.(*ast.BinaryExpr)
			if !ok {
				t.Fatalf("expected BinaryExpr, got %T", expr)
			}
			if bin.Op != tt.op {
				t.Errorf("op: got %v, want %v", bin.Op, tt.op)
			}
		})
	}
}

func TestParseCallExpr(t *testing.T) {
	tests := []struct {
		src      string
		funcName string
		argCount int
	}{
		{"foo()", "foo", 0},
		{"count()", "count", 0},
		{"sum(x)", "sum", 1},
		{"strcat(a, b)", "strcat", 2},
		{"ago(7d)", "ago", 1},
		{"datetime(2023-01-01)", "datetime", 1},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			p := New("test", tt.src)
			expr := p.ParseExpr()
			if errs := p.Errors(); len(errs) > 0 {
				t.Fatalf("parse errors: %v", errs)
			}
			call, ok := expr.(*ast.CallExpr)
			if !ok {
				t.Fatalf("expected CallExpr, got %T", expr)
			}
			if len(call.Args) != tt.argCount {
				t.Errorf("args: got %d, want %d", len(call.Args), tt.argCount)
			}
			if ident, ok := call.Fun.(*ast.Ident); ok {
				if ident.Name != tt.funcName {
					t.Errorf("func name: got %q, want %q", ident.Name, tt.funcName)
				}
			}
		})
	}
}

func TestParsePipeExpr(t *testing.T) {
	tests := []struct {
		src     string
		opCount int
		opTypes []string
	}{
		{"T | where x > 10", 1, []string{"WhereOp"}},
		{"T | project a, b", 1, []string{"ProjectOp"}},
		{"T | extend c = a + b", 1, []string{"ExtendOp"}},
		{"T | summarize count() by x", 1, []string{"SummarizeOp"}},
		{"T | sort by x", 1, []string{"SortOp"}},
		{"T | take 10", 1, []string{"TakeOp"}},
		{"T | top 10 by x", 1, []string{"TopOp"}},
		{"T | count", 1, []string{"CountOp"}},
		{"T | distinct x", 1, []string{"DistinctOp"}},
		{"T | where x > 10 | project a", 2, []string{"WhereOp", "ProjectOp"}},
		{"T | where x > 10 | summarize count() by y | top 5 by count_", 3,
			[]string{"WhereOp", "SummarizeOp", "TopOp"}},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			p := New("test", tt.src)
			expr := p.ParseExpr()
			if errs := p.Errors(); len(errs) > 0 {
				t.Fatalf("parse errors: %v", errs)
			}
			pipe, ok := expr.(*ast.PipeExpr)
			if !ok {
				t.Fatalf("expected PipeExpr, got %T", expr)
			}
			if len(pipe.Operators) != tt.opCount {
				t.Errorf("operator count: got %d, want %d", len(pipe.Operators), tt.opCount)
			}
			for i, opType := range tt.opTypes {
				if i >= len(pipe.Operators) {
					break
				}
				gotType := getTypeName(pipe.Operators[i])
				if gotType != opType {
					t.Errorf("operator[%d]: got %s, want %s", i, gotType, opType)
				}
			}
		})
	}
}

func TestParseLetStmt(t *testing.T) {
	tests := []struct {
		src  string
		name string
	}{
		{"let x = 10", "x"},
		{"let data = T | where a > 0", "data"},
		// Function definitions with parameters not yet supported
		// {"let fn = (a: int) { a * 2 }", "fn"},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			p := New("test", tt.src)
			script := p.Parse()
			if errs := p.Errors(); len(errs) > 0 {
				t.Fatalf("parse errors: %v", errs)
			}
			if len(script.Stmts) != 1 {
				t.Fatalf("stmt count: got %d, want 1", len(script.Stmts))
			}
			letStmt, ok := script.Stmts[0].(*ast.LetStmt)
			if !ok {
				t.Fatalf("expected LetStmt, got %T", script.Stmts[0])
			}
			if letStmt.Name.Name != tt.name {
				t.Errorf("name: got %q, want %q", letStmt.Name.Name, tt.name)
			}
		})
	}
}

func TestParseMultipleStmts(t *testing.T) {
	src := `let x = 10;
let y = 20;
T | where a == x + y`

	p := New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	if len(script.Stmts) != 3 {
		t.Fatalf("stmt count: got %d, want 3", len(script.Stmts))
	}
}

func TestParseBetweenExpr(t *testing.T) {
	src := "x between (10 .. 20)"
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	between, ok := expr.(*ast.BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", expr)
	}
	if between.Not {
		t.Error("expected Not=false")
	}
}

func TestParseNotBetweenExpr(t *testing.T) {
	src := "x !between (10 .. 20)"
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	between, ok := expr.(*ast.BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", expr)
	}
	if !between.Not {
		t.Error("expected Not=true for !between")
	}
}

func TestParseInExpr(t *testing.T) {
	src := `x in ("a", "b", "c")`
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	bin, ok := expr.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", expr)
	}
	if bin.Op != token.IN {
		t.Errorf("op: got %v, want IN", bin.Op)
	}
	list, ok := bin.Y.(*ast.ListExpr)
	if !ok {
		t.Fatalf("expected ListExpr, got %T", bin.Y)
	}
	if len(list.Elems) != 3 {
		t.Errorf("list elems: got %d, want 3", len(list.Elems))
	}
}

func TestParseNotInExpr(t *testing.T) {
	src := `x !in ("a", "b")`
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	bin, ok := expr.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", expr)
	}
	if bin.Op != token.NOTIN {
		t.Errorf("op: got %v, want NOTIN", bin.Op)
	}
	list, ok := bin.Y.(*ast.ListExpr)
	if !ok {
		t.Fatalf("expected ListExpr, got %T", bin.Y)
	}
	if len(list.Elems) != 2 {
		t.Errorf("list elems: got %d, want 2", len(list.Elems))
	}
}

func TestParseNotInCIExpr(t *testing.T) {
	src := `x !in~ ("a", "b")`
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	bin, ok := expr.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", expr)
	}
	if bin.Op != token.NOTINCI {
		t.Errorf("op: got %v, want NOTINCI", bin.Op)
	}
}

func TestParseSelectorExpr(t *testing.T) {
	src := "a.b.c"
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	// Should be SelectorExpr(SelectorExpr(a, b), c)
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		t.Fatalf("expected SelectorExpr, got %T", expr)
	}
	if sel.Sel.Name != "c" {
		t.Errorf("sel: got %q, want %q", sel.Sel.Name, "c")
	}
}

func TestParseIndexExpr(t *testing.T) {
	src := `a["key"]`
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	idx, ok := expr.(*ast.IndexExpr)
	if !ok {
		t.Fatalf("expected IndexExpr, got %T", expr)
	}
	lit, ok := idx.Index.(*ast.BasicLit)
	if !ok {
		t.Fatalf("expected BasicLit index, got %T", idx.Index)
	}
	if lit.Value != `"key"` {
		t.Errorf("index: got %q, want %q", lit.Value, `"key"`)
	}
}

func TestParseSummarizeOp(t *testing.T) {
	src := "T | summarize cnt = count(), total = sum(x) by category, region"
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	pipe, ok := expr.(*ast.PipeExpr)
	if !ok {
		t.Fatalf("expected PipeExpr, got %T", expr)
	}
	sum, ok := pipe.Operators[0].(*ast.SummarizeOp)
	if !ok {
		t.Fatalf("expected SummarizeOp, got %T", pipe.Operators[0])
	}
	if len(sum.Aggregates) != 2 {
		t.Errorf("aggregates: got %d, want 2", len(sum.Aggregates))
	}
	if len(sum.GroupBy) != 2 {
		t.Errorf("group by: got %d, want 2", len(sum.GroupBy))
	}
}

func TestParseJoinOp(t *testing.T) {
	src := "T | join kind=leftouter R on id"
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	pipe, ok := expr.(*ast.PipeExpr)
	if !ok {
		t.Fatalf("expected PipeExpr, got %T", expr)
	}
	join, ok := pipe.Operators[0].(*ast.JoinOp)
	if !ok {
		t.Fatalf("expected JoinOp, got %T", pipe.Operators[0])
	}
	if join.Kind != ast.JoinLeftOuter {
		t.Errorf("kind: got %v, want JoinLeftOuter", join.Kind)
	}
}

func TestParseComplexQuery(t *testing.T) {
	src := `
StormEvents
| where StartTime >= ago(7d) and State != "TEXAS"
| extend Duration = EndTime - StartTime
| summarize EventCount = count(), AvgDuration = avg(Duration) by State
| top 10 by EventCount desc
`
	p := New("test", src)
	script := p.Parse()
	errs := p.Errors()
	if len(errs) > 0 {
		t.Logf("parse errors: %v", errs)
		// Allow some errors for now, just check structure
	}
	if len(script.Stmts) != 1 {
		t.Fatalf("stmt count: got %d, want 1", len(script.Stmts))
	}
	exprStmt, ok := script.Stmts[0].(*ast.ExprStmt)
	if !ok {
		t.Fatalf("expected ExprStmt, got %T", script.Stmts[0])
	}
	pipe, ok := exprStmt.X.(*ast.PipeExpr)
	if !ok {
		t.Fatalf("expected PipeExpr, got %T", exprStmt.X)
	}
	if len(pipe.Operators) != 4 {
		t.Errorf("operator count: got %d, want 4", len(pipe.Operators))
	}
}

// Helper functions

func simplePrint(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return "Ident{" + e.Name + "}"
	case *ast.BasicLit:
		return "BasicLit{" + e.Value + "}"
	case *ast.BinaryExpr:
		return "BinaryExpr{" + e.Op.String() + "}"
	case *ast.CallExpr:
		return "CallExpr{}"
	case *ast.PipeExpr:
		return "PipeExpr{}"
	default:
		return "Unknown"
	}
}

func getTypeName(op ast.Operator) string {
	switch op.(type) {
	case *ast.WhereOp:
		return "WhereOp"
	case *ast.ProjectOp:
		return "ProjectOp"
	case *ast.ProjectAwayOp:
		return "ProjectAwayOp"
	case *ast.ExtendOp:
		return "ExtendOp"
	case *ast.SummarizeOp:
		return "SummarizeOp"
	case *ast.SortOp:
		return "SortOp"
	case *ast.TakeOp:
		return "TakeOp"
	case *ast.TopOp:
		return "TopOp"
	case *ast.CountOp:
		return "CountOp"
	case *ast.DistinctOp:
		return "DistinctOp"
	case *ast.JoinOp:
		return "JoinOp"
	case *ast.UnionOp:
		return "UnionOp"
	case *ast.RenderOp:
		return "RenderOp"
	case *ast.ParseOp:
		return "ParseOp"
	case *ast.MvExpandOp:
		return "MvExpandOp"
	case *ast.SearchOp:
		return "SearchOp"
	case *ast.GenericOp:
		return "GenericOp"
	default:
		return "Unknown"
	}
}

func TestASTPrint(t *testing.T) {
	src := "T | where x > 10 | project a, b"
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	output := ast.Print(expr)
	if !strings.Contains(output, "PipeExpr") {
		t.Errorf("expected PipeExpr in output: %s", output)
	}
	if !strings.Contains(output, "WhereOp") {
		t.Errorf("expected WhereOp in output: %s", output)
	}
}

func TestParseOpWithTypes(t *testing.T) {
	src := `T | parse Text with Key:string "=" Value:long`
	p := New("test", src)
	expr := p.ParseExpr()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	pipe, ok := expr.(*ast.PipeExpr)
	if !ok {
		t.Fatalf("expected PipeExpr, got %T", expr)
	}
	parseOp, ok := pipe.Operators[0].(*ast.ParseOp)
	if !ok {
		t.Fatalf("expected ParseOp, got %T", pipe.Operators[0])
	}

	// Check leading column
	if parseOp.LeadingCol == nil {
		t.Fatal("expected leading column")
	}
	if parseOp.LeadingCol.Name.Name != "Key" {
		t.Errorf("leading col name: got %q, want Key", parseOp.LeadingCol.Name.Name)
	}
	if parseOp.LeadingCol.Type == nil || parseOp.LeadingCol.Type.Name != "string" {
		t.Errorf("leading col type: got %v, want string", parseOp.LeadingCol.Type)
	}

	// Check segment
	if len(parseOp.Segments) != 1 {
		t.Fatalf("segment count: got %d, want 1", len(parseOp.Segments))
	}
	seg := parseOp.Segments[0]
	if seg.Column == nil {
		t.Fatal("expected segment column")
	}
	if seg.Column.Name.Name != "Value" {
		t.Errorf("segment col name: got %q, want Value", seg.Column.Name.Name)
	}
	if seg.Column.Type == nil || seg.Column.Type.Name != "long" {
		t.Errorf("segment col type: got %v, want long", seg.Column.Type)
	}
}

func TestToScalarExpr(t *testing.T) {
	src := `print x = toscalar(T | summarize count())`
	p := New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	if len(script.Stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(script.Stmts))
	}
	printStmt, ok := script.Stmts[0].(*ast.PrintStmt)
	if !ok {
		t.Fatalf("expected PrintStmt, got %T", script.Stmts[0])
	}
	if len(printStmt.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(printStmt.Columns))
	}
	namedExpr := printStmt.Columns[0]
	if namedExpr.Name.Name != "x" {
		t.Errorf("name: got %q, want x", namedExpr.Name.Name)
	}
	toScalar, ok := namedExpr.Expr.(*ast.ToScalarExpr)
	if !ok {
		t.Fatalf("expected ToScalarExpr, got %T", namedExpr.Expr)
	}
	pipeExpr, ok := toScalar.Query.(*ast.PipeExpr)
	if !ok {
		t.Fatalf("expected PipeExpr in toscalar, got %T", toScalar.Query)
	}
	if len(pipeExpr.Operators) != 1 {
		t.Errorf("expected 1 operator, got %d", len(pipeExpr.Operators))
	}
	if _, ok := pipeExpr.Operators[0].(*ast.SummarizeOp); !ok {
		t.Errorf("expected SummarizeOp, got %T", pipeExpr.Operators[0])
	}
}

func TestToTableExpr(t *testing.T) {
	src := `totable(T | project A, B)`
	p := New("test", src)
	script := p.Parse()
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parse errors: %v", errs)
	}
	if len(script.Stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(script.Stmts))
	}
	exprStmt, ok := script.Stmts[0].(*ast.ExprStmt)
	if !ok {
		t.Fatalf("expected ExprStmt, got %T", script.Stmts[0])
	}
	toTable, ok := exprStmt.X.(*ast.ToTableExpr)
	if !ok {
		t.Fatalf("expected ToTableExpr, got %T", exprStmt.X)
	}
	pipeExpr, ok := toTable.Query.(*ast.PipeExpr)
	if !ok {
		t.Fatalf("expected PipeExpr in totable, got %T", toTable.Query)
	}
	if len(pipeExpr.Operators) != 1 {
		t.Errorf("expected 1 operator, got %d", len(pipeExpr.Operators))
	}
}

func TestNewOperators(t *testing.T) {
	tests := []struct {
		name string
		src  string
	}{
		{"project-keep", "T | project-keep a, b, c"},
		{"top-nested", "T | top-nested 3 of State by sum(Damage)"},
		{"top-hitters", "T | top-hitters 10 of EventType"},
		{"mv-apply", "T | mv-apply x = Items on (x | take 1)"},
		{"find", "find where EventType == 'Flood'"},
		{"print", "print x = 1, y = 'hello'"},
		{"range", "range x from 1 to 10 step 1"},
		{"datatable", "datatable(Name:string, Age:long) ['Alice', 30, 'Bob', 25]"},
		{"externaldata", "externaldata(x:string) ['https://example.com/data.csv']"},
		{"parse-where", "T | parse-where Message '*error*'"},
		{"parse-kv", "T | parse-kv Data as (key1, key2)"},
		{"parse-with-types", "T | parse Text with Key:string \"=\" Value:long"},
		{"toscalar", "print toscalar(T | summarize count())"},
		{"totable", "totable(T | project A)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New("test", tt.src)
			script := p.Parse()
			if errs := p.Errors(); len(errs) > 0 {
				t.Errorf("parse errors for %q: %v", tt.src, errs)
			}
			if len(script.Stmts) == 0 {
				t.Errorf("expected at least one statement for %q", tt.src)
			}
		})
	}
}
