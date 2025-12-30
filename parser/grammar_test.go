package parser_test

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudygreybeard/kqlparser/parser"
)

// TestGrammarDerivedCases tests KQL patterns extracted from the official ANTLR grammar.
// These test cases are located in testdata/grammar/*.kql files.
func TestGrammarDerivedCases(t *testing.T) {
	testDataDir := filepath.Join("..", "testdata", "grammar")

	// Find all .kql files in the grammar directory
	files, err := filepath.Glob(filepath.Join(testDataDir, "*.kql"))
	if err != nil {
		t.Fatalf("failed to glob test files: %v", err)
	}

	if len(files) == 0 {
		t.Skip("no grammar test files found")
	}

	for _, file := range files {
		file := file // capture
		baseName := filepath.Base(file)
		t.Run(baseName, func(t *testing.T) {
			t.Parallel()
			testGrammarFile(t, file)
		})
	}
}

func testGrammarFile(t *testing.T, filename string) {
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("failed to open %s: %v", filename, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var currentQuery strings.Builder
	lineNum := 0
	startLine := 0
	inMultiLine := false

	flush := func() {
		query := strings.TrimSpace(currentQuery.String())
		if query != "" && !strings.HasPrefix(query, "//") {
			testSingleQuery(t, query, filename, startLine)
		}
		currentQuery.Reset()
		inMultiLine = false
	}

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip pure comment lines (entire line is a comment)
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			// If we have accumulated content, flush it first
			if currentQuery.Len() > 0 {
				flush()
			}
			continue
		}

		// Skip empty lines - they terminate multi-line queries
		if trimmed == "" {
			if currentQuery.Len() > 0 {
				flush()
			}
			continue
		}

		// Start new query or continue existing
		if currentQuery.Len() == 0 {
			startLine = lineNum
		} else {
			currentQuery.WriteString("\n")
		}

		// Check for multi-line string markers
		if strings.Contains(line, "```") || strings.Contains(line, "~~~") {
			inMultiLine = !inMultiLine
		}

		currentQuery.WriteString(line)

		// In multi-line mode, don't flush until we close the block
		if inMultiLine {
			continue
		}

		// Single-line statements ending with certain patterns can be flushed
		// But let statements, set statements, etc. may span multiple lines
		// We rely on empty lines to separate queries
	}

	// Flush remaining content
	flush()

	if err := scanner.Err(); err != nil {
		t.Errorf("error scanning %s: %v", filename, err)
	}
}

func testSingleQuery(t *testing.T, query, filename string, lineNum int) {
	t.Helper()

	// Create a parser for this query
	p := parser.New(fmt.Sprintf("%s:%d", filepath.Base(filename), lineNum), query)
	_ = p.Parse()

	// Check for parse errors
	errs := p.Errors()
	if len(errs) > 0 {
		// Report the first error with context
		t.Errorf("%s:%d: parse error for query:\n  %s\n  Error: %v",
			filepath.Base(filename), lineNum,
			truncateQuery(query, 80),
			errs[0])
	}
}

func truncateQuery(q string, maxLen int) string {
	// Replace newlines with spaces for compact display
	q = strings.ReplaceAll(q, "\n", " ")
	q = strings.Join(strings.Fields(q), " ") // normalize whitespace
	if len(q) > maxLen {
		return q[:maxLen-3] + "..."
	}
	return q
}

// TestSpecificGrammarPatterns tests specific patterns from the grammar
// that are particularly important or tricky.
func TestSpecificGrammarPatterns(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		// Literals
		{"long_literal_hex", "print 0x1A2B", false},
		{"long_literal_typed", "print long(42)", false},
		{"real_literal_exp", "print 1.5e-10", false},
		{"timespan_minutes", "print 5min", false},
		{"timespan_hours", "print 2hr", false},
		{"string_verbatim", `print @"no\escape"`, false},
		{"string_multiline", "print ```multi\nline```", true},                      // TODO: not yet supported
		{"guid_literal", "print guid(12345678-1234-1234-1234-123456789abc)", true}, // TODO: not yet supported

		// Operators
		{"where_simple", "T | where x > 0", false},
		{"where_and_or", "T | where x > 0 and y < 10 or z == 5", false},
		{"join_simple", "T1 | join T2 on Key", false},
		{"join_kind", "T1 | join kind=inner T2 on Key", false},
		{"union_multiple", "union T1, T2, T3", true}, // TODO: comma-separated union not supported
		{"union_wildcard", "union T*", false},
		{"make_series", "T | make-series count() on Time step 1h", false},
		{"mv_expand", "T | mv-expand Col", false},
		{"mv_apply", "T | mv-apply x = Arr on (extend y = x * 2)", true}, // TODO: assignment form not supported
		{"parse_with", `T | parse Text with * "=" Value`, false},
		{"top_nested", "T | top-nested 5 of State by count()", false},
		{"sample_distinct", "T | sample-distinct 10 of State", false},
		{"graph_match", "G | graph-match (a)-[e]->(b) project a.Name", true}, // TODO: graph ops not supported

		// Expressions
		{"between", "T | where x between (1 .. 10)", false},
		{"in_list", `T | where State in ("CA", "WA")`, false},
		{"has_any", `T | where Tags has_any ("a", "b")`, true}, // TODO: has_any multi-arg not supported
		{"string_has", `T | where Text has "word"`, false},
		{"string_contains", `T | where Text contains "sub"`, false},
		{"matches_regex", `T | where Text matches regex "^[0-9]+$"`, false},
		{"toscalar", "print toscalar(T | summarize count())", true}, // TODO: pipe in toscalar not supported
		{"path_access", "print obj.prop.nested", false},
		{"element_access", "print arr[0]", false},

		// Statements
		{"let_simple", "let x = 5", false},
		{"let_function", "let f = (a: int) { a * 2 }", true},    // TODO: typed params not supported
		{"let_tabular", "let f = (T: (*)) { T | count }", true}, // TODO: tabular params not supported
		{"set_option", "set notruncation", false},
		{"declare_params", "declare query_parameters(x: int)", true}, // TODO: not supported

		// Entity references
		{"cluster_db_table", "cluster('help').database('Samples').StormEvents", false},
		{"database_table", "database('Samples').StormEvents", false},

		// datatable and externaldata
		{"datatable", `datatable(Name: string, Val: int) ["A", 1]`, false},
		{"externaldata", `externaldata(Col: string) ["https://example.com/data"]`, false},
		{"range", "range x from 1 to 10 step 1", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p := parser.New("test", tt.query)
			_ = p.Parse()
			errs := p.Errors()

			if tt.wantErr && len(errs) == 0 {
				t.Errorf("expected error, got none for: %s", tt.query)
			}
			if !tt.wantErr && len(errs) > 0 {
				t.Errorf("unexpected error for %q: %v", tt.query, errs[0])
			}
		})
	}
}

// TestTokenPatterns tests that specific tokens from KqlTokens.g4 are recognized.
func TestTokenPatterns(t *testing.T) {
	// Test timespan literal variations from grammar
	timespanTests := []string{
		"5m", "5min", "5minute", "5minutes",
		"30s", "30sec", "30second", "30seconds",
		"1d", "1day", "7days",
		"2h", "2hour", "24hours", "2hr", "2hrs",
		"100ms", "100milli", "100milliseconds",
		"100micro", "100microseconds",
		"100nano", "100nanoseconds",
		"1000tick", "1000ticks",
	}

	for _, ts := range timespanTests {
		t.Run("timespan/"+ts, func(t *testing.T) {
			p := parser.New("test", "print "+ts)
			_ = p.Parse()
			if errs := p.Errors(); len(errs) > 0 {
				t.Errorf("failed to parse timespan %q: %v", ts, errs[0])
			}
		})
	}

	// Test string operator tokens
	stringOps := []struct {
		op   string
		skip bool
	}{
		// Positive operators
		{"has", false},
		{"has_cs", false},
		{"hasprefix", false},
		{"hassuffix", false},
		{"contains", false},
		{"contains_cs", false},
		{"startswith", false},
		{"startswith_cs", false},
		{"endswith", false},
		{"endswith_cs", false},
		{"matches regex", false},
		// Negated operators - now supported!
		{"!has", false},
		{"!has_cs", false},
		{"!hasprefix", false},
		{"!hasprefix_cs", false},
		{"!hassuffix", false},
		{"!hassuffix_cs", false},
		{"!contains", false},
		{"!contains_cs", false},
		{"!startswith", false},
		{"!startswith_cs", false},
		{"!endswith", false},
		{"!endswith_cs", false},
		// like/notlike - TODO: not yet fully supported
		{"like", true},
		{"notlike", true},
		{"likecs", true},
		{"notlikecs", true},
	}

	for _, tc := range stringOps {
		tc := tc
		t.Run("stringop/"+tc.op, func(t *testing.T) {
			t.Parallel()
			if tc.skip {
				t.Skipf("string operator %q not yet supported", tc.op)
			}
			query := fmt.Sprintf(`T | where Col %s "test"`, tc.op)
			p := parser.New("test", query)
			_ = p.Parse()
			if errs := p.Errors(); len(errs) > 0 {
				t.Errorf("failed to parse string operator %q: %v", tc.op, errs[0])
			}
		})
	}
}

// TestOperatorKeywords tests operator keywords from the grammar.
func TestOperatorKeywords(t *testing.T) {
	operators := []struct {
		keyword string
		example string
		skip    bool // Skip if feature not yet supported
	}{
		{"as", "T | as MyTable", false},
		{"assert-schema", "T | assert-schema (x: int)", true}, // TODO: not supported
		{"consume", "T | consume", false},
		{"count", "T | count", false},
		{"distinct", "T | distinct *", false},
		{"evaluate", "T | evaluate bag_unpack(Col)", false},
		{"extend", "T | extend x = 1", false},
		{"facet", "T | facet by Col", false},
		{"find", "find 'test'", false},
		{"fork", "T | fork (take 5)", true}, // TODO: subquery in parens not working
		{"getschema", "T | getschema", false},
		{"invoke", "T | invoke myFunc()", false},
		{"join", "T1 | join T2 on Key", false},
		{"limit", "T | limit 10", false},
		{"lookup", "T1 | lookup T2 on Key", false},
		{"make-series", "T | make-series count() on Time step 1h", false},
		{"mv-apply", "T | mv-apply Items on (take 5)", true}, // TODO: subquery form not working
		{"mv-expand", "T | mv-expand Col", false},
		{"parse", `T | parse Col with * "=" Val`, false},
		{"parse-kv", "T | parse-kv Col as (A, B)", false}, // Simplified (no types)
		{"parse-where", `T | parse-where Col with * "=" Val`, false},
		{"partition", "T | partition by Col (take 5)", true}, // TODO: subquery form not working
		{"print", "print 1", false},
		{"project", "T | project A, B", false},
		{"project-away", "T | project-away Temp", false},
		{"project-keep", "T | project-keep Name", false},
		{"project-rename", "T | project-rename New = Old", false},
		{"project-reorder", "T | project-reorder A, B, *", false},
		{"range", "range x from 1 to 10 step 1", false},
		{"reduce", "T | reduce by Col", false},
		{"render", "T | render table", false},
		{"sample", "T | sample 100", false},
		{"sample-distinct", "T | sample-distinct 10 of Col", false},
		{"scan", "T | scan with (step s: true;)", true}, // TODO: step syntax not working
		{"search", "search 'term'", false},
		{"serialize", "T | serialize", false},
		{"sort", "T | sort by Col", false},
		{"order", "T | order by Col", false},
		{"summarize", "T | summarize count()", false},
		{"take", "T | take 10", false},
		{"top", "T | top 10 by Col", false},
		{"top-hitters", "T | top-hitters 10 of Col", false},
		{"top-nested", "T | top-nested 5 of Col by count()", false},
		{"union", "union (T)", false}, // Use parenthesized form
		{"where", "T | where x > 0", false},
		{"filter", "T | filter x > 0", false},
	}

	for _, op := range operators {
		op := op // capture
		t.Run(op.keyword, func(t *testing.T) {
			t.Parallel()
			if op.skip {
				t.Skipf("operator %q not yet supported", op.keyword)
			}
			p := parser.New("test", op.example)
			_ = p.Parse()
			if errs := p.Errors(); len(errs) > 0 {
				t.Errorf("failed to parse %q operator: %v\n  Query: %s",
					op.keyword, errs[0], op.example)
			}
		})
	}
}
