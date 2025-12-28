package kqlparser

import (
	"strings"
	"testing"

	"github.com/cloudygreybeard/kqlparser/lexer"
	"github.com/cloudygreybeard/kqlparser/parser"
	"github.com/cloudygreybeard/kqlparser/symbol"
	"github.com/cloudygreybeard/kqlparser/token"
	"github.com/cloudygreybeard/kqlparser/types"
)

// Test queries of varying complexity
var (
	querySimple = `StormEvents | take 10`

	queryMedium = `
StormEvents
| where StartTime >= datetime("2007-01-01") and StartTime < datetime("2008-01-01")
| where State == "TEXAS"
| summarize count() by EventType
| top 10 by count_ desc
`

	queryComplex = `
let startDate = datetime("2007-01-01");
let endDate = datetime("2008-01-01");
StormEvents
| where StartTime >= startDate and StartTime < endDate
| where DamageProperty > 0 or DamageCrops > 0
| extend TotalDamage = DamageProperty + DamageCrops
| summarize 
    EventCount = count(),
    TotalPropertyDamage = sum(DamageProperty),
    TotalCropDamage = sum(DamageCrops),
    AvgDamage = avg(TotalDamage),
    MaxDamage = max(TotalDamage)
    by State, EventType
| where EventCount > 5
| order by TotalPropertyDamage desc
| project State, EventType, EventCount, TotalPropertyDamage, TotalCropDamage
| take 100
`

	queryLarge = strings.Repeat(`
StormEvents
| where StartTime >= datetime("2007-01-01")
| extend Year = datetime_part("year", StartTime)
| extend Month = datetime_part("month", StartTime)
| summarize count() by Year, Month, State
| order by Year, Month, State
`, 10)
)

// =============================================================================
// Lexer Benchmarks
// =============================================================================

func BenchmarkLexer_Simple(b *testing.B) {
	benchmarkLexer(b, querySimple)
}

func BenchmarkLexer_Medium(b *testing.B) {
	benchmarkLexer(b, queryMedium)
}

func BenchmarkLexer_Complex(b *testing.B) {
	benchmarkLexer(b, queryComplex)
}

func BenchmarkLexer_Large(b *testing.B) {
	benchmarkLexer(b, queryLarge)
}

func benchmarkLexer(b *testing.B, src string) {
	b.ReportAllocs()
	b.SetBytes(int64(len(src)))

	for i := 0; i < b.N; i++ {
		l := lexer.New("bench", src)
		for {
			tok := l.Scan()
			if tok.Type == token.EOF {
				break
			}
		}
	}
}

// =============================================================================
// Parser Benchmarks
// =============================================================================

func BenchmarkParser_Simple(b *testing.B) {
	benchmarkParser(b, querySimple)
}

func BenchmarkParser_Medium(b *testing.B) {
	benchmarkParser(b, queryMedium)
}

func BenchmarkParser_Complex(b *testing.B) {
	benchmarkParser(b, queryComplex)
}

func BenchmarkParser_Large(b *testing.B) {
	benchmarkParser(b, queryLarge)
}

func benchmarkParser(b *testing.B, src string) {
	b.ReportAllocs()
	b.SetBytes(int64(len(src)))

	for i := 0; i < b.N; i++ {
		p := parser.New("bench", src)
		_ = p.Parse()
	}
}

// =============================================================================
// Full Pipeline Benchmarks (Parse + Analyze)
// =============================================================================

func BenchmarkParseAndAnalyze_Simple(b *testing.B) {
	benchmarkParseAndAnalyze(b, querySimple, nil)
}

func BenchmarkParseAndAnalyze_Medium(b *testing.B) {
	benchmarkParseAndAnalyze(b, queryMedium, nil)
}

func BenchmarkParseAndAnalyze_Complex(b *testing.B) {
	benchmarkParseAndAnalyze(b, queryComplex, nil)
}

func BenchmarkParseAndAnalyze_Large(b *testing.B) {
	benchmarkParseAndAnalyze(b, queryLarge, nil)
}

func BenchmarkParseAndAnalyze_WithSchema(b *testing.B) {
	globals := NewGlobals()
	globals.Database = symbol.NewDatabase("Samples")
	globals.Database.AddTable(symbol.NewTable("StormEvents",
		types.NewColumn("StartTime", types.Typ_DateTime),
		types.NewColumn("EndTime", types.Typ_DateTime),
		types.NewColumn("State", types.Typ_String),
		types.NewColumn("EventType", types.Typ_String),
		types.NewColumn("DamageProperty", types.Typ_Long),
		types.NewColumn("DamageCrops", types.Typ_Long),
		types.NewColumn("DeathsDirect", types.Typ_Long),
		types.NewColumn("DeathsIndirect", types.Typ_Long),
		types.NewColumn("InjuriesDirect", types.Typ_Long),
		types.NewColumn("InjuriesIndirect", types.Typ_Long),
	))

	benchmarkParseAndAnalyze(b, queryComplex, globals)
}

func benchmarkParseAndAnalyze(b *testing.B, src string, globals *Globals) {
	b.ReportAllocs()
	b.SetBytes(int64(len(src)))

	for i := 0; i < b.N; i++ {
		_ = ParseAndAnalyze("bench", src, globals)
	}
}

// =============================================================================
// Comparison Benchmarks (Parse-only vs Full Analysis)
// =============================================================================

func BenchmarkParseOnly_Complex(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(queryComplex)))

	for i := 0; i < b.N; i++ {
		_ = Parse("bench", queryComplex)
	}
}

func BenchmarkFullAnalysis_Complex(b *testing.B) {
	globals := NewGlobals()
	globals.Database = symbol.NewDatabase("Samples")
	globals.Database.AddTable(symbol.NewTable("StormEvents",
		types.NewColumn("StartTime", types.Typ_DateTime),
		types.NewColumn("State", types.Typ_String),
		types.NewColumn("EventType", types.Typ_String),
		types.NewColumn("DamageProperty", types.Typ_Long),
		types.NewColumn("DamageCrops", types.Typ_Long),
	))

	b.ReportAllocs()
	b.SetBytes(int64(len(queryComplex)))

	for i := 0; i < b.N; i++ {
		_ = ParseAndAnalyze("bench", queryComplex, globals)
	}
}

// =============================================================================
// Throughput Benchmark
// =============================================================================

func BenchmarkThroughput_1KB(b *testing.B) {
	// Create ~1KB of KQL
	src := strings.Repeat(queryMedium, 5)
	benchmarkParser(b, src)
}

func BenchmarkThroughput_10KB(b *testing.B) {
	// Create ~10KB of KQL
	src := strings.Repeat(queryMedium, 50)
	benchmarkParser(b, src)
}

func BenchmarkThroughput_100KB(b *testing.B) {
	// Create ~100KB of KQL
	src := strings.Repeat(queryMedium, 500)
	benchmarkParser(b, src)
}
