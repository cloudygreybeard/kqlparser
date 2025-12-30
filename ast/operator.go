package ast

import (
	"github.com/cloudygreybeard/kqlparser/token"
)

// OperatorParam represents a query operator parameter (e.g., hint.strategy=broadcast).
type OperatorParam struct {
	Name   *Ident    // Parameter name (e.g., "hint.strategy", "kind")
	Assign token.Pos // Position of "="
	Value  Expr      // Parameter value (identifier or literal)
}

func (x *OperatorParam) Pos() token.Pos { return x.Name.Pos() }
func (x *OperatorParam) End() token.Pos { return x.Value.End() }

// WhereOp represents a where operator (| where <predicate>).
type WhereOp struct {
	Pipe      token.Pos // Position of "|"
	Where     token.Pos // Position of "where"
	Predicate Expr      // Filter predicate
}

func (x *WhereOp) Pos() token.Pos { return x.Pipe }
func (x *WhereOp) End() token.Pos { return x.Predicate.End() }

// ProjectOp represents a project operator.
type ProjectOp struct {
	Pipe    token.Pos    // Position of "|"
	Project token.Pos    // Position of "project"
	Columns []*NamedExpr // Projected columns
}

func (x *ProjectOp) Pos() token.Pos { return x.Pipe }

func (x *ProjectOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.Project + 7 // len("project")
}

// ProjectAwayOp represents a project-away operator.
type ProjectAwayOp struct {
	Pipe        token.Pos // Position of "|"
	ProjectAway token.Pos // Position of "project-away"
	Columns     []*Ident  // Columns to remove
}

func (x *ProjectAwayOp) Pos() token.Pos { return x.Pipe }

func (x *ProjectAwayOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.ProjectAway + 12 // len("project-away")
}

// ExtendOp represents an extend operator.
type ExtendOp struct {
	Pipe    token.Pos    // Position of "|"
	Extend  token.Pos    // Position of "extend"
	Columns []*NamedExpr // Extended columns
}

func (x *ExtendOp) Pos() token.Pos { return x.Pipe }

func (x *ExtendOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.Extend + 6 // len("extend")
}

// SummarizeOp represents a summarize operator.
type SummarizeOp struct {
	Pipe       token.Pos        // Position of "|"
	Summarize  token.Pos        // Position of "summarize"
	Params     []*OperatorParam // Operator parameters (hints, etc.)
	Aggregates []*NamedExpr     // Aggregate expressions
	ByPos      token.Pos        // Position of "by" (NoPos if no by clause)
	GroupBy    []*NamedExpr     // Group by expressions
}

func (x *SummarizeOp) Pos() token.Pos { return x.Pipe }

func (x *SummarizeOp) End() token.Pos {
	if len(x.GroupBy) > 0 {
		return x.GroupBy[len(x.GroupBy)-1].End()
	}
	if len(x.Aggregates) > 0 {
		return x.Aggregates[len(x.Aggregates)-1].End()
	}
	return x.Summarize + 9 // len("summarize")
}

// SortOp represents a sort/order operator.
type SortOp struct {
	Pipe   token.Pos        // Position of "|"
	Sort   token.Pos        // Position of "sort" or "order"
	Params []*OperatorParam // Operator parameters (hints, etc.)
	ByPos  token.Pos        // Position of "by"
	Orders []*OrderExpr     // Ordered expressions
}

func (x *SortOp) Pos() token.Pos { return x.Pipe }

func (x *SortOp) End() token.Pos {
	if len(x.Orders) > 0 {
		return x.Orders[len(x.Orders)-1].End()
	}
	return x.ByPos + 2 // len("by")
}

// OrderExpr represents an expression with optional ordering.
type OrderExpr struct {
	Expr  Expr        // Expression to order by
	Order token.Token // ASC, DESC, or ILLEGAL for default
	Nulls token.Token // FIRST, LAST, or ILLEGAL for default
}

func (x *OrderExpr) Pos() token.Pos { return x.Expr.Pos() }
func (x *OrderExpr) End() token.Pos { return x.Expr.End() } // Simplified

// TakeOp represents a take/limit operator.
type TakeOp struct {
	Pipe  token.Pos // Position of "|"
	Take  token.Pos // Position of "take" or "limit"
	Count Expr      // Number of rows to take
}

func (x *TakeOp) Pos() token.Pos { return x.Pipe }
func (x *TakeOp) End() token.Pos { return x.Count.End() }

// TopOp represents a top operator.
type TopOp struct {
	Pipe   token.Pos  // Position of "|"
	Top    token.Pos  // Position of "top"
	Count  Expr       // Number of rows
	ByPos  token.Pos  // Position of "by"
	ByExpr *OrderExpr // Expression to order by
}

func (x *TopOp) Pos() token.Pos { return x.Pipe }
func (x *TopOp) End() token.Pos { return x.ByExpr.End() }

// CountOp represents a count operator.
type CountOp struct {
	Pipe  token.Pos // Position of "|"
	Count token.Pos // Position of "count"
}

func (x *CountOp) Pos() token.Pos { return x.Pipe }
func (x *CountOp) End() token.Pos { return x.Count + 5 } // len("count")

// DistinctOp represents a distinct operator.
type DistinctOp struct {
	Pipe     token.Pos // Position of "|"
	Distinct token.Pos // Position of "distinct"
	Columns  []Expr    // Columns to make distinct (can include *)
}

func (x *DistinctOp) Pos() token.Pos { return x.Pipe }

func (x *DistinctOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.Distinct + 8 // len("distinct")
}

// JoinKind represents the kind of join.
type JoinKind int

const (
	JoinInner JoinKind = iota
	JoinLeftOuter
	JoinRightOuter
	JoinFullOuter
	JoinLeftSemi
	JoinRightSemi
	JoinLeftAnti
	JoinRightAnti
)

// JoinOp represents a join operator.
type JoinOp struct {
	Pipe   token.Pos        // Position of "|"
	Join   token.Pos        // Position of "join"
	Params []*OperatorParam // Operator parameters (kind, hints, etc.)
	Kind   JoinKind         // Join kind (derived from kind parameter)
	Right  Expr             // Right side expression
	OnPos  token.Pos        // Position of "on"
	OnExpr []Expr           // Join conditions
}

func (x *JoinOp) Pos() token.Pos { return x.Pipe }

func (x *JoinOp) End() token.Pos {
	if len(x.OnExpr) > 0 {
		return x.OnExpr[len(x.OnExpr)-1].End()
	}
	return x.Right.End()
}

// UnionOp represents a union operator.
type UnionOp struct {
	Pipe   token.Pos        // Position of "|"
	Union  token.Pos        // Position of "union"
	Params []*OperatorParam // Operator parameters (kind, withsource, etc.)
	Tables []Expr           // Tables to union
}

func (x *UnionOp) Pos() token.Pos { return x.Pipe }

func (x *UnionOp) End() token.Pos {
	if len(x.Tables) > 0 {
		return x.Tables[len(x.Tables)-1].End()
	}
	return x.Union + 5 // len("union")
}

// RenderOp represents a render operator.
type RenderOp struct {
	Pipe       token.Pos         // Position of "|"
	Render     token.Pos         // Position of "render"
	ChartType  *Ident            // Chart type (table, piechart, barchart, etc.)
	WithPos    token.Pos         // Position of "with" (NoPos if no properties)
	Properties []*RenderProperty // Render properties (title, xcolumn, etc.)
}

// RenderProperty represents a property in a render with clause.
type RenderProperty struct {
	Name   *Ident    // Property name (title, xcolumn, series, kind, etc.)
	Assign token.Pos // Position of "="
	Value  Expr      // Property value (string, identifier, or list)
}

func (x *RenderOp) Pos() token.Pos { return x.Pipe }
func (x *RenderOp) End() token.Pos {
	if len(x.Properties) > 0 {
		return x.Properties[len(x.Properties)-1].Value.End()
	}
	return x.ChartType.End()
}

// ParseColumn represents a column capture in a parse pattern (e.g., Key:string).
type ParseColumn struct {
	Name *Ident // Column name
	Type *Ident // Optional type annotation (e.g., "string", "long")
}

func (x *ParseColumn) Pos() token.Pos { return x.Name.Pos() }
func (x *ParseColumn) End() token.Pos {
	if x.Type != nil {
		return x.Type.End()
	}
	return x.Name.End()
}
func (x *ParseColumn) expr() {}

// ParsePatternSegment represents a segment in a parse pattern.
type ParsePatternSegment struct {
	Star   bool         // Whether preceded by *
	Text   Expr         // String literal delimiter
	Column *ParseColumn // Optional column capture after delimiter
}

// ParseOp represents a parse operator.
type ParseOp struct {
	Pipe         token.Pos              // Position of "|"
	Parse        token.Pos              // Position of "parse"
	Kind         string                 // Parse kind: "", "simple", "regex", "relaxed"
	Source       Expr                   // Source expression to parse
	WithPos      token.Pos              // Position of "with"
	LeadingCol   *ParseColumn           // Optional leading column before any delimiter
	Segments     []*ParsePatternSegment // Pattern segments
	TrailingStar bool                   // Whether pattern ends with *
	Pattern      Expr                   // Legacy: simple parse pattern (for backward compat)
}

func (x *ParseOp) Pos() token.Pos { return x.Pipe }
func (x *ParseOp) End() token.Pos {
	if len(x.Segments) > 0 {
		lastSeg := x.Segments[len(x.Segments)-1]
		if lastSeg.Column != nil {
			return lastSeg.Column.End()
		}
		return lastSeg.Text.End()
	}
	if x.LeadingCol != nil {
		return x.LeadingCol.End()
	}
	if x.Pattern != nil {
		return x.Pattern.End()
	}
	return x.WithPos
}

// ParseWhereOp represents a parse-where operator.
type ParseWhereOp struct {
	Pipe       token.Pos // Position of "|"
	ParseWhere token.Pos // Position of "parse-where"
	Source     Expr      // Source expression to parse
	Pattern    Expr      // Parse pattern
}

func (x *ParseWhereOp) Pos() token.Pos { return x.Pipe }
func (x *ParseWhereOp) End() token.Pos { return x.Pattern.End() }

// ParseKvOp represents a parse-kv operator.
type ParseKvOp struct {
	Pipe    token.Pos // Position of "|"
	ParseKv token.Pos // Position of "parse-kv"
	Source  Expr      // Source column
	AsPos   token.Pos // Position of "as" (optional)
	Columns []Expr    // Output columns (optional)
	WithPos token.Pos // Position of "with" (optional)
	Options []Expr    // Options (optional)
}

func (x *ParseKvOp) Pos() token.Pos { return x.Pipe }

func (x *ParseKvOp) End() token.Pos {
	if len(x.Options) > 0 {
		return x.Options[len(x.Options)-1].End()
	}
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.Source.End()
}

// MvExpandOp represents an mv-expand operator.
// MvExpandColumn represents a column in mv-expand with optional name and type.
type MvExpandColumn struct {
	Name   *Ident    // Optional name for the expanded column
	Assign token.Pos // Position of "=" (NoPos if unnamed)
	Expr   Expr      // Column expression to expand
	ToPos  token.Pos // Position of "to" (NoPos if no type)
	Type   Expr      // Type annotation (nil if none)
}

func (x *MvExpandColumn) Pos() token.Pos {
	if x.Name != nil {
		return x.Name.Pos()
	}
	return x.Expr.Pos()
}

func (x *MvExpandColumn) End() token.Pos {
	if x.Type != nil {
		return x.Type.End()
	}
	return x.Expr.End()
}

type MvExpandOp struct {
	Pipe     token.Pos         // Position of "|"
	MvExpand token.Pos         // Position of "mv-expand"
	Params   []*OperatorParam  // Parameters (bagexpansion, with_itemindex)
	LimitPos token.Pos         // Position of "limit" (NoPos if none)
	Limit    Expr              // Limit expression (nil if none)
	Columns  []*MvExpandColumn // Columns to expand
}

func (x *MvExpandOp) Pos() token.Pos { return x.Pipe }

func (x *MvExpandOp) End() token.Pos {
	if len(x.Columns) > 0 {
		last := x.Columns[len(x.Columns)-1]
		if last.Type != nil {
			return last.Type.End()
		}
		return last.Expr.End()
	}
	return x.MvExpand + 9 // len("mv-expand")
}

// SearchOp represents a search operator.
type SearchOp struct {
	Pipe      token.Pos // Position of "|"
	Search    token.Pos // Position of "search"
	Predicate Expr      // Search predicate
}

func (x *SearchOp) Pos() token.Pos { return x.Pipe }
func (x *SearchOp) End() token.Pos { return x.Predicate.End() }

// ProjectRenameOp represents a project-rename operator.
type ProjectRenameOp struct {
	Pipe          token.Pos     // Position of "|"
	ProjectRename token.Pos     // Position of "project-rename"
	Columns       []*RenameExpr // Column renamings
}

func (x *ProjectRenameOp) Pos() token.Pos { return x.Pipe }

func (x *ProjectRenameOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.ProjectRename + 14 // len("project-rename")
}

// RenameExpr represents a column renaming (NewName = OldName).
type RenameExpr struct {
	NewName *Ident    // New column name
	Assign  token.Pos // Position of "="
	OldName *Ident    // Old column name
}

func (x *RenameExpr) Pos() token.Pos { return x.NewName.Pos() }
func (x *RenameExpr) End() token.Pos { return x.OldName.End() }

// ProjectReorderOp represents a project-reorder operator.
type ProjectReorderOp struct {
	Pipe           token.Pos // Position of "|"
	ProjectReorder token.Pos // Position of "project-reorder"
	Columns        []*Ident  // Column names in new order
}

func (x *ProjectReorderOp) Pos() token.Pos { return x.Pipe }

func (x *ProjectReorderOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.ProjectReorder + 15 // len("project-reorder")
}

// SampleOp represents a sample operator.
type SampleOp struct {
	Pipe   token.Pos // Position of "|"
	Sample token.Pos // Position of "sample"
	Count  Expr      // Number of rows to sample
}

func (x *SampleOp) Pos() token.Pos { return x.Pipe }
func (x *SampleOp) End() token.Pos { return x.Count.End() }

// SampleDistinctOp represents a sample-distinct operator.
type SampleDistinctOp struct {
	Pipe           token.Pos // Position of "|"
	SampleDistinct token.Pos // Position of "sample-distinct"
	Count          Expr      // Number of distinct values to sample
	OfPos          token.Pos // Position of "of"
	Column         Expr      // Column to sample from
}

func (x *SampleDistinctOp) Pos() token.Pos { return x.Pipe }
func (x *SampleDistinctOp) End() token.Pos { return x.Column.End() }

// LookupOp represents a lookup operator.
type LookupOp struct {
	Pipe   token.Pos // Position of "|"
	Lookup token.Pos // Position of "lookup"
	Kind   JoinKind  // Kind of lookup (inner, leftouter)
	Table  Expr      // Table to lookup from
	OnPos  token.Pos // Position of "on"
	OnExpr []Expr    // Match conditions
}

func (x *LookupOp) Pos() token.Pos { return x.Pipe }

func (x *LookupOp) End() token.Pos {
	if len(x.OnExpr) > 0 {
		return x.OnExpr[len(x.OnExpr)-1].End()
	}
	return x.Table.End()
}

// MakeSeriesOp represents a make-series operator.
// MakeSeriesAggregation represents an aggregation with optional default value.
type MakeSeriesAggregation struct {
	Expr       *NamedExpr // The aggregation expression (e.g., count(), avg(Value))
	DefaultPos token.Pos  // Position of "default" (NoPos if none)
	Default    Expr       // Default value for missing bins (nil if none)
}

type MakeSeriesOp struct {
	Pipe       token.Pos                // Position of "|"
	MakeSeries token.Pos                // Position of "make-series"
	Aggregates []*MakeSeriesAggregation // Aggregations with optional defaults
	OnPos      token.Pos                // Position of "on"
	OnColumn   Expr                     // Time column
	FromPos    token.Pos                // Position of "from" (NoPos if none)
	From       Expr                     // Start time (nil if none)
	ToPos      token.Pos                // Position of "to" (NoPos if none)
	To         Expr                     // End time (nil if none)
	StepPos    token.Pos                // Position of "step"
	Step       Expr                     // Step interval
	InRange    *InRangeExpr             // Optional in range(start, stop, step) - legacy
	ByPos      token.Pos                // Position of "by" (NoPos if none)
	GroupBy    []*NamedExpr             // Group by expressions
}

func (x *MakeSeriesOp) Pos() token.Pos { return x.Pipe }

func (x *MakeSeriesOp) End() token.Pos {
	if len(x.GroupBy) > 0 {
		return x.GroupBy[len(x.GroupBy)-1].End()
	}
	if x.InRange != nil {
		return x.InRange.End()
	}
	return x.OnColumn.End()
}

// InRangeExpr represents the "in range(start, stop, step)" clause.
type InRangeExpr struct {
	InPos  token.Pos // Position of "in"
	Range  token.Pos // Position of "range"
	Lparen token.Pos
	Start  Expr
	Stop   Expr
	Step   Expr
	Rparen token.Pos
}

func (x *InRangeExpr) Pos() token.Pos { return x.InPos }
func (x *InRangeExpr) End() token.Pos { return x.Rparen + 1 }

// AsOp represents an as operator (name a result).
type AsOp struct {
	Pipe   token.Pos        // Position of "|"
	As     token.Pos        // Position of "as"
	Params []*OperatorParam // Optional parameters (e.g., hint.materialized=true)
	Name   *Ident           // Result name
}

func (x *AsOp) Pos() token.Pos { return x.Pipe }
func (x *AsOp) End() token.Pos { return x.Name.End() }

// ConsumeOp represents a consume operator.
type ConsumeOp struct {
	Pipe    token.Pos        // Position of "|"
	Consume token.Pos        // Position of "consume"
	Params  []*OperatorParam // Optional parameters
}

func (x *ConsumeOp) Pos() token.Pos { return x.Pipe }
func (x *ConsumeOp) End() token.Pos {
	if len(x.Params) > 0 {
		return x.Params[len(x.Params)-1].Value.End()
	}
	return x.Consume + 7 // len("consume")
}

// GetSchemaOp represents a getschema operator.
type GetSchemaOp struct {
	Pipe      token.Pos // Position of "|"
	GetSchema token.Pos // Position of "getschema"
}

func (x *GetSchemaOp) Pos() token.Pos { return x.Pipe }
func (x *GetSchemaOp) End() token.Pos { return x.GetSchema + 9 } // len("getschema")

// SerializeOp represents a serialize operator.
type SerializeOp struct {
	Pipe      token.Pos    // Position of "|"
	Serialize token.Pos    // Position of "serialize"
	Columns   []*NamedExpr // Optional column expressions
}

func (x *SerializeOp) Pos() token.Pos { return x.Pipe }

func (x *SerializeOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.Serialize + 9 // len("serialize")
}

// InvokeOp represents an invoke operator (call a stored function).
type InvokeOp struct {
	Pipe     token.Pos // Position of "|"
	Invoke   token.Pos // Position of "invoke"
	Function *CallExpr // Function to invoke
}

func (x *InvokeOp) Pos() token.Pos { return x.Pipe }
func (x *InvokeOp) End() token.Pos { return x.Function.End() }

// ScanOp represents a scan operator.
type ScanOp struct {
	Pipe   token.Pos // Position of "|"
	Scan   token.Pos // Position of "scan"
	With   token.Pos // Position of "with"
	Steps  []Expr    // Scan steps
	EndPos token.Pos
}

func (x *ScanOp) Pos() token.Pos { return x.Pipe }
func (x *ScanOp) End() token.Pos { return x.EndPos }

// EvaluateOp represents an evaluate operator (plugin invocation).
type EvaluateOp struct {
	Pipe     token.Pos // Position of "|"
	Evaluate token.Pos // Position of "evaluate"
	Plugin   *CallExpr // Plugin call
}

func (x *EvaluateOp) Pos() token.Pos { return x.Pipe }
func (x *EvaluateOp) End() token.Pos { return x.Plugin.End() }

// ReduceOp represents a reduce operator.
type ReduceOp struct {
	Pipe   token.Pos // Position of "|"
	Reduce token.Pos // Position of "reduce"
	ByPos  token.Pos // Position of "by"
	Column Expr      // Column to reduce
}

func (x *ReduceOp) Pos() token.Pos { return x.Pipe }
func (x *ReduceOp) End() token.Pos { return x.Column.End() }

// ForkOp represents a fork operator.
type ForkOp struct {
	Pipe   token.Pos    // Position of "|"
	Fork   token.Pos    // Position of "fork"
	Prongs []*ForkProng // Fork branches
	EndPos token.Pos
}

func (x *ForkOp) Pos() token.Pos { return x.Pipe }
func (x *ForkOp) End() token.Pos { return x.EndPos }

// ForkProng represents one branch of a fork.
type ForkProng struct {
	Name   *Ident // Optional name
	Lparen token.Pos
	Query  *PipeExpr // Query for this branch
	Rparen token.Pos
}

func (x *ForkProng) Pos() token.Pos { return x.Lparen }
func (x *ForkProng) End() token.Pos { return x.Rparen + 1 }

// FacetOp represents a facet operator.
type FacetOp struct {
	Pipe    token.Pos // Position of "|"
	Facet   token.Pos // Position of "facet"
	ByPos   token.Pos // Position of "by"
	Columns []*Ident  // Columns to facet by
	With    token.Pos // Position of "with" (NoPos if none)
	Query   *PipeExpr // Optional with query
}

func (x *FacetOp) Pos() token.Pos { return x.Pipe }

func (x *FacetOp) End() token.Pos {
	if x.Query != nil {
		return x.Query.End()
	}
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.ByPos + 2
}

// ProjectKeepOp represents a project-keep operator.
type ProjectKeepOp struct {
	Pipe        token.Pos // Position of "|"
	ProjectKeep token.Pos // Position of "project-keep"
	Columns     []*Ident  // Columns to keep
}

func (x *ProjectKeepOp) Pos() token.Pos { return x.Pipe }

func (x *ProjectKeepOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.ProjectKeep + 12 // len("project-keep")
}

// TopNestedOp represents a top-nested operator.
type TopNestedOp struct {
	Pipe      token.Pos // Position of "|"
	TopNested token.Pos // Position of "top-nested"
	Clauses   []*TopNestedClause
	EndPos    token.Pos
}

func (x *TopNestedOp) Pos() token.Pos { return x.Pipe }
func (x *TopNestedOp) End() token.Pos { return x.EndPos }

// TopNestedClause represents one level in top-nested.
type TopNestedClause struct {
	Count  Expr      // Number of top values
	OfPos  token.Pos // Position of "of"
	Column Expr      // Column expression
	ByPos  token.Pos // Position of "by"
	ByExpr Expr      // Order by expression
	With   token.Pos // Position of "with" (optional)
	Others Expr      // "with others" expression (optional)
}

func (x *TopNestedClause) Pos() token.Pos { return x.Count.Pos() }
func (x *TopNestedClause) End() token.Pos {
	if x.Others != nil {
		return x.Others.End()
	}
	return x.ByExpr.End()
}

// TopHittersOp represents a top-hitters operator.
type TopHittersOp struct {
	Pipe       token.Pos // Position of "|"
	TopHitters token.Pos // Position of "top-hitters"
	Count      Expr      // Number of top hitters
	OfPos      token.Pos // Position of "of"
	Column     Expr      // Column to analyze
	ByPos      token.Pos // Position of "by" (optional)
	ByExpr     Expr      // Weight expression (optional)
}

func (x *TopHittersOp) Pos() token.Pos { return x.Pipe }

func (x *TopHittersOp) End() token.Pos {
	if x.ByExpr != nil {
		return x.ByExpr.End()
	}
	return x.Column.End()
}

// MvApplyColumn represents a column in mv-apply with optional name and type.
type MvApplyColumn struct {
	Name   *Ident    // Optional name for the column
	Assign token.Pos // Position of "=" (NoPos if unnamed)
	Expr   Expr      // Column expression
	ToPos  token.Pos // Position of "to" (NoPos if no type)
	Type   Expr      // Type annotation (nil if none)
}

func (x *MvApplyColumn) Pos() token.Pos {
	if x.Name != nil {
		return x.Name.Pos()
	}
	return x.Expr.Pos()
}

func (x *MvApplyColumn) End() token.Pos {
	if x.Type != nil {
		return x.Type.End()
	}
	return x.Expr.End()
}

// MvApplyOp represents an mv-apply operator.
type MvApplyOp struct {
	Pipe     token.Pos        // Position of "|"
	MvApply  token.Pos        // Position of "mv-apply"
	Params   []*OperatorParam // Parameters (context_id, id_column)
	LimitPos token.Pos        // Position of "limit" (NoPos if none)
	Limit    Expr             // Limit expression (nil if none)
	Items    []*MvApplyColumn // Items to apply
	OnPos    token.Pos        // Position of "on"
	OnExpr   *PipeExpr        // Subquery to apply
}

func (x *MvApplyOp) Pos() token.Pos { return x.Pipe }
func (x *MvApplyOp) End() token.Pos {
	if x.OnExpr != nil {
		return x.OnExpr.End()
	}
	if len(x.Items) > 0 {
		return x.Items[len(x.Items)-1].End()
	}
	return x.MvApply + 8 // len("mv-apply")
}

// FindOp represents a find operator.
type FindOp struct {
	Pipe       token.Pos // Position of "|"
	Find       token.Pos // Position of "find"
	InPos      token.Pos // Position of "in" (optional)
	Tables     []Expr    // Tables to search (optional)
	WherePos   token.Pos // Position of "where"
	Predicate  Expr      // Search predicate
	ProjectPos token.Pos // Position of "project" (optional)
	Columns    []Expr    // Projected columns (optional)
}

func (x *FindOp) Pos() token.Pos { return x.Pipe }

func (x *FindOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.Predicate.End()
}

// PrintStmt represents a print statement (not a pipe operator).
type PrintStmt struct {
	Print   token.Pos    // Position of "print"
	Columns []*NamedExpr // Values to print
}

func (x *PrintStmt) Pos() token.Pos { return x.Print }

func (x *PrintStmt) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.Print + 5 // len("print")
}

// RangeStmt represents a range statement.
type RangeStmt struct {
	Range  token.Pos // Position of "range"
	Column *Ident    // Column name
	From   Expr      // Start value
	To     Expr      // End value
	Step   Expr      // Step value
}

func (x *RangeStmt) Pos() token.Pos { return x.Range }
func (x *RangeStmt) End() token.Pos { return x.Step.End() }

// DatatableStmt represents a datatable statement.
type DatatableStmt struct {
	Datatable token.Pos         // Position of "datatable"
	Columns   []*ColumnDeclExpr // Column declarations
	Values    []Expr            // Row values
	EndPos    token.Pos
}

func (x *DatatableStmt) Pos() token.Pos { return x.Datatable }
func (x *DatatableStmt) End() token.Pos { return x.EndPos }

// ColumnDeclExpr represents a column declaration (name:type).
type ColumnDeclExpr struct {
	Name  *Ident    // Column name
	Colon token.Pos // Position of ":"
	Type  *Ident    // Type name
}

func (x *ColumnDeclExpr) Pos() token.Pos { return x.Name.Pos() }
func (x *ColumnDeclExpr) End() token.Pos { return x.Type.End() }

// ExternalDataOp represents an externaldata operator.
type ExternalDataOp struct {
	ExternalData token.Pos         // Position of "externaldata"
	Columns      []*ColumnDeclExpr // Column declarations
	URIs         []Expr            // Data source URIs
	WithPos      token.Pos         // Position of "with" (optional)
	Properties   []Expr            // Properties (optional)
	EndPos       token.Pos
}

func (x *ExternalDataOp) Pos() token.Pos { return x.ExternalData }
func (x *ExternalDataOp) End() token.Pos { return x.EndPos }

// GenericOp represents an unrecognized or less common operator.
// ============================================================================
// Graph Operators
// ============================================================================

// MakeGraphOp represents a make-graph operator.
// Syntax: make-graph SourceColumn --> TargetColumn [with ...] [partitionedby ...]
type MakeGraphOp struct {
	Pipe         token.Pos        // Position of "|"
	MakeGraph    token.Pos        // Position of "make-graph"
	Params       []*OperatorParam // Optional parameters
	SourceColumn Expr             // Source node column
	Direction    token.Pos        // Position of "-->" or "--"
	Directed     bool             // true for "-->" (directed), false for "--" (undirected)
	TargetColumn Expr             // Target node column
	WithClause   *MakeGraphWith   // Optional with clause
}

func (x *MakeGraphOp) Pos() token.Pos { return x.Pipe }
func (x *MakeGraphOp) End() token.Pos {
	if x.WithClause != nil {
		return x.WithClause.End()
	}
	return x.TargetColumn.End()
}

// MakeGraphWith represents the with clause of make-graph.
type MakeGraphWith struct {
	WithPos token.Pos // Position of "with"
	NodeId  *Ident    // Node ID column name (for with_node_id=name)
	Table   Expr      // Table expression (for with Table on Column)
	OnPos   token.Pos // Position of "on"
	OnCol   Expr      // On column
}

func (x *MakeGraphWith) Pos() token.Pos { return x.WithPos }
func (x *MakeGraphWith) End() token.Pos {
	if x.OnCol != nil {
		return x.OnCol.End()
	}
	if x.NodeId != nil {
		return x.NodeId.End()
	}
	return x.WithPos
}

// GraphMatchOp represents a graph-match operator.
// Syntax: graph-match (a)-[e]->(b) [where ...] [project ...]
type GraphMatchOp struct {
	Pipe       token.Pos            // Position of "|"
	GraphMatch token.Pos            // Position of "graph-match"
	Params     []*OperatorParam     // Optional parameters
	Patterns   []*GraphMatchPattern // Pattern elements
	Where      *WhereClause         // Optional where clause
	Project    *ProjectClause       // Optional project clause
}

func (x *GraphMatchOp) Pos() token.Pos { return x.Pipe }
func (x *GraphMatchOp) End() token.Pos {
	if x.Project != nil {
		return x.Project.End()
	}
	if x.Where != nil {
		return x.Where.End()
	}
	if len(x.Patterns) > 0 {
		return x.Patterns[len(x.Patterns)-1].End()
	}
	return x.GraphMatch
}

// GraphMatchPattern represents a single pattern in graph-match.
type GraphMatchPattern struct {
	Elements []GraphPatternElement // Alternating nodes and edges
}

func (x *GraphMatchPattern) Pos() token.Pos {
	if len(x.Elements) > 0 {
		return x.Elements[0].Pos()
	}
	return token.NoPos
}
func (x *GraphMatchPattern) End() token.Pos {
	if len(x.Elements) > 0 {
		return x.Elements[len(x.Elements)-1].End()
	}
	return token.NoPos
}

// GraphPatternElement is an interface for graph pattern elements.
type GraphPatternElement interface {
	Node
	graphPatternElement()
}

// GraphPatternNode represents a node in a graph pattern: (name)
type GraphPatternNode struct {
	Lparen token.Pos // Position of "("
	Name   *Ident    // Node variable name
	Rparen token.Pos // Position of ")"
}

func (x *GraphPatternNode) Pos() token.Pos       { return x.Lparen }
func (x *GraphPatternNode) End() token.Pos       { return x.Rparen + 1 }
func (x *GraphPatternNode) graphPatternElement() {}

// GraphPatternEdge represents an edge in a graph pattern: -[e]-> or --> or <-- or --
type GraphPatternEdge struct {
	Start     token.Pos  // Position of first token (- or <)
	Lbracket  token.Pos  // Position of "[" (NoPos if unnamed)
	Name      *Ident     // Edge variable name (nil if unnamed)
	RangeExpr *EdgeRange // Optional range *min..max (nil if none)
	Rbracket  token.Pos  // Position of "]" (NoPos if unnamed)
	End_      token.Pos  // Position of last token (> or -)
	Direction int        // 1 = -->, -1 = <--, 0 = --
}

func (x *GraphPatternEdge) Pos() token.Pos       { return x.Start }
func (x *GraphPatternEdge) End() token.Pos       { return x.End_ + 1 }
func (x *GraphPatternEdge) graphPatternElement() {}

// EdgeRange represents a range constraint on an edge: *1..5
type EdgeRange struct {
	Star   token.Pos // Position of "*"
	MinVal Expr      // Minimum (may be nil)
	DotDot token.Pos // Position of ".."
	MaxVal Expr      // Maximum (may be nil)
}

func (x *EdgeRange) Pos() token.Pos { return x.Star }
func (x *EdgeRange) End() token.Pos {
	if x.MaxVal != nil {
		return x.MaxVal.End()
	}
	return x.DotDot + 2
}

// WhereClause represents a where clause in graph operators.
type WhereClause struct {
	Where     token.Pos // Position of "where"
	Predicate Expr      // Where predicate
}

func (x *WhereClause) Pos() token.Pos { return x.Where }
func (x *WhereClause) End() token.Pos { return x.Predicate.End() }

// ProjectClause represents a project clause in graph operators.
type ProjectClause struct {
	Project token.Pos    // Position of "project"
	Columns []*NamedExpr // Projected columns
}

func (x *ProjectClause) Pos() token.Pos { return x.Project }
func (x *ProjectClause) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
	}
	return x.Project
}

// GraphShortestPathsOp represents a graph-shortest-paths operator.
type GraphShortestPathsOp struct {
	Pipe              token.Pos            // Position of "|"
	GraphShortestPath token.Pos            // Position of "graph-shortest-paths"
	Params            []*OperatorParam     // Optional parameters
	Patterns          []*GraphMatchPattern // Pattern elements
	Where             *WhereClause         // Optional where clause
	Project           *ProjectClause       // Optional project clause
}

func (x *GraphShortestPathsOp) Pos() token.Pos { return x.Pipe }
func (x *GraphShortestPathsOp) End() token.Pos {
	if x.Project != nil {
		return x.Project.End()
	}
	if x.Where != nil {
		return x.Where.End()
	}
	if len(x.Patterns) > 0 {
		return x.Patterns[len(x.Patterns)-1].End()
	}
	return x.GraphShortestPath
}

// GraphMarkComponentsOp represents a graph-mark-components operator.
type GraphMarkComponentsOp struct {
	Pipe                token.Pos        // Position of "|"
	GraphMarkComponents token.Pos        // Position of "graph-mark-components"
	Params              []*OperatorParam // Optional parameters
}

func (x *GraphMarkComponentsOp) Pos() token.Pos { return x.Pipe }
func (x *GraphMarkComponentsOp) End() token.Pos {
	if len(x.Params) > 0 {
		return x.Params[len(x.Params)-1].Value.End()
	}
	return x.GraphMarkComponents + 22 // len("graph-mark-components")
}

// GraphToTableOp represents a graph-to-table operator.
type GraphToTableOp struct {
	Pipe         token.Pos             // Position of "|"
	GraphToTable token.Pos             // Position of "graph-to-table"
	Outputs      []*GraphToTableOutput // Output clauses (nodes/edges)
}

func (x *GraphToTableOp) Pos() token.Pos { return x.Pipe }
func (x *GraphToTableOp) End() token.Pos {
	if len(x.Outputs) > 0 {
		return x.Outputs[len(x.Outputs)-1].End()
	}
	return x.GraphToTable + 14 // len("graph-to-table")
}

// GraphToTableOutput represents a nodes or edges output clause.
type GraphToTableOutput struct {
	Keyword token.Pos        // Position of "nodes" or "edges"
	IsNodes bool             // true for nodes, false for edges
	AsName  *Ident           // Optional "as Name"
	Params  []*OperatorParam // Parameters
}

func (x *GraphToTableOutput) Pos() token.Pos { return x.Keyword }
func (x *GraphToTableOutput) End() token.Pos {
	if len(x.Params) > 0 {
		return x.Params[len(x.Params)-1].Value.End()
	}
	if x.AsName != nil {
		return x.AsName.End()
	}
	return x.Keyword + 5 // approximate
}

// GraphWhereNodesOp represents a graph-where-nodes operator.
type GraphWhereNodesOp struct {
	Pipe            token.Pos // Position of "|"
	GraphWhereNodes token.Pos // Position of "graph-where-nodes"
	Predicate       Expr      // Filter predicate
}

func (x *GraphWhereNodesOp) Pos() token.Pos { return x.Pipe }
func (x *GraphWhereNodesOp) End() token.Pos { return x.Predicate.End() }

// GraphWhereEdgesOp represents a graph-where-edges operator.
type GraphWhereEdgesOp struct {
	Pipe            token.Pos // Position of "|"
	GraphWhereEdges token.Pos // Position of "graph-where-edges"
	Predicate       Expr      // Filter predicate
}

func (x *GraphWhereEdgesOp) Pos() token.Pos { return x.Pipe }
func (x *GraphWhereEdgesOp) End() token.Pos { return x.Predicate.End() }

// ============================================================================
// Additional Operators
// ============================================================================

// ExecuteAndCacheOp represents an execute-and-cache operator.
type ExecuteAndCacheOp struct {
	Pipe            token.Pos // Position of "|"
	ExecuteAndCache token.Pos // Position of "execute-and-cache"
}

func (x *ExecuteAndCacheOp) Pos() token.Pos { return x.Pipe }
func (x *ExecuteAndCacheOp) End() token.Pos { return x.ExecuteAndCache + 17 } // len("execute-and-cache")

// AssertSchemaOp represents an assert-schema operator.
type AssertSchemaOp struct {
	Pipe         token.Pos // Position of "|"
	AssertSchema token.Pos // Position of "assert-schema"
	Lparen       token.Pos // Position of "("
	Columns      []*ColumnDeclExpr // Schema columns
	Rparen       token.Pos // Position of ")"
}

func (x *AssertSchemaOp) Pos() token.Pos { return x.Pipe }
func (x *AssertSchemaOp) End() token.Pos { return x.Rparen + 1 }

// MacroExpandOp represents a macro-expand operator.
type MacroExpandOp struct {
	Pipe        token.Pos        // Position of "|"
	MacroExpand token.Pos        // Position of "macro-expand"
	Params      []*OperatorParam // Optional parameters
	EntityGroup Expr             // Entity group expression
	AsPos       token.Pos        // Position of "as"
	ScopeName   *Ident           // Scope name
	Lparen      token.Pos        // Position of "("
	Statements  []Stmt           // Statements in the macro body
	Rparen      token.Pos        // Position of ")"
}

func (x *MacroExpandOp) Pos() token.Pos { return x.Pipe }
func (x *MacroExpandOp) End() token.Pos { return x.Rparen + 1 }

// PartitionByOp represents a partition-by operator (internal __partitionby).
type PartitionByOp struct {
	Pipe        token.Pos        // Position of "|"
	PartitionBy token.Pos        // Position of "__partitionby" or "partition-by"
	Params      []*OperatorParam // Optional parameters
	Column      Expr             // Column to partition by
	IdColumn    *Ident           // Optional ID column name
	Lparen      token.Pos        // Position of "("
	SubExpr     *PipeExpr        // Subexpression
	Rparen      token.Pos        // Position of ")"
}

func (x *PartitionByOp) Pos() token.Pos { return x.Pipe }
func (x *PartitionByOp) End() token.Pos { return x.Rparen + 1 }

// ============================================================================
// Generic/Fallback Operator
// ============================================================================

// This allows parsing to continue even with operators we don't fully support.
type GenericOp struct {
	Pipe    token.Pos // Position of "|"
	OpPos   token.Pos // Position of operator keyword
	OpName  string    // Operator name
	Content []Expr    // Raw content expressions
	EndPos  token.Pos // End position
}

func (x *GenericOp) Pos() token.Pos { return x.Pipe }
func (x *GenericOp) End() token.Pos { return x.EndPos }
