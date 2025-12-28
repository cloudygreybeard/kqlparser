package ast

import (
	"github.com/cloudygreybeard/kqlparser/token"
)

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
	Pipe       token.Pos    // Position of "|"
	Summarize  token.Pos    // Position of "summarize"
	Aggregates []*NamedExpr // Aggregate expressions
	ByPos      token.Pos    // Position of "by" (NoPos if no by clause)
	GroupBy    []*NamedExpr // Group by expressions
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
	Pipe   token.Pos    // Position of "|"
	Sort   token.Pos    // Position of "sort" or "order"
	ByPos  token.Pos    // Position of "by"
	Orders []*OrderExpr // Ordered expressions
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
	Pipe   token.Pos // Position of "|"
	Join   token.Pos // Position of "join"
	Kind   JoinKind  // Join kind
	Right  Expr      // Right side expression
	OnPos  token.Pos // Position of "on"
	OnExpr []Expr    // Join conditions
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
	Pipe   token.Pos // Position of "|"
	Union  token.Pos // Position of "union"
	Tables []Expr    // Tables to union
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
	Pipe      token.Pos // Position of "|"
	Render    token.Pos // Position of "render"
	ChartType *Ident    // Chart type (table, piechart, barchart, etc.)
	// TODO: with clause properties
}

func (x *RenderOp) Pos() token.Pos { return x.Pipe }
func (x *RenderOp) End() token.Pos { return x.ChartType.End() }

// ParseOp represents a parse operator.
type ParseOp struct {
	Pipe    token.Pos // Position of "|"
	Parse   token.Pos // Position of "parse"
	Source  Expr      // Source expression to parse
	WithPos token.Pos // Position of "with"
	Pattern Expr      // Parse pattern
}

func (x *ParseOp) Pos() token.Pos { return x.Pipe }
func (x *ParseOp) End() token.Pos { return x.Pattern.End() }

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
type MvExpandOp struct {
	Pipe     token.Pos // Position of "|"
	MvExpand token.Pos // Position of "mv-expand"
	Columns  []Expr    // Columns to expand
}

func (x *MvExpandOp) Pos() token.Pos { return x.Pipe }

func (x *MvExpandOp) End() token.Pos {
	if len(x.Columns) > 0 {
		return x.Columns[len(x.Columns)-1].End()
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
type MakeSeriesOp struct {
	Pipe       token.Pos    // Position of "|"
	MakeSeries token.Pos    // Position of "make-series"
	Aggregates []*NamedExpr // Aggregations (e.g., count())
	OnPos      token.Pos    // Position of "on"
	OnColumn   Expr         // Time column
	InRange    *InRangeExpr // Optional in range(start, stop, step)
	ByPos      token.Pos    // Position of "by" (NoPos if none)
	GroupBy    []*NamedExpr // Group by expressions
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
	Pipe token.Pos // Position of "|"
	As   token.Pos // Position of "as"
	Name *Ident    // Result name
}

func (x *AsOp) Pos() token.Pos { return x.Pipe }
func (x *AsOp) End() token.Pos { return x.Name.End() }

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

// ConsumeOp represents a consume operator.
type ConsumeOp struct {
	Pipe    token.Pos // Position of "|"
	Consume token.Pos // Position of "consume"
}

func (x *ConsumeOp) Pos() token.Pos { return x.Pipe }
func (x *ConsumeOp) End() token.Pos { return x.Consume + 7 } // len("consume")

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
	Pipe      token.Pos      // Position of "|"
	TopNested token.Pos      // Position of "top-nested"
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

// MvApplyOp represents an mv-apply operator.
type MvApplyOp struct {
	Pipe    token.Pos    // Position of "|"
	MvApply token.Pos    // Position of "mv-apply"
	Items   []*NamedExpr // Items to apply (name = expr or just expr)
	OnPos   token.Pos    // Position of "on"
	OnExpr  *PipeExpr    // Subquery to apply
}

func (x *MvApplyOp) Pos() token.Pos { return x.Pipe }
func (x *MvApplyOp) End() token.Pos { return x.OnExpr.End() }

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
