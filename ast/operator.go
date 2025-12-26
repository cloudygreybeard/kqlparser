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
