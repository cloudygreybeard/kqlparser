package ast

// Visitor defines the interface for AST visitors.
// If Visit returns nil, the children of the node are not visited.
type Visitor interface {
	Visit(node Node) Visitor
}

// Walk traverses an AST in depth-first order.
// It starts by calling v.Visit(node); if the visitor returned by Visit is not nil,
// Walk calls itself recursively for each of the node's children, followed by a call to v.Visit(nil).
func Walk(v Visitor, node Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	// Expressions
	case *Ident:
		// No children

	case *BadExpr:
		// No children

	case *BasicLit:
		// No children

	case *ParenExpr:
		Walk(v, n.X)

	case *UnaryExpr:
		Walk(v, n.X)

	case *BinaryExpr:
		Walk(v, n.X)
		Walk(v, n.Y)

	case *CallExpr:
		Walk(v, n.Fun)
		for _, arg := range n.Args {
			Walk(v, arg)
		}

	case *IndexExpr:
		Walk(v, n.X)
		Walk(v, n.Index)

	case *SelectorExpr:
		Walk(v, n.X)
		Walk(v, n.Sel)

	case *ListExpr:
		for _, elem := range n.Elems {
			Walk(v, elem)
		}

	case *BetweenExpr:
		Walk(v, n.X)
		Walk(v, n.Low)
		Walk(v, n.High)

	case *DynamicLit:
		if n.Value != nil {
			Walk(v, n.Value)
		}

	case *StarExpr:
		// No children

	case *NamedExpr:
		if n.Name != nil {
			Walk(v, n.Name)
		}
		Walk(v, n.Expr)

	case *PipeExpr:
		Walk(v, n.Source)
		for _, op := range n.Operators {
			Walk(v, op)
		}

	// Statements
	case *LetStmt:
		Walk(v, n.Name)
		Walk(v, n.Value)

	case *ExprStmt:
		Walk(v, n.X)

	case *QueryStmt:
		for _, stmt := range n.Stmts {
			Walk(v, stmt)
		}

	case *Script:
		for _, stmt := range n.Stmts {
			Walk(v, stmt)
		}

	// Operators
	case *WhereOp:
		Walk(v, n.Predicate)

	case *ProjectOp:
		for _, col := range n.Columns {
			Walk(v, col)
		}

	case *ProjectAwayOp:
		for _, col := range n.Columns {
			Walk(v, col)
		}

	case *ExtendOp:
		for _, col := range n.Columns {
			Walk(v, col)
		}

	case *SummarizeOp:
		for _, agg := range n.Aggregates {
			Walk(v, agg)
		}
		for _, grp := range n.GroupBy {
			Walk(v, grp)
		}

	case *SortOp:
		for _, ord := range n.Orders {
			Walk(v, ord.Expr)
		}

	case *TakeOp:
		Walk(v, n.Count)

	case *TopOp:
		Walk(v, n.Count)
		Walk(v, n.ByExpr.Expr)

	case *CountOp:
		// No children

	case *DistinctOp:
		for _, col := range n.Columns {
			Walk(v, col)
		}

	case *JoinOp:
		Walk(v, n.Right)
		for _, cond := range n.OnExpr {
			Walk(v, cond)
		}

	case *UnionOp:
		for _, tbl := range n.Tables {
			Walk(v, tbl)
		}

	case *RenderOp:
		Walk(v, n.ChartType)

	case *ParseOp:
		Walk(v, n.Source)
		Walk(v, n.Pattern)

	case *MvExpandOp:
		for _, col := range n.Columns {
			Walk(v, col)
		}

	case *SearchOp:
		Walk(v, n.Predicate)

	case *GenericOp:
		for _, content := range n.Content {
			Walk(v, content)
		}
	}

	v.Visit(nil)
}

// Inspect traverses an AST and calls f for each node.
// If f returns true, Inspect continues traversing the children.
// If f returns false, Inspect skips the children.
func Inspect(node Node, f func(Node) bool) {
	Walk(inspector(f), node)
}

type inspector func(Node) bool

func (f inspector) Visit(node Node) Visitor {
	if node == nil {
		return nil
	}
	if f(node) {
		return f
	}
	return nil
}
