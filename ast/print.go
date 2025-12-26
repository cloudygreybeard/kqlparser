package ast

import (
	"fmt"
	"io"
	"strings"
)

// Fprint "pretty prints" an AST node to w.
func Fprint(w io.Writer, node Node) error {
	p := &printer{w: w}
	p.print(node, 0)
	return p.err
}

// Print returns a string representation of an AST node.
func Print(node Node) string {
	var b strings.Builder
	Fprint(&b, node)
	return b.String()
}

type printer struct {
	w   io.Writer
	err error
}

func (p *printer) printf(format string, args ...any) {
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintf(p.w, format, args...)
}

func (p *printer) indent(level int) {
	for i := 0; i < level; i++ {
		p.printf("  ")
	}
}

func (p *printer) print(node Node, level int) {
	if node == nil {
		p.indent(level)
		p.printf("nil\n")
		return
	}

	p.indent(level)

	switch n := node.(type) {
	case *Ident:
		p.printf("Ident{Name: %q}\n", n.Name)

	case *BadExpr:
		p.printf("BadExpr{}\n")

	case *BasicLit:
		p.printf("BasicLit{Kind: %v, Value: %q}\n", n.Kind, n.Value)

	case *ParenExpr:
		p.printf("ParenExpr{\n")
		p.print(n.X, level+1)
		p.indent(level)
		p.printf("}\n")

	case *UnaryExpr:
		p.printf("UnaryExpr{Op: %v\n", n.Op)
		p.print(n.X, level+1)
		p.indent(level)
		p.printf("}\n")

	case *BinaryExpr:
		p.printf("BinaryExpr{Op: %v\n", n.Op)
		p.print(n.X, level+1)
		p.print(n.Y, level+1)
		p.indent(level)
		p.printf("}\n")

	case *CallExpr:
		p.printf("CallExpr{\n")
		p.indent(level + 1)
		p.printf("Fun:\n")
		p.print(n.Fun, level+2)
		if len(n.Args) > 0 {
			p.indent(level + 1)
			p.printf("Args:\n")
			for _, arg := range n.Args {
				p.print(arg, level+2)
			}
		}
		p.indent(level)
		p.printf("}\n")

	case *IndexExpr:
		p.printf("IndexExpr{\n")
		p.print(n.X, level+1)
		p.print(n.Index, level+1)
		p.indent(level)
		p.printf("}\n")

	case *SelectorExpr:
		p.printf("SelectorExpr{\n")
		p.print(n.X, level+1)
		p.print(n.Sel, level+1)
		p.indent(level)
		p.printf("}\n")

	case *StarExpr:
		p.printf("StarExpr{}\n")

	case *NamedExpr:
		if n.Name != nil {
			p.printf("NamedExpr{Name: %q\n", n.Name.Name)
		} else {
			p.printf("NamedExpr{\n")
		}
		p.print(n.Expr, level+1)
		p.indent(level)
		p.printf("}\n")

	case *PipeExpr:
		p.printf("PipeExpr{\n")
		p.indent(level + 1)
		p.printf("Source:\n")
		p.print(n.Source, level+2)
		if len(n.Operators) > 0 {
			p.indent(level + 1)
			p.printf("Operators:\n")
			for _, op := range n.Operators {
				p.print(op, level+2)
			}
		}
		p.indent(level)
		p.printf("}\n")

	case *WhereOp:
		p.printf("WhereOp{\n")
		p.print(n.Predicate, level+1)
		p.indent(level)
		p.printf("}\n")

	case *ProjectOp:
		p.printf("ProjectOp{\n")
		for _, col := range n.Columns {
			p.print(col, level+1)
		}
		p.indent(level)
		p.printf("}\n")

	case *ExtendOp:
		p.printf("ExtendOp{\n")
		for _, col := range n.Columns {
			p.print(col, level+1)
		}
		p.indent(level)
		p.printf("}\n")

	case *SummarizeOp:
		p.printf("SummarizeOp{\n")
		if len(n.Aggregates) > 0 {
			p.indent(level + 1)
			p.printf("Aggregates:\n")
			for _, agg := range n.Aggregates {
				p.print(agg, level+2)
			}
		}
		if len(n.GroupBy) > 0 {
			p.indent(level + 1)
			p.printf("GroupBy:\n")
			for _, grp := range n.GroupBy {
				p.print(grp, level+2)
			}
		}
		p.indent(level)
		p.printf("}\n")

	case *SortOp:
		p.printf("SortOp{\n")
		for _, ord := range n.Orders {
			p.print(ord.Expr, level+1)
		}
		p.indent(level)
		p.printf("}\n")

	case *TakeOp:
		p.printf("TakeOp{\n")
		p.print(n.Count, level+1)
		p.indent(level)
		p.printf("}\n")

	case *TopOp:
		p.printf("TopOp{\n")
		p.print(n.Count, level+1)
		p.print(n.ByExpr.Expr, level+1)
		p.indent(level)
		p.printf("}\n")

	case *CountOp:
		p.printf("CountOp{}\n")

	case *LetStmt:
		p.printf("LetStmt{Name: %q\n", n.Name.Name)
		p.print(n.Value, level+1)
		p.indent(level)
		p.printf("}\n")

	case *ExprStmt:
		p.printf("ExprStmt{\n")
		p.print(n.X, level+1)
		p.indent(level)
		p.printf("}\n")

	case *Script:
		p.printf("Script{\n")
		for _, stmt := range n.Stmts {
			p.print(stmt, level+1)
		}
		p.indent(level)
		p.printf("}\n")

	default:
		p.printf("%T{...}\n", node)
	}
}
