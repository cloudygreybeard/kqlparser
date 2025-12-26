package ast

import (
	"github.com/cloudygreybeard/kqlparser/token"
)

// LetStmt represents a let statement (let name = expr).
type LetStmt struct {
	Let    token.Pos // Position of "let"
	Name   *Ident    // Variable name
	Assign token.Pos // Position of "="
	Value  Expr      // Variable value (expression or function)
}

func (x *LetStmt) Pos() token.Pos { return x.Let }
func (x *LetStmt) End() token.Pos { return x.Value.End() }

// ExprStmt represents a statement consisting of a single expression.
type ExprStmt struct {
	X Expr // Expression
}

func (x *ExprStmt) Pos() token.Pos { return x.X.Pos() }
func (x *ExprStmt) End() token.Pos { return x.X.End() }

// QueryStmt represents a query statement, which may have multiple statements.
type QueryStmt struct {
	Stmts []Stmt // Statements separated by semicolons
}

func (x *QueryStmt) Pos() token.Pos {
	if len(x.Stmts) > 0 {
		return x.Stmts[0].Pos()
	}
	return token.NoPos
}

func (x *QueryStmt) End() token.Pos {
	if len(x.Stmts) > 0 {
		return x.Stmts[len(x.Stmts)-1].End()
	}
	return token.NoPos
}

// Script represents an entire KQL script (multiple statements).
type Script struct {
	Stmts []Stmt // All statements in the script
}

func (x *Script) Pos() token.Pos {
	if len(x.Stmts) > 0 {
		return x.Stmts[0].Pos()
	}
	return token.NoPos
}

func (x *Script) End() token.Pos {
	if len(x.Stmts) > 0 {
		return x.Stmts[len(x.Stmts)-1].End()
	}
	return token.NoPos
}
