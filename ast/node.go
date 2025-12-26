// Package ast declares the types used to represent KQL abstract syntax trees.
package ast

import (
	"github.com/cloudygreybeard/kqlparser/token"
)

// Node is the interface implemented by all AST nodes.
type Node interface {
	Pos() token.Pos // Position of first character belonging to the node
	End() token.Pos // Position of first character immediately after the node
	node()          // Marker method to restrict implementations to this package
}

// Expr is the interface for all expression nodes.
type Expr interface {
	Node
	expr()
}

// Stmt is the interface for all statement nodes.
type Stmt interface {
	Node
	stmt()
}

// Operator is the interface for query operator nodes (where, project, etc.).
type Operator interface {
	Node
	operator()
}

// All node types implement Node
func (*Ident) node()         {}
func (*BadExpr) node()       {}
func (*BasicLit) node()      {}
func (*ParenExpr) node()     {}
func (*UnaryExpr) node()     {}
func (*BinaryExpr) node()    {}
func (*CallExpr) node()      {}
func (*IndexExpr) node()     {}
func (*SelectorExpr) node()  {}
func (*ListExpr) node()      {}
func (*BetweenExpr) node()   {}
func (*DynamicLit) node()    {}
func (*StarExpr) node()      {}
func (*NamedExpr) node()     {}
func (*PipeExpr) node()      {}
func (*LetStmt) node()       {}
func (*ExprStmt) node()      {}
func (*QueryStmt) node()     {}
func (*WhereOp) node()       {}
func (*ProjectOp) node()     {}
func (*ProjectAwayOp) node() {}
func (*ExtendOp) node()      {}
func (*SummarizeOp) node()   {}
func (*SortOp) node()        {}
func (*TakeOp) node()        {}
func (*TopOp) node()         {}
func (*CountOp) node()       {}
func (*DistinctOp) node()    {}
func (*JoinOp) node()        {}
func (*UnionOp) node()       {}
func (*RenderOp) node()      {}
func (*ParseOp) node()       {}
func (*MvExpandOp) node()    {}
func (*SearchOp) node()      {}
func (*GenericOp) node()     {}
func (*Script) node()        {}

// Expression nodes implement Expr
func (*Ident) expr()        {}
func (*BadExpr) expr()      {}
func (*BasicLit) expr()     {}
func (*ParenExpr) expr()    {}
func (*UnaryExpr) expr()    {}
func (*BinaryExpr) expr()   {}
func (*CallExpr) expr()     {}
func (*IndexExpr) expr()    {}
func (*SelectorExpr) expr() {}
func (*ListExpr) expr()     {}
func (*BetweenExpr) expr()  {}
func (*DynamicLit) expr()   {}
func (*StarExpr) expr()     {}
func (*NamedExpr) expr()    {}
func (*PipeExpr) expr()     {}

// Statement nodes implement Stmt
func (*LetStmt) stmt()   {}
func (*ExprStmt) stmt()  {}
func (*QueryStmt) stmt() {}

// Operator nodes implement Operator
func (*WhereOp) operator()       {}
func (*ProjectOp) operator()     {}
func (*ProjectAwayOp) operator() {}
func (*ExtendOp) operator()      {}
func (*SummarizeOp) operator()   {}
func (*SortOp) operator()        {}
func (*TakeOp) operator()        {}
func (*TopOp) operator()         {}
func (*CountOp) operator()       {}
func (*DistinctOp) operator()    {}
func (*JoinOp) operator()        {}
func (*UnionOp) operator()       {}
func (*RenderOp) operator()      {}
func (*ParseOp) operator()       {}
func (*MvExpandOp) operator()    {}
func (*SearchOp) operator()      {}
func (*GenericOp) operator()     {}
