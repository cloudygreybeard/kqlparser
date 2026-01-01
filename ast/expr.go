package ast

import (
	"github.com/cloudygreybeard/kqlparser/token"
)

// Ident represents an identifier.
type Ident struct {
	NamePos token.Pos   // Position of the identifier
	Name    string      // Identifier name
	Tok     token.Token // Token type (IDENT or keyword used as name)
}

func (x *Ident) Pos() token.Pos { return x.NamePos }
func (x *Ident) End() token.Pos { return token.Pos(int(x.NamePos) + len(x.Name)) }

// BadExpr represents a malformed expression.
type BadExpr struct {
	From, To token.Pos // Position range of bad expression
}

func (x *BadExpr) Pos() token.Pos { return x.From }
func (x *BadExpr) End() token.Pos { return x.To }

// BasicLit represents a literal of basic type.
type BasicLit struct {
	ValuePos token.Pos   // Position of the literal
	Kind     token.Token // INT, REAL, STRING, BOOL, DATETIME, TIMESPAN, GUID
	Value    string      // Literal string value
}

func (x *BasicLit) Pos() token.Pos { return x.ValuePos }
func (x *BasicLit) End() token.Pos { return token.Pos(int(x.ValuePos) + len(x.Value)) }

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Lparen token.Pos // Position of "("
	X      Expr      // Parenthesized expression
	Rparen token.Pos // Position of ")"
}

func (x *ParenExpr) Pos() token.Pos { return x.Lparen }
func (x *ParenExpr) End() token.Pos { return x.Rparen + 1 }

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	OpPos token.Pos   // Position of operator
	Op    token.Token // Operator: +, -, not
	X     Expr        // Operand
}

func (x *UnaryExpr) Pos() token.Pos { return x.OpPos }
func (x *UnaryExpr) End() token.Pos { return x.X.End() }

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	X     Expr        // Left operand
	OpPos token.Pos   // Position of operator
	Op    token.Token // Operator
	Y     Expr        // Right operand
}

func (x *BinaryExpr) Pos() token.Pos { return x.X.Pos() }
func (x *BinaryExpr) End() token.Pos { return x.Y.End() }

// CallExpr represents a function call expression.
type CallExpr struct {
	Fun    Expr      // Function expression
	Lparen token.Pos // Position of "("
	Args   []Expr    // Function arguments
	Rparen token.Pos // Position of ")"
}

func (x *CallExpr) Pos() token.Pos { return x.Fun.Pos() }
func (x *CallExpr) End() token.Pos { return x.Rparen + 1 }

// IndexExpr represents an index expression (a[b]).
type IndexExpr struct {
	X        Expr      // Expression being indexed
	Lbracket token.Pos // Position of "["
	Index    Expr      // Index expression
	Rbracket token.Pos // Position of "]"
}

func (x *IndexExpr) Pos() token.Pos { return x.X.Pos() }
func (x *IndexExpr) End() token.Pos { return x.Rbracket + 1 }

// SelectorExpr represents a selector expression (a.b).
type SelectorExpr struct {
	X   Expr      // Expression
	Dot token.Pos // Position of "."
	Sel *Ident    // Selector
}

func (x *SelectorExpr) Pos() token.Pos { return x.X.Pos() }
func (x *SelectorExpr) End() token.Pos { return x.Sel.End() }

// ListExpr represents a list of expressions in parentheses, used with 'in'.
type ListExpr struct {
	Lparen token.Pos // Position of "("
	Elems  []Expr    // List elements
	Rparen token.Pos // Position of ")"
}

func (x *ListExpr) Pos() token.Pos { return x.Lparen }
func (x *ListExpr) End() token.Pos { return x.Rparen + 1 }

// BetweenExpr represents a between expression (x between (a .. b)).
type BetweenExpr struct {
	X      Expr      // Value to test
	OpPos  token.Pos // Position of "between" or "!between"
	Not    bool      // True for !between
	Lparen token.Pos // Position of "("
	Low    Expr      // Low bound
	High   Expr      // High bound
	Rparen token.Pos // Position of ")"
}

func (x *BetweenExpr) Pos() token.Pos { return x.X.Pos() }
func (x *BetweenExpr) End() token.Pos { return x.Rparen + 1 }

// DynamicLit represents a dynamic literal (dynamic([1,2,3])).
type DynamicLit struct {
	Dynamic token.Pos // Position of "dynamic"
	Lparen  token.Pos // Position of "("
	Value   Expr      // JSON-like value (can be array, object, etc.)
	Rparen  token.Pos // Position of ")"
}

func (x *DynamicLit) Pos() token.Pos { return x.Dynamic }
func (x *DynamicLit) End() token.Pos { return x.Rparen + 1 }

// StarExpr represents the * wildcard expression.
type StarExpr struct {
	Star token.Pos // Position of "*"
}

func (x *StarExpr) Pos() token.Pos { return x.Star }
func (x *StarExpr) End() token.Pos { return x.Star + 1 }

// NamedExpr represents a named expression (name = expr).
type NamedExpr struct {
	Name   *Ident    // Column name (nil if unnamed or tuple)
	Names  []*Ident  // Multiple names for tuple unpacking (A, B) = expr
	Assign token.Pos // Position of "=" (NoPos if unnamed)
	Expr   Expr      // Expression
}

func (x *NamedExpr) Pos() token.Pos {
	if x.Name != nil {
		return x.Name.Pos()
	}
	if len(x.Names) > 0 {
		return x.Names[0].Pos()
	}
	return x.Expr.Pos()
}

func (x *NamedExpr) End() token.Pos { return x.Expr.End() }

// PipeExpr represents a piped query expression (source | op1 | op2).
type PipeExpr struct {
	Source    Expr       // Source expression (table reference, function call, etc.)
	Operators []Operator // Sequence of piped operators
}

func (x *PipeExpr) Pos() token.Pos { return x.Source.Pos() }

func (x *PipeExpr) End() token.Pos {
	if len(x.Operators) > 0 {
		return x.Operators[len(x.Operators)-1].End()
	}
	return x.Source.End()
}

// ToScalarExpr represents a toscalar expression that converts a tabular result to scalar.
// Syntax: toscalar([kind=nooptimization] (pipe_expression))
type ToScalarExpr struct {
	ToScalar   token.Pos // Position of "toscalar"
	NoOptimize bool      // Whether kind=nooptimization is specified
	Lparen     token.Pos // Position of "("
	Query      Expr      // The pipe expression inside (usually a PipeExpr)
	Rparen     token.Pos // Position of ")"
}

func (x *ToScalarExpr) Pos() token.Pos { return x.ToScalar }
func (x *ToScalarExpr) End() token.Pos { return x.Rparen + 1 }

// ToTableExpr represents a totable expression that ensures tabular result.
// Syntax: totable([kind=nooptimization] (pipe_expression))
type ToTableExpr struct {
	ToTable    token.Pos // Position of "totable"
	NoOptimize bool      // Whether kind=nooptimization is specified
	Lparen     token.Pos // Position of "("
	Query      Expr      // The pipe expression inside (usually a PipeExpr)
	Rparen     token.Pos // Position of ")"
}

func (x *ToTableExpr) Pos() token.Pos { return x.ToTable }
func (x *ToTableExpr) End() token.Pos { return x.Rparen + 1 }

// MaterializeExpr represents a materialize expression that caches a tabular result.
// Syntax: materialize(pipe_expression)
type MaterializeExpr struct {
	Materialize token.Pos // Position of "materialize"
	Lparen      token.Pos // Position of "("
	Query       Expr      // The pipe expression inside (usually a PipeExpr)
	Rparen      token.Pos // Position of ")"
}

func (x *MaterializeExpr) Pos() token.Pos { return x.Materialize }
func (x *MaterializeExpr) End() token.Pos { return x.Rparen + 1 }

// FuncExpr represents a function expression in a let statement.
// Syntax: (params) { body }
type FuncExpr struct {
	Lparen token.Pos    // Position of "("
	Params []*FuncParam // Function parameters
	Rparen token.Pos    // Position of ")"
	Lbrace token.Pos    // Position of "{"
	Body   Expr         // Function body
	Rbrace token.Pos    // Position of "}"
}

func (x *FuncExpr) Pos() token.Pos { return x.Lparen }
func (x *FuncExpr) End() token.Pos {
	if x.Rbrace != token.NoPos {
		return x.Rbrace + 1
	}
	return x.Rparen + 1
}

// FuncParam represents a function parameter.
// Syntax: name: type [= default]
type FuncParam struct {
	Name         *Ident    // Parameter name
	Colon        token.Pos // Position of ":" (NoPos if no type)
	Type         Expr      // Type (Ident or TabularTypeExpr)
	Default      token.Pos // Position of "=" (NoPos if no default)
	DefaultValue Expr      // Default value
}

func (x *FuncParam) Pos() token.Pos { return x.Name.Pos() }
func (x *FuncParam) End() token.Pos {
	if x.DefaultValue != nil {
		return x.DefaultValue.End()
	}
	if x.Type != nil {
		return x.Type.End()
	}
	return x.Name.End()
}

// ViewExpr represents a view expression in a let statement.
// Syntax: view() { body }
type ViewExpr struct {
	View   token.Pos // Position of "view"
	Lparen token.Pos // Position of "("
	Rparen token.Pos // Position of ")"
	Lbrace token.Pos // Position of "{"
	Body   Expr      // View body
	Rbrace token.Pos // Position of "}"
}

func (x *ViewExpr) Pos() token.Pos { return x.View }
func (x *ViewExpr) End() token.Pos {
	if x.Rbrace != token.NoPos {
		return x.Rbrace + 1
	}
	return x.Rparen + 1
}

// TabularTypeExpr represents a tabular type schema.
// Syntax: (Col1: type1, Col2: type2, ...)
type TabularTypeExpr struct {
	Lparen  token.Pos         // Position of "("
	Columns []*ColumnDeclExpr // Column declarations
	Rparen  token.Pos         // Position of ")"
}

func (x *TabularTypeExpr) Pos() token.Pos { return x.Lparen }
func (x *TabularTypeExpr) End() token.Pos { return x.Rparen + 1 }
