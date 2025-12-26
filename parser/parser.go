// Package parser implements a recursive descent parser for KQL.
package parser

import (
	"fmt"

	"github.com/cloudygreybeard/kqlparser/ast"
	"github.com/cloudygreybeard/kqlparser/lexer"
	"github.com/cloudygreybeard/kqlparser/token"
)

// Parser holds the state of the parser.
type Parser struct {
	lex    *lexer.Lexer
	file   *token.File
	errors ErrorList

	// Current token
	tok token.Token
	pos token.Pos
	lit string

	// Lookahead for backtracking
	saved []savedState
}

type savedState struct {
	tok token.Token
	pos token.Pos
	lit string
}

// Error represents a parser error.
type Error struct {
	Pos token.Position
	Msg string
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Pos, e.Msg)
}

// ErrorList is a list of parser errors.
type ErrorList []Error

func (el ErrorList) Error() string {
	switch len(el) {
	case 0:
		return "no errors"
	case 1:
		return el[0].Error()
	default:
		return fmt.Sprintf("%s (and %d more errors)", el[0], len(el)-1)
	}
}

// Err returns an error if the list is non-empty, nil otherwise.
func (el ErrorList) Err() error {
	if len(el) == 0 {
		return nil
	}
	return el
}

// New creates a new parser for the given source.
func New(filename, src string) *Parser {
	l := lexer.New(filename, src)
	p := &Parser{
		lex:  l,
		file: l.File(),
	}
	p.next() // Initialize first token
	return p
}

// Errors returns any errors encountered during parsing.
func (p *Parser) Errors() ErrorList {
	// Combine lexer and parser errors
	var all ErrorList
	for _, e := range p.lex.Errors() {
		all = append(all, Error{Pos: e.Pos, Msg: e.Msg})
	}
	all = append(all, p.errors...)
	return all
}

// next advances to the next token.
func (p *Parser) next() {
	t := p.lex.Scan()
	p.tok = t.Type
	p.pos = t.Pos
	p.lit = t.Lit
}

// error adds a parser error.
func (p *Parser) error(pos token.Pos, msg string) {
	position := p.file.Position(pos)
	p.errors = append(p.errors, Error{Pos: position, Msg: msg})
}

// errorf adds a formatted parser error.
func (p *Parser) errorf(pos token.Pos, format string, args ...any) {
	p.error(pos, fmt.Sprintf(format, args...))
}

// expect consumes the next token if it matches, otherwise reports an error.
func (p *Parser) expect(tok token.Token) token.Pos {
	pos := p.pos
	if p.tok != tok {
		p.errorf(pos, "expected %s, got %s", tok, p.tok)
	}
	p.next()
	return pos
}

// accept consumes the next token if it matches and returns true.
func (p *Parser) accept(tok token.Token) bool {
	if p.tok == tok {
		p.next()
		return true
	}
	return false
}

// Parse parses the entire source and returns a Script.
func (p *Parser) Parse() *ast.Script {
	script := &ast.Script{}

	for p.tok != token.EOF {
		stmt := p.parseStmt()
		if stmt != nil {
			script.Stmts = append(script.Stmts, stmt)
		}
		// Skip optional semicolons between statements
		for p.accept(token.SEMI) {
		}
	}

	return script
}

// ParseExpr parses a single expression.
func (p *Parser) ParseExpr() ast.Expr {
	return p.parseExpr()
}

// parseStmt parses a statement.
func (p *Parser) parseStmt() ast.Stmt {
	switch p.tok {
	case token.LET:
		return p.parseLetStmt()
	default:
		// Expression statement (query)
		expr := p.parseExpr()
		if expr == nil {
			return nil
		}
		return &ast.ExprStmt{X: expr}
	}
}

// parseLetStmt parses a let statement.
func (p *Parser) parseLetStmt() *ast.LetStmt {
	letPos := p.pos
	p.next() // consume 'let'

	name := p.parseIdent()
	if name == nil {
		return nil
	}

	assignPos := p.expect(token.ASSIGN)
	value := p.parseExpr()

	return &ast.LetStmt{
		Let:    letPos,
		Name:   name,
		Assign: assignPos,
		Value:  value,
	}
}

// parseExpr parses an expression (may include pipe operators).
func (p *Parser) parseExpr() ast.Expr {
	expr := p.parseOrExpr()
	if expr == nil {
		return nil
	}

	// Check for pipe expression
	if p.tok == token.PIPE {
		return p.parsePipeExpr(expr)
	}

	return expr
}

// parsePipeExpr parses a pipe expression (source | op1 | op2 ...).
func (p *Parser) parsePipeExpr(source ast.Expr) *ast.PipeExpr {
	pipe := &ast.PipeExpr{Source: source}

	for p.tok == token.PIPE {
		op := p.parseOperator()
		if op != nil {
			pipe.Operators = append(pipe.Operators, op)
		}
	}

	return pipe
}

// parseOrExpr parses an 'or' expression.
func (p *Parser) parseOrExpr() ast.Expr {
	left := p.parseAndExpr()
	for p.tok == token.OR {
		opPos := p.pos
		p.next()
		right := p.parseAndExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: token.OR, Y: right}
	}
	return left
}

// parseAndExpr parses an 'and' expression.
func (p *Parser) parseAndExpr() ast.Expr {
	left := p.parseCompareExpr()
	for p.tok == token.AND {
		opPos := p.pos
		p.next()
		right := p.parseCompareExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: token.AND, Y: right}
	}
	return left
}

// parseCompareExpr parses a comparison expression.
func (p *Parser) parseCompareExpr() ast.Expr {
	left := p.parseAddExpr()

	switch p.tok {
	case token.EQL, token.NEQ, token.LSS, token.GTR, token.LEQ, token.GEQ,
		token.TILDE, token.NTILDE,
		token.CONTAINS, token.CONTAINSCS, token.NOTCONTAINS, token.NOTCONTAINSCS,
		token.STARTSWITH, token.STARTSWITHCS, token.ENDSWITH, token.ENDSWITHCS,
		token.HAS, token.HASCS, token.HASALL, token.HASANY,
		token.HASPREFIX, token.HASPREFIXCS, token.HASSUFFIX, token.HASSUFFIXCS,
		token.LIKE, token.MATCHESREGEX:
		opPos := p.pos
		op := p.tok
		p.next()
		right := p.parseAddExpr()
		return &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: right}

	case token.IN:
		return p.parseInExpr(left)

	case token.BETWEEN:
		return p.parseBetweenExpr(left, false)
	}

	return left
}

// parseInExpr parses an 'in' expression.
func (p *Parser) parseInExpr(left ast.Expr) ast.Expr {
	opPos := p.pos
	p.next() // consume 'in'

	lparen := p.expect(token.LPAREN)
	var elems []ast.Expr
	for p.tok != token.RPAREN && p.tok != token.EOF {
		elem := p.parseAddExpr()
		elems = append(elems, elem)
		if !p.accept(token.COMMA) {
			break
		}
	}
	rparen := p.expect(token.RPAREN)

	list := &ast.ListExpr{Lparen: lparen, Elems: elems, Rparen: rparen}
	return &ast.BinaryExpr{X: left, OpPos: opPos, Op: token.IN, Y: list}
}

// parseBetweenExpr parses a 'between' expression.
func (p *Parser) parseBetweenExpr(left ast.Expr, not bool) ast.Expr {
	opPos := p.pos
	p.next() // consume 'between'

	lparen := p.expect(token.LPAREN)
	low := p.parseAddExpr()
	p.expect(token.DOTDOT)
	high := p.parseAddExpr()
	rparen := p.expect(token.RPAREN)

	return &ast.BetweenExpr{
		X:      left,
		OpPos:  opPos,
		Not:    not,
		Lparen: lparen,
		Low:    low,
		High:   high,
		Rparen: rparen,
	}
}

// parseAddExpr parses an additive expression.
func (p *Parser) parseAddExpr() ast.Expr {
	left := p.parseMulExpr()
	for p.tok == token.ADD || p.tok == token.SUB {
		opPos := p.pos
		op := p.tok
		p.next()
		right := p.parseMulExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: right}
	}
	return left
}

// parseMulExpr parses a multiplicative expression.
func (p *Parser) parseMulExpr() ast.Expr {
	left := p.parseUnaryExpr()
	for p.tok == token.MUL || p.tok == token.QUO || p.tok == token.REM {
		opPos := p.pos
		op := p.tok
		p.next()
		right := p.parseUnaryExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: right}
	}
	return left
}

// parseUnaryExpr parses a unary expression.
func (p *Parser) parseUnaryExpr() ast.Expr {
	switch p.tok {
	case token.ADD, token.SUB, token.NOT:
		opPos := p.pos
		op := p.tok
		p.next()
		x := p.parseUnaryExpr()
		return &ast.UnaryExpr{OpPos: opPos, Op: op, X: x}
	}
	return p.parsePostfixExpr()
}

// parsePostfixExpr parses postfix expressions (calls, indexing, selectors).
func (p *Parser) parsePostfixExpr() ast.Expr {
	x := p.parsePrimaryExpr()

	for {
		switch p.tok {
		case token.LPAREN:
			// Function call
			x = p.parseCallExpr(x)
		case token.LBRACKET:
			// Index expression
			x = p.parseIndexExpr(x)
		case token.DOT:
			// Selector expression
			x = p.parseSelectorExpr(x)
		default:
			return x
		}
	}
}

// parseCallExpr parses a function call.
func (p *Parser) parseCallExpr(fun ast.Expr) *ast.CallExpr {
	lparen := p.expect(token.LPAREN)
	var args []ast.Expr

	for p.tok != token.RPAREN && p.tok != token.EOF {
		// Check for named argument (name = expr)
		arg := p.parseNamedExpr()
		args = append(args, arg)
		if !p.accept(token.COMMA) {
			break
		}
	}

	rparen := p.expect(token.RPAREN)
	return &ast.CallExpr{Fun: fun, Lparen: lparen, Args: args, Rparen: rparen}
}

// parseIndexExpr parses an index expression.
func (p *Parser) parseIndexExpr(x ast.Expr) *ast.IndexExpr {
	lbracket := p.expect(token.LBRACKET)
	index := p.parseExpr()
	rbracket := p.expect(token.RBRACKET)
	return &ast.IndexExpr{X: x, Lbracket: lbracket, Index: index, Rbracket: rbracket}
}

// parseSelectorExpr parses a selector expression.
func (p *Parser) parseSelectorExpr(x ast.Expr) *ast.SelectorExpr {
	dotPos := p.pos
	p.next() // consume '.'
	sel := p.parseIdent()
	return &ast.SelectorExpr{X: x, Dot: dotPos, Sel: sel}
}

// parsePrimaryExpr parses a primary expression.
func (p *Parser) parsePrimaryExpr() ast.Expr {
	switch p.tok {
	case token.IDENT:
		return p.parseIdent()

	case token.INT, token.REAL, token.STRING, token.BOOL,
		token.DATETIME, token.TIMESPAN, token.GUID:
		return p.parseLiteral()

	case token.LPAREN:
		return p.parseParenExpr()

	case token.MUL:
		// Star expression
		pos := p.pos
		p.next()
		return &ast.StarExpr{Star: pos}

	case token.DYNAMICTYPE:
		return p.parseDynamicLit()

	case token.DATATABLE:
		return p.parseDatatableExpr()

	// Handle keywords that can be used as identifiers in certain contexts
	case token.COUNT:
		return p.parseIdent()

	// Type keywords used as function names
	case token.DATETIMETYPE, token.TIMESPANTYPE, token.GUIDTYPE,
		token.LONGTYPE, token.INTTYPE, token.REALTYPE, token.STRINGTYPE,
		token.BOOLTYPE:
		return p.parseIdent()

	case token.EOF:
		return nil

	default:
		// Try to use keyword as identifier
		if p.tok.IsKeyword() {
			return p.parseIdent()
		}
		p.errorf(p.pos, "unexpected token %s", p.tok)
		p.next()
		return &ast.BadExpr{From: p.pos, To: p.pos}
	}
}

// parseIdent parses an identifier (including keywords used as names).
func (p *Parser) parseIdent() *ast.Ident {
	pos := p.pos
	name := p.lit
	tok := p.tok

	if p.tok == token.IDENT || p.tok.IsKeyword() {
		p.next()
	} else {
		p.errorf(pos, "expected identifier, got %s", p.tok)
		name = "_"
	}

	return &ast.Ident{NamePos: pos, Name: name, Tok: tok}
}

// parseLiteral parses a literal value.
func (p *Parser) parseLiteral() *ast.BasicLit {
	lit := &ast.BasicLit{
		ValuePos: p.pos,
		Kind:     p.tok,
		Value:    p.lit,
	}
	p.next()
	return lit
}

// parseParenExpr parses a parenthesized expression.
func (p *Parser) parseParenExpr() *ast.ParenExpr {
	lparen := p.expect(token.LPAREN)
	x := p.parseExpr()
	rparen := p.expect(token.RPAREN)
	return &ast.ParenExpr{Lparen: lparen, X: x, Rparen: rparen}
}

// parseDynamicLit parses a dynamic literal.
func (p *Parser) parseDynamicLit() *ast.DynamicLit {
	dynPos := p.pos
	p.next() // consume 'dynamic'
	lparen := p.expect(token.LPAREN)
	value := p.parseJSONValue()
	rparen := p.expect(token.RPAREN)
	return &ast.DynamicLit{Dynamic: dynPos, Lparen: lparen, Value: value, Rparen: rparen}
}

// parseJSONValue parses a JSON-like value inside dynamic().
func (p *Parser) parseJSONValue() ast.Expr {
	switch p.tok {
	case token.LBRACKET:
		return p.parseJSONArray()
	case token.LBRACE:
		return p.parseJSONObject()
	default:
		return p.parseAddExpr()
	}
}

// parseJSONArray parses a JSON array.
func (p *Parser) parseJSONArray() *ast.ListExpr {
	lparen := p.expect(token.LBRACKET)
	var elems []ast.Expr
	for p.tok != token.RBRACKET && p.tok != token.EOF {
		elem := p.parseJSONValue()
		elems = append(elems, elem)
		if !p.accept(token.COMMA) {
			break
		}
	}
	rparen := p.expect(token.RBRACKET)
	return &ast.ListExpr{Lparen: lparen, Elems: elems, Rparen: rparen}
}

// parseJSONObject parses a JSON object (simplified - returns as list of pairs).
func (p *Parser) parseJSONObject() *ast.ListExpr {
	lparen := p.expect(token.LBRACE)
	var elems []ast.Expr
	for p.tok != token.RBRACE && p.tok != token.EOF {
		// Parse key
		key := p.parsePrimaryExpr()
		p.expect(token.COLON)
		value := p.parseJSONValue()
		// Store as named expr
		elems = append(elems, &ast.NamedExpr{
			Name:   &ast.Ident{NamePos: key.Pos(), Name: ""},
			Assign: token.NoPos,
			Expr:   value,
		})
		if !p.accept(token.COMMA) {
			break
		}
	}
	rparen := p.expect(token.RBRACE)
	return &ast.ListExpr{Lparen: lparen, Elems: elems, Rparen: rparen}
}

// parseDatatableExpr parses a datatable expression.
func (p *Parser) parseDatatableExpr() ast.Expr {
	// For now, parse as a function call
	return p.parseIdent()
}

// parseNamedExpr parses an optionally named expression.
func (p *Parser) parseNamedExpr() ast.Expr {
	// Check if this is a named expression (name = expr)
	if p.tok == token.IDENT {
		// Look ahead to see if there's an =
		name := p.parseIdent()
		if p.tok == token.ASSIGN {
			assignPos := p.pos
			p.next()
			expr := p.parseExpr()
			return &ast.NamedExpr{Name: name, Assign: assignPos, Expr: expr}
		}
		// Not a named expression, treat the ident as the start of an expression
		return p.continueParsingExpr(name)
	}
	return p.parseExpr()
}

// continueParsingExpr continues parsing an expression given an already-parsed identifier.
func (p *Parser) continueParsingExpr(ident *ast.Ident) ast.Expr {
	// Continue with postfix and binary operators
	x := p.continuePostfixExpr(ident)
	return p.continueBinaryExpr(x)
}

// continuePostfixExpr continues parsing postfix operators.
func (p *Parser) continuePostfixExpr(x ast.Expr) ast.Expr {
	for {
		switch p.tok {
		case token.LPAREN:
			x = p.parseCallExpr(x)
		case token.LBRACKET:
			x = p.parseIndexExpr(x)
		case token.DOT:
			x = p.parseSelectorExpr(x)
		default:
			return x
		}
	}
}

// continueBinaryExpr continues parsing binary operators with proper precedence.
func (p *Parser) continueBinaryExpr(left ast.Expr) ast.Expr {
	// Handle multiplicative
	for p.tok == token.MUL || p.tok == token.QUO || p.tok == token.REM {
		opPos := p.pos
		op := p.tok
		p.next()
		right := p.parseUnaryExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: right}
	}

	// Handle additive
	for p.tok == token.ADD || p.tok == token.SUB {
		opPos := p.pos
		op := p.tok
		p.next()
		right := p.parseMulExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: right}
	}

	// Handle comparison
	switch p.tok {
	case token.EQL, token.NEQ, token.LSS, token.GTR, token.LEQ, token.GEQ,
		token.TILDE, token.NTILDE,
		token.CONTAINS, token.CONTAINSCS, token.NOTCONTAINS, token.NOTCONTAINSCS,
		token.STARTSWITH, token.STARTSWITHCS, token.ENDSWITH, token.ENDSWITHCS,
		token.HAS, token.HASCS, token.HASALL, token.HASANY,
		token.HASPREFIX, token.HASPREFIXCS, token.HASSUFFIX, token.HASSUFFIXCS,
		token.LIKE, token.MATCHESREGEX:
		opPos := p.pos
		op := p.tok
		p.next()
		right := p.parseAddExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: right}
	case token.IN:
		left = p.parseInExpr(left)
	case token.BETWEEN:
		left = p.parseBetweenExpr(left, false)
	}

	// Handle and
	for p.tok == token.AND {
		opPos := p.pos
		p.next()
		right := p.parseCompareExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: token.AND, Y: right}
	}

	// Handle or
	for p.tok == token.OR {
		opPos := p.pos
		p.next()
		right := p.parseAndExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: token.OR, Y: right}
	}

	return left
}
