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

// File returns the source file for position information.
func (p *Parser) File() *token.File {
	return p.file
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
	case token.PRINT:
		return p.parsePrintStmt()
	case token.RANGE:
		return p.parseRangeStmt()
	case token.DATATABLE:
		return p.parseDatatableStmt()
	case token.EXTERNALDATA:
		return p.parseExternalDataStmt()
	case token.FIND:
		// find is a special standalone operator that can start a query
		return p.parseFindStmt()
	case token.DECLARE:
		return p.parseDeclareStmt()
	case token.ALIAS:
		return p.parseAliasStmt()
	default:
		// Expression statement (query)
		expr := p.parseExpr()
		if expr == nil {
			return nil
		}
		return &ast.ExprStmt{X: expr}
	}
}

// parseFindStmt parses a find statement as a query.
func (p *Parser) parseFindStmt() ast.Stmt {
	findOp := p.parseFindOp(token.NoPos)

	// Wrap in a PipeExpr
	pipe := &ast.PipeExpr{
		Operators: []ast.Operator{findOp},
	}

	// Parse any additional piped operators
	for p.tok == token.PIPE {
		op := p.parseOperator()
		if op != nil {
			pipe.Operators = append(pipe.Operators, op)
		}
	}

	return &ast.ExprStmt{X: pipe}
}

// parseDeclareStmt parses a declare statement.
// Syntax: declare query_parameters(...) or declare pattern name = ...
func (p *Parser) parseDeclareStmt() ast.Stmt {
	declarePos := p.pos
	p.next() // consume 'declare'

	switch p.tok {
	case token.IDENT:
		if p.lit == "query_parameters" {
			return p.parseDeclareQueryParamsStmt(declarePos)
		}
	case token.PATTERN:
		return p.parseDeclarePatternStmt(declarePos)
	}

	// Fall back to expression parsing
	expr := p.parseExpr()
	return &ast.DeclareStmt{Declare: declarePos, Body: expr}
}

// parseDeclareQueryParamsStmt parses declare query_parameters(...)
func (p *Parser) parseDeclareQueryParamsStmt(declarePos token.Pos) *ast.DeclareStmt {
	kindPos := p.pos
	p.next() // consume 'query_parameters'

	var params []*ast.FuncParam
	if p.tok == token.LPAREN {
		p.next() // consume '('
		for p.tok != token.RPAREN && p.tok != token.EOF {
			param := p.parseFuncParam()
			if param != nil {
				params = append(params, param)
			}
			if !p.accept(token.COMMA) {
				break
			}
		}
		p.expect(token.RPAREN)
	}

	return &ast.DeclareStmt{
		Declare: declarePos,
		Kind:    "query_parameters",
		KindPos: kindPos,
		Params:  params,
	}
}

// parseDeclarePatternStmt parses declare pattern name = ...
func (p *Parser) parseDeclarePatternStmt(declarePos token.Pos) *ast.DeclareStmt {
	kindPos := p.pos
	p.next() // consume 'pattern'

	name := p.parseIdent()
	assignPos := p.expect(token.ASSIGN)

	// Parse pattern body - function-like
	var params []*ast.FuncParam
	if p.tok == token.LPAREN {
		p.next() // consume '('
		for p.tok != token.RPAREN && p.tok != token.EOF {
			param := p.parseFuncParam()
			if param != nil {
				params = append(params, param)
			}
			if !p.accept(token.COMMA) {
				break
			}
		}
		p.expect(token.RPAREN)
	}

	var body ast.Expr
	if p.tok == token.LBRACE {
		p.next() // consume '{'
		body = p.parseExpr()
		p.expect(token.RBRACE)
	}

	return &ast.DeclareStmt{
		Declare:   declarePos,
		Kind:      "pattern",
		KindPos:   kindPos,
		Name:      name,
		AssignPos: assignPos,
		Params:    params,
		Body:      body,
	}
}

// parseAliasStmt parses an alias statement.
// Syntax: alias database Name = cluster('...').database('...')
func (p *Parser) parseAliasStmt() ast.Stmt {
	aliasPos := p.pos
	p.next() // consume 'alias'

	// Expect 'database'
	if p.tok != token.DATABASE {
		p.error(p.pos, "expected 'database' after 'alias'")
		return nil
	}
	p.next() // consume 'database'

	name := p.parseIdent()
	assignPos := p.expect(token.ASSIGN)
	value := p.parseExpr()

	return &ast.AliasStmt{
		Alias:     aliasPos,
		Name:      name,
		AssignPos: assignPos,
		Value:     value,
	}
}

// parseLetStmt parses a let statement.
// Supports: let x = expr; let f = (params) { body }; let v = view() { body }
func (p *Parser) parseLetStmt() *ast.LetStmt {
	letPos := p.pos
	p.next() // consume 'let'

	name := p.parseIdent()
	if name == nil {
		return nil
	}

	assignPos := p.expect(token.ASSIGN)

	// Check for view() declaration
	if p.tok == token.VIEW {
		return p.parseLetViewStmt(letPos, name, assignPos)
	}

	// Check for function definition with parameters
	if p.tok == token.LPAREN {
		return p.parseLetFunctionStmt(letPos, name, assignPos)
	}

	// Regular let statement
	value := p.parseExpr()

	return &ast.LetStmt{
		Let:    letPos,
		Name:   name,
		Assign: assignPos,
		Value:  value,
	}
}

// parseLetFunctionStmt parses a let function definition.
// Syntax: let f = (params) { body }
func (p *Parser) parseLetFunctionStmt(letPos token.Pos, name *ast.Ident, assignPos token.Pos) *ast.LetStmt {
	lparen := p.pos
	p.next() // consume '('

	// Parse parameters
	var params []*ast.FuncParam
	for p.tok != token.RPAREN && p.tok != token.EOF {
		param := p.parseFuncParam()
		if param != nil {
			params = append(params, param)
		}
		if !p.accept(token.COMMA) {
			break
		}
	}

	rparen := p.expect(token.RPAREN)

	// Parse optional body { ... }
	var body ast.Expr
	if p.tok == token.LBRACE {
		p.next() // consume '{'
		body = p.parseExpr()
		p.expect(token.RBRACE)
	}

	// Create function expression
	funcExpr := &ast.FuncExpr{
		Lparen: lparen,
		Params: params,
		Rparen: rparen,
		Body:   body,
	}

	return &ast.LetStmt{
		Let:    letPos,
		Name:   name,
		Assign: assignPos,
		Value:  funcExpr,
	}
}

// parseLetViewStmt parses a let view declaration.
// Syntax: let v = view() { body }
func (p *Parser) parseLetViewStmt(letPos token.Pos, name *ast.Ident, assignPos token.Pos) *ast.LetStmt {
	viewPos := p.pos
	p.next() // consume 'view'

	lparen := p.expect(token.LPAREN)
	rparen := p.expect(token.RPAREN)

	// Parse body { ... }
	var body ast.Expr
	if p.tok == token.LBRACE {
		p.next() // consume '{'
		body = p.parseExpr()
		p.expect(token.RBRACE)
	}

	// Create view expression
	viewExpr := &ast.ViewExpr{
		View:   viewPos,
		Lparen: lparen,
		Rparen: rparen,
		Body:   body,
	}

	return &ast.LetStmt{
		Let:    letPos,
		Name:   name,
		Assign: assignPos,
		Value:  viewExpr,
	}
}

// parseFuncParam parses a function parameter.
// Syntax: name: type [= default]
func (p *Parser) parseFuncParam() *ast.FuncParam {
	name := p.parseIdent()

	param := &ast.FuncParam{Name: name}

	// Parse optional type
	if p.tok == token.COLON {
		param.Colon = p.pos
		p.next()

		// Check for tabular type (Name: string, ...)
		if p.tok == token.LPAREN {
			param.Type = p.parseTabularType()
		} else {
			param.Type = p.parseIdent()
		}
	}

	// Parse optional default value
	if p.tok == token.ASSIGN {
		param.Default = p.pos
		p.next()
		param.DefaultValue = p.parseExprNoPipe()
	}

	return param
}

// parseTabularType parses a tabular type schema.
// Syntax: (Col1: type1, Col2: type2, ...)
func (p *Parser) parseTabularType() ast.Expr {
	lparen := p.pos
	p.next() // consume '('

	var cols []*ast.ColumnDeclExpr
	for p.tok != token.RPAREN && p.tok != token.EOF {
		col := &ast.ColumnDeclExpr{Name: p.parseIdent()}
		if p.tok == token.COLON {
			col.Colon = p.pos
			p.next()
			col.Type = p.parseIdent()
		}
		cols = append(cols, col)
		if !p.accept(token.COMMA) {
			break
		}
	}

	rparen := p.expect(token.RPAREN)

	return &ast.TabularTypeExpr{
		Lparen:  lparen,
		Columns: cols,
		Rparen:  rparen,
	}
}

// parsePrintStmt parses a print statement.
func (p *Parser) parsePrintStmt() *ast.PrintStmt {
	printPos := p.pos
	p.next() // consume 'print'

	stmt := &ast.PrintStmt{Print: printPos}

	// Parse column expressions
	for p.tok != token.SEMI && p.tok != token.EOF && p.tok != token.PIPE {
		item := p.parseNamedExprSingle()
		stmt.Columns = append(stmt.Columns, item)
		if !p.accept(token.COMMA) {
			break
		}
	}

	return stmt
}

// parseRangeStmt parses a range statement.
func (p *Parser) parseRangeStmt() *ast.RangeStmt {
	rangePos := p.pos
	p.next() // consume 'range'

	stmt := &ast.RangeStmt{Range: rangePos}

	// Parse column name
	stmt.Column = p.parseIdent()

	// Expect 'from'
	if p.tok == token.FROM {
		p.next()
	}

	// Parse start value
	stmt.From = p.parseUnaryExpr()

	// Expect 'to'
	if p.tok == token.TO {
		p.next()
	}

	// Parse end value
	stmt.To = p.parseUnaryExpr()

	// Expect 'step'
	if p.tok == token.STEP {
		p.next()
	}

	// Parse step value
	stmt.Step = p.parseUnaryExpr()

	return stmt
}

// parseDatatableStmt parses a datatable statement.
func (p *Parser) parseDatatableStmt() *ast.DatatableStmt {
	datatablePos := p.pos
	p.next() // consume 'datatable'

	stmt := &ast.DatatableStmt{Datatable: datatablePos}

	// Parse column schema: (name:type, name:type, ...)
	if p.tok == token.LPAREN {
		p.next()
		for p.tok != token.RPAREN && p.tok != token.EOF {
			col := p.parseColumnDecl()
			if col != nil {
				stmt.Columns = append(stmt.Columns, col)
			}
			if !p.accept(token.COMMA) {
				break
			}
		}
		p.expect(token.RPAREN)
	}

	// Parse values: [value, value, ...]
	if p.tok == token.LBRACKET {
		p.next()
		for p.tok != token.RBRACKET && p.tok != token.EOF {
			val := p.parseUnaryExpr()
			stmt.Values = append(stmt.Values, val)
			if !p.accept(token.COMMA) {
				break
			}
		}
		stmt.EndPos = p.pos
		p.expect(token.RBRACKET)
	}

	return stmt
}

// parseColumnDecl parses a column declaration (name:type).
func (p *Parser) parseColumnDecl() *ast.ColumnDeclExpr {
	name := p.parseIdent()
	if name == nil {
		return nil
	}

	colonPos := p.expect(token.COLON)

	typeName := p.parseIdent()
	if typeName == nil {
		return nil
	}

	return &ast.ColumnDeclExpr{
		Name:  name,
		Colon: colonPos,
		Type:  typeName,
	}
}

// parseExternalDataStmt parses an externaldata statement.
func (p *Parser) parseExternalDataStmt() *ast.ExternalDataOp {
	externalDataPos := p.pos
	p.next() // consume 'externaldata'

	op := &ast.ExternalDataOp{ExternalData: externalDataPos}

	// Parse column schema: (name:type, name:type, ...)
	if p.tok == token.LPAREN {
		p.next()
		for p.tok != token.RPAREN && p.tok != token.EOF {
			col := p.parseColumnDecl()
			if col != nil {
				op.Columns = append(op.Columns, col)
			}
			if !p.accept(token.COMMA) {
				break
			}
		}
		p.expect(token.RPAREN)
	}

	// Parse URIs: [uri, uri, ...]
	if p.tok == token.LBRACKET {
		p.next()
		for p.tok != token.RBRACKET && p.tok != token.EOF {
			uri := p.parseUnaryExpr()
			op.URIs = append(op.URIs, uri)
			if !p.accept(token.COMMA) {
				break
			}
		}
		op.EndPos = p.pos
		p.expect(token.RBRACKET)
	}

	// Parse optional 'with (props)'
	if p.tok == token.WITH {
		op.WithPos = p.pos
		p.next()
		if p.tok == token.LPAREN {
			p.next()
			for p.tok != token.RPAREN && p.tok != token.EOF {
				prop := p.parseUnaryExpr()
				op.Properties = append(op.Properties, prop)
				if !p.accept(token.COMMA) {
					break
				}
			}
			op.EndPos = p.pos
			p.expect(token.RPAREN)
		}
	}

	return op
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

// parseContextualSubExpr parses a contextual subexpression inside parentheses.
// This is used for mv-apply, toscalar, totable, etc. where the inner expression
// can be either:
// 1. A simple expression like "x > 10"
// 2. An operator-based expression like "summarize count()" or "where x > y"
func (p *Parser) parseContextualSubExpr() *ast.PipeExpr {
	pipe := &ast.PipeExpr{}

	// First, check if we're starting with an operator keyword
	if p.isOperatorKeyword() {
		// Parse operator directly (without leading |)
		firstOp := p.parseOperatorDirect()
		if firstOp != nil {
			pipe.Operators = append(pipe.Operators, firstOp)
		}
	} else {
		// Parse as a regular expression (the source)
		pipe.Source = p.parseExprNoPipe()
	}

	// Then parse any subsequent piped operators
	for p.tok == token.PIPE {
		op := p.parseOperator()
		if op != nil {
			pipe.Operators = append(pipe.Operators, op)
		}
	}

	return pipe
}

// isOperatorKeyword returns true if the current token is a KQL operator keyword.
func (p *Parser) isOperatorKeyword() bool {
	switch p.tok {
	case token.WHERE, token.FILTER, token.PROJECT, token.PROJECTAWAY, token.EXTEND,
		token.SUMMARIZE, token.SORT, token.ORDER, token.TAKE, token.LIMIT,
		token.TOP, token.COUNT, token.DISTINCT, token.JOIN, token.UNION,
		token.RENDER, token.PARSE, token.PARSEWHERE, token.PARSEKV,
		token.MVEXPAND, token.SEARCH, token.AS, token.GETSCHEMA, token.SERIALIZE,
		token.INVOKE, token.PROJECTRENAME, token.PROJECTREORDER, token.SAMPLE,
		token.SAMPLEDISTINCT, token.LOOKUP, token.MAKESERIES, token.SCAN,
		token.CONSUME, token.EVALUATE, token.REDUCE, token.FORK, token.FACET,
		token.PROJECTKEEP, token.TOPNESTED, token.TOPHITTERS, token.MVAPPLY, token.FIND:
		return true
	default:
		return false
	}
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
		token.TILDE, token.NTILDE, token.COLON,
		// Positive string operators
		token.CONTAINS, token.CONTAINSCS,
		token.STARTSWITH, token.STARTSWITHCS,
		token.ENDSWITH, token.ENDSWITHCS,
		token.HAS, token.HASCS, token.HASALL, token.HASANY,
		token.HASPREFIX, token.HASPREFIXCS,
		token.HASSUFFIX, token.HASSUFFIXCS,
		token.LIKE, token.LIKECS, token.MATCHESREGEX,
		// Negated string operators
		token.NOTCONTAINS, token.NOTCONTAINSCS,
		token.NOTSTARTSWITH, token.NOTSTARTSWITCS,
		token.NOTENDSWITH, token.NOTENDSWITHCS,
		token.NOTHAS, token.NOTHASCS,
		token.NOTHASPREFIX, token.NOTHASPREFIXCS,
		token.NOTHASSUFFIX, token.NOTHASSUFFIXCS,
		token.NOTLIKE, token.NOTLIKECS:
		opPos := p.pos
		op := p.tok
		p.next()
		right := p.parseAddExpr()
		return &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: right}

	case token.IN:
		return p.parseInExpr(left, false)

	case token.NOTIN, token.NOTINCI:
		return p.parseInExpr(left, true)

	case token.BETWEEN:
		return p.parseBetweenExpr(left, false)

	case token.NOTBETWEEN:
		return p.parseBetweenExpr(left, true)
	}

	return left
}

// parseInExpr parses an 'in' or '!in' expression.
func (p *Parser) parseInExpr(left ast.Expr, not bool) ast.Expr {
	opPos := p.pos
	op := p.tok
	p.next() // consume 'in' or '!in' or '!in~'

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
	return &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: list}
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
// Also handles legacy element syntax: obj.["key"]
func (p *Parser) parseSelectorExpr(x ast.Expr) ast.Expr {
	dotPos := p.pos
	p.next() // consume '.'

	// Check for legacy element syntax: obj.["key"]
	if p.tok == token.LBRACKET {
		return p.parseIndexExpr(x)
	}

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

	case token.TOSCALAR:
		return p.parseToScalarExpr()

	case token.TOTABLE:
		return p.parseToTableExpr()

	case token.MATERIALIZE:
		return p.parseMaterializeExpr()

	// Handle keywords that can be used as identifiers in certain contexts
	case token.COUNT:
		return p.parseIdent()

	// Type keywords used as function names
	case token.DATETIMETYPE, token.GUIDTYPE,
		token.LONGTYPE, token.INTTYPE, token.REALTYPE, token.STRINGTYPE,
		token.BOOLTYPE:
		return p.parseIdent()

	// time() and timespan() with special literal parsing
	case token.TIMESPANTYPE:
		return p.parseTimespanLiteral()

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

// parseTimespanLiteral parses time() or timespan() with special literal content.
// Syntax: time(1:30:00) or timespan(0.01:30:00.123)
func (p *Parser) parseTimespanLiteral() ast.Expr {
	// Get the function name (time or timespan)
	fnName := p.parseIdent()

	// Check if followed by (
	if p.tok != token.LPAREN {
		// Not a literal call, just return the identifier
		return fnName
	}

	lparen := p.pos
	p.next() // consume '('

	// Scan raw content until matching ')' - this handles colons, dots, etc.
	startPos := p.pos
	content := p.scanTimespanContent()

	rparen := p.expect(token.RPAREN)

	// Return as a BasicLit with TIMESPAN kind
	return &ast.CallExpr{
		Fun:    fnName,
		Lparen: lparen,
		Args: []ast.Expr{
			&ast.BasicLit{
				ValuePos: startPos,
				Kind:     token.TIMESPAN,
				Value:    content,
			},
		},
		Rparen: rparen,
	}
}

// scanTimespanContent scans raw content for time/timespan literals.
// Handles digits, colons, dots, and minus signs.
func (p *Parser) scanTimespanContent() string {
	var content string
	for p.tok != token.RPAREN && p.tok != token.EOF {
		content += p.lit
		p.next()
	}
	return content
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

// parseToScalarExpr parses a toscalar expression.
// Syntax: toscalar([kind=nooptimization] (pipe_expression))
func (p *Parser) parseToScalarExpr() *ast.ToScalarExpr {
	pos := p.pos
	p.next() // consume 'toscalar'

	expr := &ast.ToScalarExpr{ToScalar: pos}

	// Check for optional kind=nooptimization
	if p.tok == token.KIND {
		p.next()
		p.expect(token.ASSIGN)
		// Expect 'nooptimization' identifier
		p.parseIdent()
		expr.NoOptimize = true
	}

	expr.Lparen = p.expect(token.LPAREN)

	// Parse the inner expression as a full pipe expression
	expr.Query = p.parseExpr()

	expr.Rparen = p.expect(token.RPAREN)

	return expr
}

// parseToTableExpr parses a totable expression.
// Syntax: totable([kind=nooptimization] (pipe_expression))
func (p *Parser) parseToTableExpr() *ast.ToTableExpr {
	pos := p.pos
	p.next() // consume 'totable'

	expr := &ast.ToTableExpr{ToTable: pos}

	// Check for optional kind=nooptimization
	if p.tok == token.KIND {
		p.next()
		p.expect(token.ASSIGN)
		// Expect 'nooptimization' identifier
		p.parseIdent()
		expr.NoOptimize = true
	}

	expr.Lparen = p.expect(token.LPAREN)

	// Parse the inner expression as a full pipe expression
	expr.Query = p.parseExpr()

	expr.Rparen = p.expect(token.RPAREN)

	return expr
}

// parseMaterializeExpr parses a materialize expression.
// Syntax: materialize(pipe_expression)
func (p *Parser) parseMaterializeExpr() *ast.MaterializeExpr {
	pos := p.pos
	p.next() // consume 'materialize'

	expr := &ast.MaterializeExpr{Materialize: pos}

	expr.Lparen = p.expect(token.LPAREN)

	// Parse the inner expression as a full pipe expression
	expr.Query = p.parseExpr()

	expr.Rparen = p.expect(token.RPAREN)

	return expr
}

// parseNamedExpr parses an optionally named expression.
// Also handles tuple unpacking: (A, B) = expr
func (p *Parser) parseNamedExpr() ast.Expr {
	// Check for tuple unpacking: (A, B, ...) = expr
	if p.tok == token.LPAREN {
		// Save state to restore if this isn't tuple unpacking
		savedOffset := p.lex.Offset()
		savedPos := p.pos
		savedTok := p.tok
		savedLit := p.lit

		p.next() // consume '('
		var names []*ast.Ident

		// Try to parse as tuple of identifiers
		for p.tok == token.IDENT || p.tok.IsKeyword() {
			names = append(names, p.parseIdent())
			if !p.accept(token.COMMA) {
				break
			}
		}

		if p.tok == token.RPAREN && len(names) > 0 {
			p.next() // consume ')'
			if p.tok == token.ASSIGN {
				// This is tuple unpacking
				assignPos := p.pos
				p.next()
				expr := p.parseExpr()
				return &ast.NamedExpr{Names: names, Assign: assignPos, Expr: expr}
			}
		}

		// Not tuple unpacking, restore state and parse as regular expression
		p.lex.Reset(savedOffset)
		p.pos = savedPos
		p.tok = savedTok
		p.lit = savedLit
		return p.parseExpr()
	}

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
		token.TILDE, token.NTILDE, token.COLON,
		// Positive string operators
		token.CONTAINS, token.CONTAINSCS,
		token.STARTSWITH, token.STARTSWITHCS,
		token.ENDSWITH, token.ENDSWITHCS,
		token.HAS, token.HASCS, token.HASALL, token.HASANY,
		token.HASPREFIX, token.HASPREFIXCS,
		token.HASSUFFIX, token.HASSUFFIXCS,
		token.LIKE, token.LIKECS, token.MATCHESREGEX,
		// Negated string operators
		token.NOTCONTAINS, token.NOTCONTAINSCS,
		token.NOTSTARTSWITH, token.NOTSTARTSWITCS,
		token.NOTENDSWITH, token.NOTENDSWITHCS,
		token.NOTHAS, token.NOTHASCS,
		token.NOTHASPREFIX, token.NOTHASPREFIXCS,
		token.NOTHASSUFFIX, token.NOTHASSUFFIXCS,
		token.NOTLIKE, token.NOTLIKECS:
		opPos := p.pos
		op := p.tok
		p.next()
		right := p.parseAddExpr()
		left = &ast.BinaryExpr{X: left, OpPos: opPos, Op: op, Y: right}
	case token.IN:
		left = p.parseInExpr(left, false)
	case token.NOTIN, token.NOTINCI:
		left = p.parseInExpr(left, true)
	case token.BETWEEN:
		left = p.parseBetweenExpr(left, false)
	case token.NOTBETWEEN:
		left = p.parseBetweenExpr(left, true)
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
