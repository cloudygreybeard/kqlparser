package parser

import (
	"github.com/cloudygreybeard/kqlparser/ast"
	"github.com/cloudygreybeard/kqlparser/token"
)

// parseOperator parses a piped query operator.
func (p *Parser) parseOperator() ast.Operator {
	pipePos := p.pos
	p.next() // consume '|'

	switch p.tok {
	case token.WHERE, token.FILTER:
		return p.parseWhereOp(pipePos)
	case token.PROJECT:
		return p.parseProjectOp(pipePos)
	case token.PROJECT_AWAY:
		return p.parseProjectAwayOp(pipePos)
	case token.EXTEND:
		return p.parseExtendOp(pipePos)
	case token.SUMMARIZE:
		return p.parseSummarizeOp(pipePos)
	case token.SORT, token.ORDER:
		return p.parseSortOp(pipePos)
	case token.TAKE, token.LIMIT:
		return p.parseTakeOp(pipePos)
	case token.TOP:
		return p.parseTopOp(pipePos)
	case token.COUNT:
		return p.parseCountOp(pipePos)
	case token.DISTINCT:
		return p.parseDistinctOp(pipePos)
	case token.JOIN:
		return p.parseJoinOp(pipePos)
	case token.UNION:
		return p.parseUnionOp(pipePos)
	case token.RENDER:
		return p.parseRenderOp(pipePos)
	case token.PARSE:
		return p.parseParseOp(pipePos)
	case token.MVEXPAND:
		return p.parseMvExpandOp(pipePos)
	case token.SEARCH:
		return p.parseSearchOp(pipePos)
	case token.AS:
		return p.parseAsOp(pipePos)
	case token.GETSCHEMA:
		return p.parseGetSchemaOp(pipePos)
	case token.SERIALIZE:
		return p.parseSerializeOp(pipePos)
	case token.INVOKE:
		return p.parseInvokeOp(pipePos)
	default:
		return p.parseGenericOp(pipePos)
	}
}

// parseWhereOp parses a where operator.
func (p *Parser) parseWhereOp(pipePos token.Pos) *ast.WhereOp {
	wherePos := p.pos
	p.next()                         // consume 'where' or 'filter'
	predicate := p.parseExprNoPipe() // Don't consume following pipes
	return &ast.WhereOp{Pipe: pipePos, Where: wherePos, Predicate: predicate}
}

// parseProjectOp parses a project operator.
func (p *Parser) parseProjectOp(pipePos token.Pos) *ast.ProjectOp {
	projectPos := p.pos
	p.next() // consume 'project'

	columns := p.parseNamedExprList()
	return &ast.ProjectOp{Pipe: pipePos, Project: projectPos, Columns: columns}
}

// parseProjectAwayOp parses a project-away operator.
func (p *Parser) parseProjectAwayOp(pipePos token.Pos) *ast.ProjectAwayOp {
	projectAwayPos := p.pos
	p.next() // consume 'project-away'

	var columns []*ast.Ident
	for p.tok == token.IDENT || p.tok.IsKeyword() {
		columns = append(columns, p.parseIdent())
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.ProjectAwayOp{Pipe: pipePos, ProjectAway: projectAwayPos, Columns: columns}
}

// parseExtendOp parses an extend operator.
func (p *Parser) parseExtendOp(pipePos token.Pos) *ast.ExtendOp {
	extendPos := p.pos
	p.next() // consume 'extend'

	columns := p.parseNamedExprList()
	return &ast.ExtendOp{Pipe: pipePos, Extend: extendPos, Columns: columns}
}

// parseSummarizeOp parses a summarize operator.
func (p *Parser) parseSummarizeOp(pipePos token.Pos) *ast.SummarizeOp {
	summarizePos := p.pos
	p.next() // consume 'summarize'

	op := &ast.SummarizeOp{Pipe: pipePos, Summarize: summarizePos}

	// Parse aggregates (before 'by')
	for p.tok != token.BY && p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		expr := p.parseNamedExprSingle()
		op.Aggregates = append(op.Aggregates, expr)
		if !p.accept(token.COMMA) {
			break
		}
	}

	// Parse 'by' clause
	if p.tok == token.BY {
		op.ByPos = p.pos
		p.next() // consume 'by'
		op.GroupBy = p.parseNamedExprList()
	}

	return op
}

// parseSortOp parses a sort/order operator.
func (p *Parser) parseSortOp(pipePos token.Pos) *ast.SortOp {
	sortPos := p.pos
	p.next() // consume 'sort' or 'order'

	byPos := p.expect(token.BY)

	var orders []*ast.OrderExpr
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		expr := p.parseExprNoPipe()
		order := &ast.OrderExpr{Expr: expr}

		// Check for asc/desc
		if p.tok == token.ASC || p.tok == token.DESC {
			order.Order = p.tok
			p.next()
		}

		// Check for nulls first/last
		if p.tok == token.NULLS {
			p.next()
			if p.tok == token.FIRST || p.tok == token.LAST {
				order.Nulls = p.tok
				p.next()
			}
		}

		orders = append(orders, order)

		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.SortOp{Pipe: pipePos, Sort: sortPos, ByPos: byPos, Orders: orders}
}

// parseTakeOp parses a take/limit operator.
func (p *Parser) parseTakeOp(pipePos token.Pos) *ast.TakeOp {
	takePos := p.pos
	p.next() // consume 'take' or 'limit'

	count := p.parseExprNoPipe()
	return &ast.TakeOp{Pipe: pipePos, Take: takePos, Count: count}
}

// parseTopOp parses a top operator.
func (p *Parser) parseTopOp(pipePos token.Pos) *ast.TopOp {
	topPos := p.pos
	p.next() // consume 'top'

	count := p.parseExprNoPipe()
	byPos := p.expect(token.BY)

	expr := p.parseExprNoPipe()
	order := &ast.OrderExpr{Expr: expr}

	// Check for asc/desc
	if p.tok == token.ASC || p.tok == token.DESC {
		order.Order = p.tok
		p.next()
	}

	return &ast.TopOp{Pipe: pipePos, Top: topPos, Count: count, ByPos: byPos, ByExpr: order}
}

// parseCountOp parses a count operator.
func (p *Parser) parseCountOp(pipePos token.Pos) *ast.CountOp {
	countPos := p.pos
	p.next() // consume 'count'
	return &ast.CountOp{Pipe: pipePos, Count: countPos}
}

// parseDistinctOp parses a distinct operator.
func (p *Parser) parseDistinctOp(pipePos token.Pos) *ast.DistinctOp {
	distinctPos := p.pos
	p.next() // consume 'distinct'

	var columns []ast.Expr
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		if p.tok == token.MUL {
			columns = append(columns, &ast.StarExpr{Star: p.pos})
			p.next()
		} else {
			columns = append(columns, p.parseExpr())
		}
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.DistinctOp{Pipe: pipePos, Distinct: distinctPos, Columns: columns}
}

// parseJoinOp parses a join operator.
func (p *Parser) parseJoinOp(pipePos token.Pos) *ast.JoinOp {
	joinPos := p.pos
	p.next() // consume 'join'

	op := &ast.JoinOp{Pipe: pipePos, Join: joinPos, Kind: ast.JoinInner}

	// Check for kind parameter
	if p.tok == token.KIND {
		p.next()
		p.expect(token.ASSIGN)
		kind := p.parseIdent()
		op.Kind = parseJoinKind(kind.Name)
	}

	// Parse right side
	op.Right = p.parseUnaryExpr()

	// Check for 'on' clause
	if p.tok == token.ON {
		op.OnPos = p.pos
		p.next()

		for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
			cond := p.parseExprNoPipe()
			op.OnExpr = append(op.OnExpr, cond)
			if !p.accept(token.COMMA) {
				break
			}
		}
	}

	return op
}

// parseJoinKind parses a join kind from its string representation.
func parseJoinKind(s string) ast.JoinKind {
	switch s {
	case "inner", "innerunique":
		return ast.JoinInner
	case "leftouter":
		return ast.JoinLeftOuter
	case "rightouter":
		return ast.JoinRightOuter
	case "fullouter":
		return ast.JoinFullOuter
	case "leftsemi":
		return ast.JoinLeftSemi
	case "rightsemi":
		return ast.JoinRightSemi
	case "leftanti", "anti":
		return ast.JoinLeftAnti
	case "rightanti":
		return ast.JoinRightAnti
	default:
		return ast.JoinInner
	}
}

// parseUnionOp parses a union operator.
func (p *Parser) parseUnionOp(pipePos token.Pos) *ast.UnionOp {
	unionPos := p.pos
	p.next() // consume 'union'

	var tables []ast.Expr
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		table := p.parseUnaryExpr()
		tables = append(tables, table)
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.UnionOp{Pipe: pipePos, Union: unionPos, Tables: tables}
}

// parseRenderOp parses a render operator.
func (p *Parser) parseRenderOp(pipePos token.Pos) *ast.RenderOp {
	renderPos := p.pos
	p.next() // consume 'render'

	chartType := p.parseIdent()

	// TODO: parse with clause

	return &ast.RenderOp{Pipe: pipePos, Render: renderPos, ChartType: chartType}
}

// parseParseOp parses a parse operator.
func (p *Parser) parseParseOp(pipePos token.Pos) *ast.ParseOp {
	parsePos := p.pos
	p.next() // consume 'parse'

	source := p.parseExpr()

	withPos := p.expect(token.WITH)
	pattern := p.parsePrimaryExpr() // Usually a string with parse pattern

	return &ast.ParseOp{Pipe: pipePos, Parse: parsePos, Source: source, WithPos: withPos, Pattern: pattern}
}

// parseMvExpandOp parses an mv-expand operator.
func (p *Parser) parseMvExpandOp(pipePos token.Pos) *ast.MvExpandOp {
	mvExpandPos := p.pos
	p.next() // consume 'mv-expand'

	var columns []ast.Expr
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		col := p.parseExpr()
		columns = append(columns, col)
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.MvExpandOp{Pipe: pipePos, MvExpand: mvExpandPos, Columns: columns}
}

// parseSearchOp parses a search operator.
func (p *Parser) parseSearchOp(pipePos token.Pos) *ast.SearchOp {
	searchPos := p.pos
	p.next() // consume 'search'

	predicate := p.parseExprNoPipe()
	return &ast.SearchOp{Pipe: pipePos, Search: searchPos, Predicate: predicate}
}

// parseAsOp parses an as operator (returns as GenericOp for now).
func (p *Parser) parseAsOp(pipePos token.Pos) *ast.GenericOp {
	asPos := p.pos
	p.next() // consume 'as'

	var content []ast.Expr
	content = append(content, p.parseIdent())

	return &ast.GenericOp{
		Pipe:    pipePos,
		OpPos:   asPos,
		OpName:  "as",
		Content: content,
		EndPos:  p.pos,
	}
}

// parseGetSchemaOp parses a getschema operator.
func (p *Parser) parseGetSchemaOp(pipePos token.Pos) *ast.GenericOp {
	opPos := p.pos
	p.next() // consume 'getschema'

	return &ast.GenericOp{
		Pipe:   pipePos,
		OpPos:  opPos,
		OpName: "getschema",
		EndPos: p.pos,
	}
}

// parseSerializeOp parses a serialize operator.
func (p *Parser) parseSerializeOp(pipePos token.Pos) *ast.GenericOp {
	opPos := p.pos
	p.next() // consume 'serialize'

	var content []ast.Expr
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		expr := p.parseNamedExprSingle()
		content = append(content, expr)
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.GenericOp{
		Pipe:    pipePos,
		OpPos:   opPos,
		OpName:  "serialize",
		Content: content,
		EndPos:  p.pos,
	}
}

// parseInvokeOp parses an invoke operator.
func (p *Parser) parseInvokeOp(pipePos token.Pos) *ast.GenericOp {
	opPos := p.pos
	p.next() // consume 'invoke'

	var content []ast.Expr
	// Parse the function call
	content = append(content, p.parsePostfixExpr())

	return &ast.GenericOp{
		Pipe:    pipePos,
		OpPos:   opPos,
		OpName:  "invoke",
		Content: content,
		EndPos:  p.pos,
	}
}

// parseGenericOp parses an unrecognized operator.
func (p *Parser) parseGenericOp(pipePos token.Pos) *ast.GenericOp {
	opPos := p.pos
	opName := p.lit
	p.next()

	var content []ast.Expr
	// Consume until next pipe or end
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		expr := p.parseExpr()
		if expr != nil {
			content = append(content, expr)
		}
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.GenericOp{
		Pipe:    pipePos,
		OpPos:   opPos,
		OpName:  opName,
		Content: content,
		EndPos:  p.pos,
	}
}

// parseNamedExprList parses a comma-separated list of named expressions.
func (p *Parser) parseNamedExprList() []*ast.NamedExpr {
	var exprs []*ast.NamedExpr

	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI && p.tok != token.BY {
		expr := p.parseNamedExprSingle()
		exprs = append(exprs, expr)
		if !p.accept(token.COMMA) {
			break
		}
	}

	return exprs
}

// parseNamedExprSingle parses a single named expression.
func (p *Parser) parseNamedExprSingle() *ast.NamedExpr {
	// Check if this is a named expression (name = expr)
	if p.tok == token.IDENT {
		name := p.parseIdent()
		if p.tok == token.ASSIGN {
			assignPos := p.pos
			p.next()
			expr := p.parseExprNoPipe()
			return &ast.NamedExpr{Name: name, Assign: assignPos, Expr: expr}
		}
		// Not a named expression, treat as expression
		expr := p.continueParsingExprNoPipe(name)
		return &ast.NamedExpr{Expr: expr}
	}

	expr := p.parseExprNoPipe()
	return &ast.NamedExpr{Expr: expr}
}

// parseExprNoPipe parses an expression without allowing pipe operators.
func (p *Parser) parseExprNoPipe() ast.Expr {
	return p.parseOrExpr()
}

// continueParsingExprNoPipe continues parsing given an identifier, without pipe.
func (p *Parser) continueParsingExprNoPipe(ident *ast.Ident) ast.Expr {
	x := p.continuePostfixExpr(ident)
	return p.continueBinaryExpr(x)
}
