package parser

import (
	"strings"

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
	case token.PROJECTAWAY:
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
	case token.PARSEWHERE:
		return p.parseParseWhereOp(pipePos)
	case token.PARSEKV:
		return p.parseParseKvOp(pipePos)
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
	case token.PROJECTRENAME:
		return p.parseProjectRenameOp(pipePos)
	case token.PROJECTREORDER:
		return p.parseProjectReorderOp(pipePos)
	case token.SAMPLE:
		return p.parseSampleOp(pipePos)
	case token.SAMPLEDISTINCT:
		return p.parseSampleDistinctOp(pipePos)
	case token.LOOKUP:
		return p.parseLookupOp(pipePos)
	case token.MAKESERIES:
		return p.parseMakeSeriesOp(pipePos)
	case token.SCAN:
		return p.parseScanOp(pipePos)
	case token.CONSUME:
		return p.parseConsumeOp(pipePos)
	case token.EVALUATE:
		return p.parseEvaluateOp(pipePos)
	case token.REDUCE:
		return p.parseReduceOp(pipePos)
	case token.FORK:
		return p.parseForkOp(pipePos)
	case token.FACET:
		return p.parseFacetOp(pipePos)
	case token.PROJECTKEEP:
		return p.parseProjectKeepOp(pipePos)
	case token.TOPNESTED:
		return p.parseTopNestedOp(pipePos)
	case token.TOPHITTERS:
		return p.parseTopHittersOp(pipePos)
	case token.MVAPPLY:
		return p.parseMvApplyOp(pipePos)
	case token.FIND:
		return p.parseFindOp(pipePos)
	case token.MAKEGRAPH:
		return p.parseMakeGraphOp(pipePos)
	case token.GRAPHMATCH:
		return p.parseGraphMatchOp(pipePos)
	case token.GRAPHSHORTESTPATHS:
		return p.parseGraphShortestPathsOp(pipePos)
	case token.GRAPHMARKCOMPONENTS:
		return p.parseGraphMarkComponentsOp(pipePos)
	case token.GRAPHTOTABLE:
		return p.parseGraphToTableOp(pipePos)
	case token.GRAPHWHERENODES:
		return p.parseGraphWhereNodesOp(pipePos)
	case token.GRAPHWHEREEDGES:
		return p.parseGraphWhereEdgesOp(pipePos)
	case token.EXECUTEANDCACHE:
		return p.parseExecuteAndCacheOp(pipePos)
	case token.ASSERTSCHEMA:
		return p.parseAssertSchemaOp(pipePos)
	case token.MACROEXPAND:
		return p.parseMacroExpandOp(pipePos)
	case token.PARTITIONBY:
		return p.parsePartitionByOp(pipePos)
	default:
		return p.parseGenericOp(pipePos)
	}
}

// parseOperatorDirect parses an operator without a leading pipe.
// This is used for contextual subexpressions inside mv-apply, toscalar, etc.
func (p *Parser) parseOperatorDirect() ast.Operator {
	// Use token.NoPos for the pipe position since there's no leading pipe
	pipePos := token.NoPos

	switch p.tok {
	case token.WHERE, token.FILTER:
		return p.parseWhereOp(pipePos)
	case token.PROJECT:
		return p.parseProjectOp(pipePos)
	case token.PROJECTAWAY:
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
	case token.PARSEWHERE:
		return p.parseParseWhereOp(pipePos)
	case token.PARSEKV:
		return p.parseParseKvOp(pipePos)
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
	case token.PROJECTRENAME:
		return p.parseProjectRenameOp(pipePos)
	case token.PROJECTREORDER:
		return p.parseProjectReorderOp(pipePos)
	case token.SAMPLE:
		return p.parseSampleOp(pipePos)
	case token.SAMPLEDISTINCT:
		return p.parseSampleDistinctOp(pipePos)
	case token.LOOKUP:
		return p.parseLookupOp(pipePos)
	case token.MAKESERIES:
		return p.parseMakeSeriesOp(pipePos)
	case token.SCAN:
		return p.parseScanOp(pipePos)
	case token.CONSUME:
		return p.parseConsumeOp(pipePos)
	case token.EVALUATE:
		return p.parseEvaluateOp(pipePos)
	case token.REDUCE:
		return p.parseReduceOp(pipePos)
	case token.FORK:
		return p.parseForkOp(pipePos)
	case token.FACET:
		return p.parseFacetOp(pipePos)
	case token.PROJECTKEEP:
		return p.parseProjectKeepOp(pipePos)
	case token.TOPNESTED:
		return p.parseTopNestedOp(pipePos)
	case token.TOPHITTERS:
		return p.parseTopHittersOp(pipePos)
	case token.MVAPPLY:
		return p.parseMvApplyOp(pipePos)
	case token.FIND:
		return p.parseFindOp(pipePos)
	case token.MAKEGRAPH:
		return p.parseMakeGraphOp(pipePos)
	case token.GRAPHMATCH:
		return p.parseGraphMatchOp(pipePos)
	case token.GRAPHSHORTESTPATHS:
		return p.parseGraphShortestPathsOp(pipePos)
	case token.GRAPHMARKCOMPONENTS:
		return p.parseGraphMarkComponentsOp(pipePos)
	case token.GRAPHTOTABLE:
		return p.parseGraphToTableOp(pipePos)
	case token.GRAPHWHERENODES:
		return p.parseGraphWhereNodesOp(pipePos)
	case token.GRAPHWHEREEDGES:
		return p.parseGraphWhereEdgesOp(pipePos)
	case token.EXECUTEANDCACHE:
		return p.parseExecuteAndCacheOp(pipePos)
	case token.ASSERTSCHEMA:
		return p.parseAssertSchemaOp(pipePos)
	case token.MACROEXPAND:
		return p.parseMacroExpandOp(pipePos)
	case token.PARTITIONBY:
		return p.parsePartitionByOp(pipePos)
	default:
		// Fall back to parsing as an expression (for simple expr-based subqueries)
		return nil
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

	// Parse operator parameters (hints)
	op.Params = p.parseOperatorParams()

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

	op := &ast.SortOp{Pipe: pipePos, Sort: sortPos}

	// Parse operator parameters (hints)
	op.Params = p.parseOperatorParams()

	byPos := p.expect(token.BY)
	op.ByPos = byPos

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

	op.Orders = orders
	return op
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

// parseOperatorParams parses operator parameters like kind=inner, hint.strategy=broadcast.
// Parameters have the form: name[.subname]* = value
// Returns when a non-parameter token is encountered.
func (p *Parser) parseOperatorParams() []*ast.OperatorParam {
	var params []*ast.OperatorParam

	for {
		// Check if this looks like a parameter (identifier or keyword followed by = or .)
		if p.tok != token.IDENT && p.tok != token.KIND && !p.tok.IsKeyword() {
			break
		}

		// Look ahead to see if this is a parameter (name = value or name.subname = value)
		if !p.isOperatorParam() {
			break
		}

		// Parse parameter name (possibly dotted like hint.strategy)
		name := p.parseIdent()

		// Handle dotted names (e.g., hint.strategy)
		for p.tok == token.DOT {
			p.next() // consume '.'
			suffix := p.parseIdent()
			name = &ast.Ident{
				NamePos: name.NamePos,
				Name:    name.Name + "." + suffix.Name,
			}
		}

		// Expect '='
		assignPos := p.expect(token.ASSIGN)

		// Parse parameter value (identifier or literal)
		var value ast.Expr
		switch p.tok {
		case token.INT, token.REAL, token.STRING, token.BOOL:
			value = p.parseLiteral()
		default:
			value = p.parseIdent()
		}

		params = append(params, &ast.OperatorParam{
			Name:   name,
			Assign: assignPos,
			Value:  value,
		})
	}

	return params
}

// isOperatorParam checks if the current position looks like an operator parameter.
// Only returns true for known parameter patterns:
// - kind=...
// - hint.*=...
// - withsource=..., isfuzzy=..., bagexpansion=..., decodeblocks=..., etc.
func (p *Parser) isOperatorParam() bool {
	// Check for keyword tokens that are known parameters
	switch p.tok {
	case token.KIND, token.WITHSOURCE:
		// Check if followed by '='
		savedOffset := p.lex.Offset()
		savedPos := p.pos
		savedTok := p.tok
		savedLit := p.lit

		p.next()
		isParam := p.tok == token.ASSIGN

		// Restore state
		p.lex.Reset(savedOffset)
		p.pos = savedPos
		p.tok = savedTok
		p.lit = savedLit
		return isParam
	}

	// For identifiers, check if it's a known parameter pattern
	if p.tok == token.IDENT {
		name := strings.ToLower(p.lit)

		// Check for hint.* pattern
		if name == "hint" {
			// Save state and check for dot
			savedOffset := p.lex.Offset()
			savedPos := p.pos
			savedTok := p.tok
			savedLit := p.lit

			p.next()
			if p.tok == token.DOT {
				p.next() // consume '.'
				if p.tok == token.IDENT || p.tok.IsKeyword() {
					p.next() // consume identifier
					if p.tok == token.ASSIGN {
						// Restore and return true
						p.lex.Reset(savedOffset)
						p.pos = savedPos
						p.tok = savedTok
						p.lit = savedLit
						return true
					}
				}
			}
			// Restore state
			p.lex.Reset(savedOffset)
			p.pos = savedPos
			p.tok = savedTok
			p.lit = savedLit
			return false
		}

		// Check for known simple parameter names (as identifiers)
		switch name {
		case "isfuzzy", "bagexpansion", "decodeblocks", "expandoutput",
			"with_itemindex", "with_match_id", "with_step_name",
			"with_component_id", "with_node_id":
			// Check if followed by '='
			savedOffset := p.lex.Offset()
			savedPos := p.pos
			savedTok := p.tok
			savedLit := p.lit

			p.next()
			isParam := p.tok == token.ASSIGN

			// Restore state
			p.lex.Reset(savedOffset)
			p.pos = savedPos
			p.tok = savedTok
			p.lit = savedLit
			return isParam
		}
	}

	return false
}

// parseJoinOp parses a join operator.
func (p *Parser) parseJoinOp(pipePos token.Pos) *ast.JoinOp {
	joinPos := p.pos
	p.next() // consume 'join'

	op := &ast.JoinOp{Pipe: pipePos, Join: joinPos, Kind: ast.JoinInner}

	// Parse operator parameters (kind, hints, etc.)
	op.Params = p.parseOperatorParams()

	// Extract kind from parameters
	for _, param := range op.Params {
		if param.Name.Name == "kind" {
			if ident, ok := param.Value.(*ast.Ident); ok {
				op.Kind = parseJoinKind(ident.Name)
			}
		}
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

	op := &ast.UnionOp{Pipe: pipePos, Union: unionPos}

	// Parse operator parameters (kind, withsource, isfuzzy, etc.)
	op.Params = p.parseOperatorParams()

	// Parse tables
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		table := p.parseUnaryExpr()
		op.Tables = append(op.Tables, table)
		if !p.accept(token.COMMA) {
			break
		}
	}

	return op
}

// parseRenderOp parses a render operator.
func (p *Parser) parseRenderOp(pipePos token.Pos) *ast.RenderOp {
	renderPos := p.pos
	p.next() // consume 'render'

	chartType := p.parseIdent()

	op := &ast.RenderOp{Pipe: pipePos, Render: renderPos, ChartType: chartType}

	// Parse with clause if present
	if p.tok == token.WITH {
		op.WithPos = p.pos
		p.next() // consume 'with'

		p.expect(token.LPAREN)

		// Parse properties: name = value, name = value, ...
		for p.tok != token.RPAREN && p.tok != token.EOF {
			prop := &ast.RenderProperty{
				Name: p.parseIdent(),
			}
			prop.Assign = p.expect(token.ASSIGN)

			// Parse value (can be identifier, string, number, or list)
			prop.Value = p.parseExprNoPipe()

			op.Properties = append(op.Properties, prop)

			if !p.accept(token.COMMA) {
				break
			}
		}

		p.expect(token.RPAREN)
	}

	return op
}

// parseParseOp parses a parse operator.
// Syntax: parse [kind=simple|regex|relaxed] Source with Pattern
// Pattern: [LeadingColumn] (["*"] StringLiteral [Column])* ["*"]
// Column: Name[:Type]
func (p *Parser) parseParseOp(pipePos token.Pos) *ast.ParseOp {
	parsePos := p.pos
	p.next() // consume 'parse'

	op := &ast.ParseOp{Pipe: pipePos, Parse: parsePos}

	// Check for kind clause
	if p.tok == token.KIND {
		p.next()
		p.expect(token.ASSIGN)
		kind := p.parseIdent()
		op.Kind = kind.Name
	}

	// Parse source expression
	op.Source = p.parseUnaryExpr()

	// Expect 'with'
	op.WithPos = p.expect(token.WITH)

	// Parse pattern - can be:
	// 1. Simple string literal (legacy)
	// 2. Complex pattern with column captures and type annotations

	// Check if first token is an identifier (leading column)
	if p.tok == token.IDENT {
		op.LeadingCol = p.parseParseColumn()
	}

	// Parse segments: ["*"] StringLiteral [Column]
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		seg := &ast.ParsePatternSegment{}

		// Check for leading star
		if p.tok == token.MUL {
			seg.Star = true
			p.next()
		}

		// Check for trailing star (end of pattern)
		if p.tok == token.PIPE || p.tok == token.EOF || p.tok == token.SEMI {
			if seg.Star {
				op.TrailingStar = true
			}
			break
		}

		// Expect string literal delimiter
		if p.tok == token.STRING {
			seg.Text = &ast.BasicLit{
				ValuePos: p.pos,
				Kind:     p.tok,
				Value:    p.lit,
			}
			p.next()

			// Check for column after delimiter
			if p.tok == token.IDENT {
				seg.Column = p.parseParseColumn()
			}

			op.Segments = append(op.Segments, seg)
		} else if seg.Star {
			// Star without following string - trailing star
			op.TrailingStar = true
			break
		} else {
			// Unexpected token
			break
		}
	}

	return op
}

// parseParseColumn parses a column capture: Name[:Type]
func (p *Parser) parseParseColumn() *ast.ParseColumn {
	col := &ast.ParseColumn{
		Name: p.parseIdent(),
	}

	// Check for type annotation
	if p.tok == token.COLON {
		p.next()
		col.Type = p.parseIdent()
	}

	return col
}

// parseMvExpandOp parses an mv-expand operator.
func (p *Parser) parseMvExpandOp(pipePos token.Pos) *ast.MvExpandOp {
	mvExpandPos := p.pos
	p.next() // consume 'mv-expand'

	op := &ast.MvExpandOp{Pipe: pipePos, MvExpand: mvExpandPos}

	// Parse parameters (bagexpansion, with_itemindex)
	op.Params = p.parseOperatorParams()

	// Parse optional limit
	if p.tok == token.LIMIT {
		op.LimitPos = p.pos
		p.next()
		op.Limit = p.parseLiteral()
	}

	// Parse columns with optional name, assignment, and type annotation
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		col := &ast.MvExpandColumn{}

		// Check for named column (name = expr)
		if p.tok == token.IDENT {
			// Look ahead to see if there's an =
			savedOffset := p.lex.Offset()
			savedPos := p.pos
			savedTok := p.tok
			savedLit := p.lit

			name := p.parseIdent()
			if p.tok == token.ASSIGN {
				col.Name = name
				col.Assign = p.pos
				p.next() // consume '='
				col.Expr = p.parseUnaryExpr()
			} else {
				// Not a named column, restore and parse as expression
				p.lex.Reset(savedOffset)
				p.pos = savedPos
				p.tok = savedTok
				p.lit = savedLit
				col.Expr = p.parseUnaryExpr()
			}
		} else {
			col.Expr = p.parseUnaryExpr()
		}

		// Check for 'to typeof(type)'
		if p.tok == token.TO {
			col.ToPos = p.pos
			p.next()
			col.Type = p.parseUnaryExpr() // typeof(long)
		}

		op.Columns = append(op.Columns, col)

		if !p.accept(token.COMMA) {
			break
		}
	}

	return op
}

// parseSearchOp parses a search operator.
func (p *Parser) parseSearchOp(pipePos token.Pos) *ast.SearchOp {
	searchPos := p.pos
	p.next() // consume 'search'

	predicate := p.parseExprNoPipe()
	return &ast.SearchOp{Pipe: pipePos, Search: searchPos, Predicate: predicate}
}

// parseAsOp parses an as operator.
// Syntax: as [params] Name
func (p *Parser) parseAsOp(pipePos token.Pos) *ast.AsOp {
	asPos := p.pos
	p.next() // consume 'as'

	op := &ast.AsOp{Pipe: pipePos, As: asPos}

	// Parse optional parameters (e.g., hint.materialized=true)
	op.Params = p.parseOperatorParams()

	// Parse the name
	op.Name = p.parseIdent()

	return op
}

// parseGetSchemaOp parses a getschema operator.
func (p *Parser) parseGetSchemaOp(pipePos token.Pos) *ast.GetSchemaOp {
	opPos := p.pos
	p.next() // consume 'getschema'

	return &ast.GetSchemaOp{
		Pipe:      pipePos,
		GetSchema: opPos,
	}
}

// parseSerializeOp parses a serialize operator.
func (p *Parser) parseSerializeOp(pipePos token.Pos) *ast.SerializeOp {
	opPos := p.pos
	p.next() // consume 'serialize'

	var columns []*ast.NamedExpr
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		expr := p.parseNamedExprSingle()
		columns = append(columns, expr)
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.SerializeOp{
		Pipe:      pipePos,
		Serialize: opPos,
		Columns:   columns,
	}
}

// parseInvokeOp parses an invoke operator.
func (p *Parser) parseInvokeOp(pipePos token.Pos) *ast.InvokeOp {
	opPos := p.pos
	p.next() // consume 'invoke'

	// Parse the function call
	expr := p.parsePostfixExpr()
	var funcCall *ast.CallExpr
	if call, ok := expr.(*ast.CallExpr); ok {
		funcCall = call
	} else {
		// Wrap as a call if just an identifier
		funcCall = &ast.CallExpr{Fun: expr, Lparen: expr.Pos(), Rparen: expr.End()}
	}

	return &ast.InvokeOp{
		Pipe:     pipePos,
		Invoke:   opPos,
		Function: funcCall,
	}
}

// parseProjectRenameOp parses a project-rename operator.
func (p *Parser) parseProjectRenameOp(pipePos token.Pos) *ast.ProjectRenameOp {
	opPos := p.pos
	p.next() // consume 'project-rename'

	var columns []*ast.RenameExpr
	for p.tok == token.IDENT || p.tok.IsKeyword() {
		newName := p.parseIdent()
		assignPos := p.expect(token.ASSIGN)
		oldName := p.parseIdent()
		columns = append(columns, &ast.RenameExpr{
			NewName: newName,
			Assign:  assignPos,
			OldName: oldName,
		})
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.ProjectRenameOp{
		Pipe:          pipePos,
		ProjectRename: opPos,
		Columns:       columns,
	}
}

// parseProjectReorderOp parses a project-reorder operator.
func (p *Parser) parseProjectReorderOp(pipePos token.Pos) *ast.ProjectReorderOp {
	opPos := p.pos
	p.next() // consume 'project-reorder'

	var columns []*ast.Ident
	for p.tok == token.IDENT || p.tok.IsKeyword() {
		columns = append(columns, p.parseIdent())
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.ProjectReorderOp{
		Pipe:           pipePos,
		ProjectReorder: opPos,
		Columns:        columns,
	}
}

// parseSampleOp parses a sample operator.
func (p *Parser) parseSampleOp(pipePos token.Pos) *ast.SampleOp {
	opPos := p.pos
	p.next() // consume 'sample'

	count := p.parseExprNoPipe()

	return &ast.SampleOp{
		Pipe:   pipePos,
		Sample: opPos,
		Count:  count,
	}
}

// parseSampleDistinctOp parses a sample-distinct operator.
func (p *Parser) parseSampleDistinctOp(pipePos token.Pos) *ast.SampleDistinctOp {
	opPos := p.pos
	p.next() // consume 'sample-distinct'

	count := p.parseExprNoPipe()
	ofPos := p.expect(token.OF)
	column := p.parseExprNoPipe()

	return &ast.SampleDistinctOp{
		Pipe:           pipePos,
		SampleDistinct: opPos,
		Count:          count,
		OfPos:          ofPos,
		Column:         column,
	}
}

// parseLookupOp parses a lookup operator.
func (p *Parser) parseLookupOp(pipePos token.Pos) *ast.LookupOp {
	opPos := p.pos
	p.next() // consume 'lookup'

	op := &ast.LookupOp{Pipe: pipePos, Lookup: opPos, Kind: ast.JoinLeftOuter}

	// Check for kind parameter
	if p.tok == token.KIND {
		p.next()
		p.expect(token.ASSIGN)
		kind := p.parseIdent()
		op.Kind = parseJoinKind(kind.Name)
	}

	// Parse table
	op.Table = p.parseUnaryExpr()

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

// parseMakeSeriesOp parses a make-series operator.
func (p *Parser) parseMakeSeriesOp(pipePos token.Pos) *ast.MakeSeriesOp {
	opPos := p.pos
	p.next() // consume 'make-series'

	op := &ast.MakeSeriesOp{Pipe: pipePos, MakeSeries: opPos}

	// Parse aggregates until 'on' (each can have default=value)
	for p.tok != token.ON && p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		agg := &ast.MakeSeriesAggregation{
			Expr: p.parseNamedExprSingle(),
		}

		// Check for optional 'default = value'
		if p.tok == token.DEFAULT {
			agg.DefaultPos = p.pos
			p.next()
			p.expect(token.ASSIGN)
			agg.Default = p.parseExprNoPipe()
		}

		op.Aggregates = append(op.Aggregates, agg)
		if !p.accept(token.COMMA) {
			break
		}
	}

	// Parse 'on' column
	if p.tok == token.ON {
		op.OnPos = p.pos
		p.next()
		op.OnColumn = p.parseExprNoPipe()
	}

	// Parse optional 'from datetime(...)'
	if p.tok == token.FROM {
		op.FromPos = p.pos
		p.next()
		op.From = p.parseExprNoPipe()
	}

	// Parse optional 'to datetime(...)'
	if p.tok == token.TO {
		op.ToPos = p.pos
		p.next()
		op.To = p.parseExprNoPipe()
	}

	// Parse 'step interval'
	if p.tok == token.STEP {
		op.StepPos = p.pos
		p.next()
		op.Step = p.parseExprNoPipe()
	}

	// Parse optional 'in range(...)' - legacy syntax
	if p.tok == token.IN {
		inPos := p.pos
		p.next()
		if p.tok == token.IDENT && p.lit == "range" {
			rangePos := p.pos
			p.next()
			lparen := p.expect(token.LPAREN)
			start := p.parseExprNoPipe()
			p.expect(token.COMMA)
			stop := p.parseExprNoPipe()
			p.expect(token.COMMA)
			step := p.parseExprNoPipe()
			rparen := p.expect(token.RPAREN)
			op.InRange = &ast.InRangeExpr{
				InPos:  inPos,
				Range:  rangePos,
				Lparen: lparen,
				Start:  start,
				Stop:   stop,
				Step:   step,
				Rparen: rparen,
			}
		}
	}

	// Parse optional 'by' clause
	if p.tok == token.BY {
		op.ByPos = p.pos
		p.next()
		op.GroupBy = p.parseNamedExprList()
	}

	return op
}

// parseScanOp parses a scan operator.
// Syntax: scan [params] [order by ...] [partition by ...] [declare (...)] with (steps)
func (p *Parser) parseScanOp(pipePos token.Pos) *ast.ScanOp {
	opPos := p.pos
	p.next() // consume 'scan'

	op := &ast.ScanOp{Pipe: pipePos, Scan: opPos}

	// Parse optional parameters
	op.Params = p.parseOperatorParams()

	// Parse optional 'order by' clause
	if p.tok == token.ORDER {
		op.OrderByPos = p.pos
		p.next() // consume 'order'
		p.expect(token.BY)
		for {
			expr := p.parseExprNoPipe()
			ordered := &ast.OrderExpr{Expr: expr}
			if p.tok == token.ASC {
				ordered.Order = token.ASC
				p.next()
			} else if p.tok == token.DESC {
				ordered.Order = token.DESC
				p.next()
			}
			if p.tok == token.NULLS {
				p.next() // consume 'nulls'
				if p.tok == token.IDENT && p.lit == "first" {
					ordered.Nulls = token.IDENT // Mark as "first"
					p.next()
				} else if p.tok == token.IDENT && p.lit == "last" {
					ordered.Nulls = token.IDENT // Mark as "last"
					p.next()
				}
			}
			op.OrderBy = append(op.OrderBy, ordered)
			if !p.accept(token.COMMA) {
				break
			}
		}
	}

	// Parse optional 'partition by' clause
	if p.tok == token.PARTITION {
		op.PartitionPos = p.pos
		p.next() // consume 'partition'
		p.expect(token.BY)
		for {
			expr := p.parseExprNoPipe()
			op.PartitionBy = append(op.PartitionBy, expr)
			if !p.accept(token.COMMA) {
				break
			}
		}
	}

	// Parse optional 'declare' clause
	if p.tok == token.DECLARE {
		op.DeclarePos = p.pos
		p.next() // consume 'declare'
		if p.tok == token.LPAREN {
			p.next()
			for p.tok != token.RPAREN && p.tok != token.EOF {
				col := &ast.ColumnDeclExpr{Name: p.parseIdent()}
				if p.tok == token.COLON {
					col.Colon = p.pos
					p.next()
					col.Type = p.parseIdent()
				}
				op.Declare = append(op.Declare, col)
				if !p.accept(token.COMMA) {
					break
				}
			}
			p.expect(token.RPAREN)
		}
	}

	// Parse 'with (steps)'
	if p.tok == token.WITH {
		op.With = p.pos
		p.next()
	}

	if p.tok == token.LPAREN {
		op.Lparen = p.pos
		p.next()

		// Parse steps
		for p.tok != token.RPAREN && p.tok != token.EOF {
			step := p.parseScanStep()
			if step != nil {
				op.Steps = append(op.Steps, step)
			}
		}

		op.Rparen = p.pos
		p.expect(token.RPAREN)
	}

	return op
}

// parseScanStep parses a single step in a scan operator.
// Syntax: step Name [optional] [output=...] : Expr [=> assignments];
func (p *Parser) parseScanStep() *ast.ScanStep {
	if p.tok != token.STEP {
		p.errorf(p.pos, "expected 'step', got %s", p.tok)
		return nil
	}

	step := &ast.ScanStep{StepPos: p.pos}
	p.next() // consume 'step'

	// Parse step name
	step.Name = p.parseIdent()

	// Check for 'optional'
	if p.tok == token.IDENT && p.lit == "optional" {
		step.Optional = true
		p.next()
	}

	// Check for 'output = ...'
	if p.tok == token.IDENT && p.lit == "output" {
		step.OutputPos = p.pos
		p.next()
		p.expect(token.ASSIGN)
		if p.tok == token.IDENT || p.tok.IsKeyword() {
			step.OutputKind = p.lit
			p.next()
		}
	}

	// Parse ':' and condition
	step.Colon = p.expect(token.COLON)
	step.Condition = p.parseExprNoPipe()

	// Parse optional '=>' and assignments
	if p.tok == token.ARROW {
		step.Arrow = p.pos
		p.next()
		for {
			assign := &ast.ScanAssign{}
			assign.Name = p.parseIdent()
			assign.Assign = p.expect(token.ASSIGN)
			assign.Value = p.parseExprNoPipe()
			step.Assigns = append(step.Assigns, assign)
			if !p.accept(token.COMMA) {
				break
			}
		}
	}

	// Consume ';' at end of step
	p.accept(token.SEMI)

	return step
}

// parseConsumeOp parses a consume operator.
// Syntax: consume [params]
func (p *Parser) parseConsumeOp(pipePos token.Pos) *ast.ConsumeOp {
	opPos := p.pos
	p.next() // consume 'consume'

	op := &ast.ConsumeOp{Pipe: pipePos, Consume: opPos}

	// Parse optional parameters
	op.Params = p.parseOperatorParams()

	return op
}

// parseEvaluateOp parses an evaluate operator.
// Syntax: evaluate [params] plugin(...) [: (schema)]
func (p *Parser) parseEvaluateOp(pipePos token.Pos) *ast.EvaluateOp {
	opPos := p.pos
	p.next() // consume 'evaluate'

	op := &ast.EvaluateOp{Pipe: pipePos, Evaluate: opPos}

	// Parse optional parameters (e.g., hint.distribution=...)
	op.Params = p.parseOperatorParams()

	// Parse plugin call
	expr := p.parsePostfixExpr()
	if call, ok := expr.(*ast.CallExpr); ok {
		op.Plugin = call
	} else {
		op.Plugin = &ast.CallExpr{Fun: expr, Lparen: expr.Pos(), Rparen: expr.End()}
	}

	// Parse optional schema clause: : (col:type, ...)
	if p.tok == token.COLON {
		p.next() // consume ':'
		if p.tok == token.LPAREN {
			p.next() // consume '('
			for p.tok != token.RPAREN && p.tok != token.EOF {
				col := &ast.ColumnDeclExpr{Name: p.parseIdent()}
				if p.tok == token.COLON {
					col.Colon = p.pos
					p.next()
					col.Type = p.parseIdent()
				}
				op.Schema = append(op.Schema, col)
				if !p.accept(token.COMMA) {
					break
				}
			}
			p.expect(token.RPAREN)
		}
	}

	return op
}

// parseReduceOp parses a reduce operator.
func (p *Parser) parseReduceOp(pipePos token.Pos) *ast.ReduceOp {
	opPos := p.pos
	p.next() // consume 'reduce'

	byPos := p.expect(token.BY)
	column := p.parseExprNoPipe()

	return &ast.ReduceOp{
		Pipe:   pipePos,
		Reduce: opPos,
		ByPos:  byPos,
		Column: column,
	}
}

// parseForkOp parses a fork operator.
// Syntax: fork [name=(query)] [name=(query)] ...
func (p *Parser) parseForkOp(pipePos token.Pos) *ast.ForkOp {
	opPos := p.pos
	p.next() // consume 'fork'

	op := &ast.ForkOp{Pipe: pipePos, Fork: opPos}

	// Parse prongs: either (query) or name=(query)
	for p.tok == token.LPAREN || p.tok == token.IDENT || p.tok.IsKeyword() {
		prong := &ast.ForkProng{}

		// Check for optional name=
		if p.tok == token.IDENT || p.tok.IsKeyword() {
			// Save position to check for '='
			savedOffset := p.lex.Offset()
			savedPos := p.pos
			savedTok := p.tok
			savedLit := p.lit

			name := p.parseIdent()
			if p.tok == token.ASSIGN {
				p.next() // consume '='
				prong.Name = name
			} else {
				// Not a named fork, restore state
				p.lex.Reset(savedOffset)
				p.pos = savedPos
				p.tok = savedTok
				p.lit = savedLit
				break
			}
		}

		// Expect '('
		if p.tok != token.LPAREN {
			break
		}
		prong.Lparen = p.pos
		p.next() // consume '('

		// Parse inner query
		prong.Query = p.parseContextualSubExpr()

		prong.Rparen = p.expect(token.RPAREN)
		op.Prongs = append(op.Prongs, prong)
	}

	op.EndPos = p.pos
	return op
}

// parseFacetOp parses a facet operator.
// Syntax: facet by col1, col2 [with Operator | with (query)]
func (p *Parser) parseFacetOp(pipePos token.Pos) *ast.FacetOp {
	opPos := p.pos
	p.next() // consume 'facet'

	op := &ast.FacetOp{Pipe: pipePos, Facet: opPos}
	op.ByPos = p.expect(token.BY)

	// Parse columns
	for p.tok == token.IDENT || p.tok.IsKeyword() {
		op.Columns = append(op.Columns, p.parseIdent())
		if !p.accept(token.COMMA) {
			break
		}
	}

	// Optional 'with' clause
	if p.tok == token.WITH {
		op.With = p.pos
		p.next()

		// Two forms: with (query) or with Operator
		if p.tok == token.LPAREN {
			p.next() // consume '('
			op.Query = p.parseContextualSubExpr()
			p.expect(token.RPAREN)
		} else {
			// Parse as a direct operator expression
			expr := p.parseExpr()
			if pipe, ok := expr.(*ast.PipeExpr); ok {
				op.Query = pipe
			}
		}
	}

	return op
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

// parseProjectKeepOp parses a project-keep operator.
func (p *Parser) parseProjectKeepOp(pipePos token.Pos) *ast.ProjectKeepOp {
	opPos := p.pos
	p.next() // consume 'project-keep'

	var columns []*ast.Ident
	for p.tok == token.IDENT || p.tok.IsKeyword() {
		columns = append(columns, p.parseIdent())
		if !p.accept(token.COMMA) {
			break
		}
	}

	return &ast.ProjectKeepOp{
		Pipe:        pipePos,
		ProjectKeep: opPos,
		Columns:     columns,
	}
}

// parseTopNestedOp parses a top-nested operator.
func (p *Parser) parseTopNestedOp(pipePos token.Pos) *ast.TopNestedOp {
	opPos := p.pos
	p.next() // consume 'top-nested'

	op := &ast.TopNestedOp{Pipe: pipePos, TopNested: opPos}

	// Parse clauses: top-nested N of col by expr [with others = ...]
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		clause := &ast.TopNestedClause{}

		// Parse count
		clause.Count = p.parseExprNoPipe()

		// Parse 'of column'
		if p.tok == token.OF {
			clause.OfPos = p.pos
			p.next()
			clause.Column = p.parseExprNoPipe()
		}

		// Parse 'by expr'
		if p.tok == token.BY {
			clause.ByPos = p.pos
			p.next()
			clause.ByExpr = p.parseExprNoPipe()
		}

		// Parse optional 'with others = ...'
		if p.tok == token.WITH {
			clause.With = p.pos
			p.next()
			if p.tok == token.IDENT && p.lit == "others" {
				p.next()
				p.accept(token.ASSIGN)
				clause.Others = p.parseExprNoPipe()
			}
		}

		op.Clauses = append(op.Clauses, clause)

		if !p.accept(token.COMMA) {
			break
		}
	}

	op.EndPos = p.pos
	return op
}

// parseTopHittersOp parses a top-hitters operator.
func (p *Parser) parseTopHittersOp(pipePos token.Pos) *ast.TopHittersOp {
	opPos := p.pos
	p.next() // consume 'top-hitters'

	op := &ast.TopHittersOp{Pipe: pipePos, TopHitters: opPos}

	// Parse count
	op.Count = p.parseExprNoPipe()

	// Parse 'of column'
	if p.tok == token.OF {
		op.OfPos = p.pos
		p.next()
		op.Column = p.parseExprNoPipe()
	}

	// Parse optional 'by weight'
	if p.tok == token.BY {
		op.ByPos = p.pos
		p.next()
		op.ByExpr = p.parseExprNoPipe()
	}

	return op
}

// parseMvApplyOp parses an mv-apply operator.
func (p *Parser) parseMvApplyOp(pipePos token.Pos) *ast.MvApplyOp {
	opPos := p.pos
	p.next() // consume 'mv-apply'

	op := &ast.MvApplyOp{Pipe: pipePos, MvApply: opPos}

	// Parse parameters
	op.Params = p.parseOperatorParams()

	// Parse optional limit
	if p.tok == token.LIMIT {
		op.LimitPos = p.pos
		p.next()
		op.Limit = p.parseLiteral()
	}

	// Parse items until 'on' (columns with optional name and type)
	for p.tok != token.ON && p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		item := &ast.MvApplyColumn{}

		// Check for named column (name = expr)
		if p.tok == token.IDENT {
			// Look ahead to see if there's an =
			savedOffset := p.lex.Offset()
			savedPos := p.pos
			savedTok := p.tok
			savedLit := p.lit

			name := p.parseIdent()
			if p.tok == token.ASSIGN {
				item.Name = name
				item.Assign = p.pos
				p.next() // consume '='
				item.Expr = p.parseUnaryExpr()
			} else {
				// Not a named column, restore and parse as expression
				p.lex.Reset(savedOffset)
				p.pos = savedPos
				p.tok = savedTok
				p.lit = savedLit
				item.Expr = p.parseUnaryExpr()
			}
		} else {
			item.Expr = p.parseUnaryExpr()
		}

		// Check for 'to typeof(type)'
		if p.tok == token.TO {
			item.ToPos = p.pos
			p.next()
			item.Type = p.parseUnaryExpr() // typeof(long)
		}

		op.Items = append(op.Items, item)
		if !p.accept(token.COMMA) {
			break
		}
	}

	// Parse 'on' subquery
	if p.tok == token.ON {
		op.OnPos = p.pos
		p.next()

		// Expect '('
		if p.tok == token.LPAREN {
			p.next()
			// Parse inner query as a contextual subexpression (pipe expression)
			op.OnExpr = p.parseContextualSubExpr()
			p.expect(token.RPAREN)
		}
	}

	return op
}

// parseFindOp parses a find operator.
// Syntax: find [datascope] [params] [in (tables)] where expr [project cols] [project-smart] [project-away cols]
func (p *Parser) parseFindOp(pipePos token.Pos) *ast.FindOp {
	opPos := p.pos
	p.next() // consume 'find'

	op := &ast.FindOp{Pipe: pipePos, Find: opPos}

	// Parse optional parameters (including datascope)
	op.Params = p.parseOperatorParams()

	// Parse optional 'in (tables)'
	if p.tok == token.IN {
		op.InPos = p.pos
		p.next()
		if p.tok == token.LPAREN {
			p.next()
			for p.tok != token.RPAREN && p.tok != token.EOF {
				// Parse table name, possibly with wildcard suffix
				table := p.parseIdent()
				// Check for wildcard suffix (*)
				for p.tok == token.MUL {
					table.Name = table.Name + "*"
					p.next()
				}
				op.Tables = append(op.Tables, table)
				if !p.accept(token.COMMA) {
					break
				}
			}
			p.expect(token.RPAREN)
		}
	}

	// Parse 'where predicate'
	if p.tok == token.WHERE {
		op.WherePos = p.pos
		p.next()
		op.Predicate = p.parseExprNoPipe()
	}

	// Parse optional 'project columns' or 'project-smart'
	if p.tok == token.PROJECT {
		op.ProjectPos = p.pos
		p.next()
		op.Columns = p.parseFindColumns()
	} else if p.tok == token.PROJECTSMART {
		op.ProjectPos = p.pos
		op.ProjectSmart = true
		p.next()
	}

	// Parse optional 'project-away columns'
	if p.tok == token.PROJECTAWAY {
		op.ProjectAwayPos = p.pos
		p.next()
		if p.tok == token.MUL {
			op.ProjectAwayStar = true
			p.next()
		} else {
			op.ProjectAway = p.parseFindColumns()
		}
	}

	return op
}

// parseFindColumns parses find column list: col1, col2:type, pack(*)
func (p *Parser) parseFindColumns() []*ast.FindColumn {
	var cols []*ast.FindColumn
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI && p.tok != token.PROJECTAWAY {
		col := &ast.FindColumn{}

		// Check for pack(*)
		if p.tok == token.PACK {
			col.Pack = true
			col.Name = p.parseIdent()
			if p.tok == token.LPAREN {
				p.next()
				p.expect(token.MUL)
				p.expect(token.RPAREN)
			}
		} else {
			col.Name = p.parseIdent()
			// Optional :type
			if p.tok == token.COLON {
				col.Colon = p.pos
				p.next()
				col.Type = p.parseIdent()
			}
		}

		cols = append(cols, col)
		if !p.accept(token.COMMA) {
			break
		}
	}
	return cols
}

// parseParseWhereOp parses a parse-where operator.
func (p *Parser) parseParseWhereOp(pipePos token.Pos) *ast.ParseWhereOp {
	opPos := p.pos
	p.next() // consume 'parse-where'

	op := &ast.ParseWhereOp{Pipe: pipePos, ParseWhere: opPos}

	// Parse source expression
	op.Source = p.parseExprNoPipe()

	// Parse pattern (typically a string with wildcards)
	if p.tok == token.STRING || p.tok == token.IDENT {
		op.Pattern = p.parseUnaryExpr()
	}

	return op
}

// parseParseKvOp parses a parse-kv operator.
// Syntax: parse-kv Expr (Col: type, ...) [with (options)]
func (p *Parser) parseParseKvOp(pipePos token.Pos) *ast.ParseKvOp {
	opPos := p.pos
	p.next() // consume 'parse-kv'

	op := &ast.ParseKvOp{Pipe: pipePos, ParseKv: opPos}

	// Parse source expression (just identifier to avoid consuming the schema as function args)
	op.Source = p.parseIdent()

	// Parse optional 'as' keyword (for backwards compat)
	if p.tok == token.AS {
		op.AsPos = p.pos
		p.next()
	}

	// Parse key schema: (Col: type, ...)
	if p.tok == token.LPAREN {
		op.Lparen = p.pos
		p.next()
		for p.tok != token.RPAREN && p.tok != token.EOF {
			col := &ast.ColumnDeclExpr{Name: p.parseIdent()}
			if p.tok == token.COLON {
				col.Colon = p.pos
				p.next()
				col.Type = p.parseIdent()
			}
			op.Keys = append(op.Keys, col)
			if !p.accept(token.COMMA) {
				break
			}
		}
		op.Rparen = p.pos
		p.expect(token.RPAREN)
	}

	// Parse optional 'with (options)'
	if p.tok == token.WITH {
		op.WithPos = p.pos
		p.next()
		if p.tok == token.LPAREN {
			op.WithOpen = p.pos
			p.next()
			for p.tok != token.RPAREN && p.tok != token.EOF {
				// Parse option as name=value
				name := p.parseIdent()
				assignPos := p.expect(token.ASSIGN)
				value := p.parseExprNoPipe()
				op.Options = append(op.Options, &ast.OperatorParam{
					Name:   name,
					Assign: assignPos,
					Value:  value,
				})
				if !p.accept(token.COMMA) {
					break
				}
			}
			op.WithClose = p.pos
			p.expect(token.RPAREN)
		}
	}

	return op
}

// ============================================================================
// Graph Operators
// ============================================================================

// parseMakeGraphOp parses a make-graph operator.
// Syntax: make-graph SourceColumn --> TargetColumn [with ...]
func (p *Parser) parseMakeGraphOp(pipePos token.Pos) *ast.MakeGraphOp {
	opPos := p.pos
	p.next() // consume 'make-graph'

	op := &ast.MakeGraphOp{Pipe: pipePos, MakeGraph: opPos}

	// Parse optional parameters
	op.Params = p.parseOperatorParams()

	// Parse source column
	op.SourceColumn = p.parseExprNoPipe()

	// Parse direction: --> or --
	if p.tok == token.DASHGT {
		op.Direction = p.pos
		op.Directed = true
		p.next()
	} else if p.tok == token.DASHDASH {
		op.Direction = p.pos
		op.Directed = false
		p.next()
	} else {
		p.error(p.pos, "expected --> or --")
	}

	// Parse target column
	op.TargetColumn = p.parseExprNoPipe()

	// Parse optional with clause
	if p.tok == token.WITH {
		withPos := p.pos
		p.next()

		op.WithClause = &ast.MakeGraphWith{WithPos: withPos}

		// Check for with_node_id = name
		if p.tok == token.WITHNODEID || (p.tok == token.IDENT && p.lit == "with_node_id") {
			p.next()
			p.expect(token.ASSIGN)
			op.WithClause.NodeId = p.parseIdent()
		} else {
			// with Table on Column
			op.WithClause.Table = p.parseExprNoPipe()
			if p.tok == token.ON {
				op.WithClause.OnPos = p.pos
				p.next()
				op.WithClause.OnCol = p.parseExprNoPipe()
			}
		}
	}

	return op
}

// parseGraphMatchOp parses a graph-match operator.
// Syntax: graph-match (a)-[e]->(b) [where ...] [project ...]
func (p *Parser) parseGraphMatchOp(pipePos token.Pos) *ast.GraphMatchOp {
	opPos := p.pos
	p.next() // consume 'graph-match'

	op := &ast.GraphMatchOp{Pipe: pipePos, GraphMatch: opPos}

	// Parse optional parameters
	op.Params = p.parseOperatorParams()

	// Parse patterns
	op.Patterns = p.parseGraphPatterns()

	// Parse optional where clause
	if p.tok == token.WHERE {
		op.Where = p.parseWhereClause()
	}

	// Parse optional project clause
	if p.tok == token.PROJECT {
		op.Project = p.parseProjectClause()
	}

	return op
}

// parseGraphShortestPathsOp parses a graph-shortest-paths operator.
func (p *Parser) parseGraphShortestPathsOp(pipePos token.Pos) *ast.GraphShortestPathsOp {
	opPos := p.pos
	p.next() // consume 'graph-shortest-paths'

	op := &ast.GraphShortestPathsOp{Pipe: pipePos, GraphShortestPath: opPos}

	// Parse optional parameters
	op.Params = p.parseOperatorParams()

	// Parse patterns
	op.Patterns = p.parseGraphPatterns()

	// Parse optional where clause
	if p.tok == token.WHERE {
		op.Where = p.parseWhereClause()
	}

	// Parse optional project clause
	if p.tok == token.PROJECT {
		op.Project = p.parseProjectClause()
	}

	return op
}

// parseGraphMarkComponentsOp parses a graph-mark-components operator.
func (p *Parser) parseGraphMarkComponentsOp(pipePos token.Pos) *ast.GraphMarkComponentsOp {
	opPos := p.pos
	p.next() // consume 'graph-mark-components'

	op := &ast.GraphMarkComponentsOp{Pipe: pipePos, GraphMarkComponents: opPos}

	// Parse optional parameters
	op.Params = p.parseOperatorParams()

	return op
}

// parseGraphToTableOp parses a graph-to-table operator.
// Syntax: graph-to-table nodes [as Name], edges [as Name]
func (p *Parser) parseGraphToTableOp(pipePos token.Pos) *ast.GraphToTableOp {
	opPos := p.pos
	p.next() // consume 'graph-to-table'

	op := &ast.GraphToTableOp{Pipe: pipePos, GraphToTable: opPos}

	// Parse output clauses
	for p.tok == token.NODES || p.tok == token.EDGES {
		output := &ast.GraphToTableOutput{Keyword: p.pos}
		output.IsNodes = p.tok == token.NODES
		p.next()

		// Parse optional 'as Name'
		if p.tok == token.AS {
			p.next()
			output.AsName = p.parseIdent()
		}

		// Parse optional parameters
		output.Params = p.parseOperatorParams()

		op.Outputs = append(op.Outputs, output)

		if !p.accept(token.COMMA) {
			break
		}
	}

	return op
}

// parseGraphWhereNodesOp parses a graph-where-nodes operator.
func (p *Parser) parseGraphWhereNodesOp(pipePos token.Pos) *ast.GraphWhereNodesOp {
	opPos := p.pos
	p.next() // consume 'graph-where-nodes'

	return &ast.GraphWhereNodesOp{
		Pipe:            pipePos,
		GraphWhereNodes: opPos,
		Predicate:       p.parseExprNoPipe(),
	}
}

// parseGraphWhereEdgesOp parses a graph-where-edges operator.
func (p *Parser) parseGraphWhereEdgesOp(pipePos token.Pos) *ast.GraphWhereEdgesOp {
	opPos := p.pos
	p.next() // consume 'graph-where-edges'

	return &ast.GraphWhereEdgesOp{
		Pipe:            pipePos,
		GraphWhereEdges: opPos,
		Predicate:       p.parseExprNoPipe(),
	}
}

// parseGraphPatterns parses a list of graph match patterns.
func (p *Parser) parseGraphPatterns() []*ast.GraphMatchPattern {
	var patterns []*ast.GraphMatchPattern

	for {
		pattern := p.parseGraphPattern()
		if pattern != nil && len(pattern.Elements) > 0 {
			patterns = append(patterns, pattern)
		} else {
			break
		}

		if !p.accept(token.COMMA) {
			break
		}
	}

	return patterns
}

// parseGraphPattern parses a single graph pattern like (a)-[e]->(b).
func (p *Parser) parseGraphPattern() *ast.GraphMatchPattern {
	pattern := &ast.GraphMatchPattern{}

	for {
		switch p.tok {
		case token.LPAREN:
			// Node: (name)
			node := &ast.GraphPatternNode{Lparen: p.pos}
			p.next()
			node.Name = p.parseIdent()
			node.Rparen = p.pos
			p.expect(token.RPAREN)
			pattern.Elements = append(pattern.Elements, node)

		case token.DASHDASH:
			// Unnamed undirected edge: --
			edge := &ast.GraphPatternEdge{Start: p.pos, Direction: 0}
			p.next()
			edge.End_ = p.pos - 1
			pattern.Elements = append(pattern.Elements, edge)

		case token.DASHGT:
			// Unnamed forward edge: -->
			edge := &ast.GraphPatternEdge{Start: p.pos, Direction: 1}
			p.next()
			edge.End_ = p.pos - 1
			pattern.Elements = append(pattern.Elements, edge)

		case token.LTDASH:
			// Unnamed backward edge: <--
			edge := &ast.GraphPatternEdge{Start: p.pos, Direction: -1}
			p.next()
			edge.End_ = p.pos - 1
			pattern.Elements = append(pattern.Elements, edge)

		case token.DASHLBRACK:
			// Named edge starting with -[
			edge := &ast.GraphPatternEdge{Start: p.pos, Lbracket: p.pos + 1}
			p.next()
			edge.Name = p.parseIdent()

			// Check for range *min..max
			if p.tok == token.MUL {
				edge.RangeExpr = p.parseEdgeRange()
			}

			// Expect closing bracket with direction
			if p.tok == token.RBRACKDASHGT {
				edge.Rbracket = p.pos
				edge.Direction = 1
				p.next()
				edge.End_ = p.pos - 1
			} else if p.tok == token.RBRACKDASH {
				edge.Rbracket = p.pos
				edge.Direction = 0
				p.next()
				edge.End_ = p.pos - 1
			} else if p.tok == token.RBRACKET {
				edge.Rbracket = p.pos
				edge.Direction = 0
				p.next()
				// Check for trailing - or ->
				if p.tok == token.SUB {
					p.next()
					if p.tok == token.GTR {
						edge.Direction = 1
						p.next()
					}
				}
				edge.End_ = p.pos - 1
			} else {
				p.error(p.pos, "expected ]-, ]->, or ]")
			}
			pattern.Elements = append(pattern.Elements, edge)

		case token.LTDASHLBRACK:
			// Named backward edge starting with <-[
			edge := &ast.GraphPatternEdge{Start: p.pos, Lbracket: p.pos + 2, Direction: -1}
			p.next()
			edge.Name = p.parseIdent()

			// Check for range *min..max
			if p.tok == token.MUL {
				edge.RangeExpr = p.parseEdgeRange()
			}

			// Expect closing ]- for backward edge
			if p.tok == token.RBRACKDASH {
				edge.Rbracket = p.pos
				p.next()
				edge.End_ = p.pos - 1
			} else if p.tok == token.RBRACKET {
				edge.Rbracket = p.pos
				p.next()
				if p.tok == token.SUB {
					p.next()
				}
				edge.End_ = p.pos - 1
			} else {
				p.error(p.pos, "expected ]- or ]")
			}
			pattern.Elements = append(pattern.Elements, edge)

		default:
			// End of pattern
			return pattern
		}
	}
}

// parseEdgeRange parses *min..max for edge ranges.
func (p *Parser) parseEdgeRange() *ast.EdgeRange {
	r := &ast.EdgeRange{Star: p.pos}
	p.next() // consume *

	// Parse optional min
	if p.tok == token.INT {
		r.MinVal = p.parseLiteral()
	}

	// Expect ..
	if p.tok == token.DOTDOT {
		r.DotDot = p.pos
		p.next()

		// Parse optional max
		if p.tok == token.INT {
			r.MaxVal = p.parseLiteral()
		}
	}

	return r
}

// parseWhereClause parses a where clause for graph operators.
func (p *Parser) parseWhereClause() *ast.WhereClause {
	wherePos := p.pos
	p.next() // consume 'where'
	return &ast.WhereClause{
		Where:     wherePos,
		Predicate: p.parseExprNoPipe(),
	}
}

// parseProjectClause parses a project clause for graph operators.
func (p *Parser) parseProjectClause() *ast.ProjectClause {
	projectPos := p.pos
	p.next() // consume 'project'

	clause := &ast.ProjectClause{Project: projectPos}
	clause.Columns = p.parseNamedExprList()
	return clause
}

// ============================================================================
// Additional Operators
// ============================================================================

// parseExecuteAndCacheOp parses an execute-and-cache operator.
func (p *Parser) parseExecuteAndCacheOp(pipePos token.Pos) *ast.ExecuteAndCacheOp {
	opPos := p.pos
	p.next() // consume 'execute-and-cache'

	return &ast.ExecuteAndCacheOp{
		Pipe:            pipePos,
		ExecuteAndCache: opPos,
	}
}

// parseAssertSchemaOp parses an assert-schema operator.
// Syntax: assert-schema (col1:type1, col2:type2, ...)
func (p *Parser) parseAssertSchemaOp(pipePos token.Pos) *ast.AssertSchemaOp {
	opPos := p.pos
	p.next() // consume 'assert-schema'

	op := &ast.AssertSchemaOp{Pipe: pipePos, AssertSchema: opPos}

	// Parse schema: (col1:type1, col2:type2, ...)
	if p.tok == token.LPAREN {
		op.Lparen = p.pos
		p.next()

		for p.tok != token.RPAREN && p.tok != token.EOF {
			// Parse column declaration: name:type
			col := &ast.ColumnDeclExpr{Name: p.parseIdent()}
			if p.tok == token.COLON {
				col.Colon = p.pos
				p.next()
				col.Type = p.parseIdent()
			}
			op.Columns = append(op.Columns, col)

			if !p.accept(token.COMMA) {
				break
			}
		}

		op.Rparen = p.pos
		p.expect(token.RPAREN)
	}

	return op
}

// parseMacroExpandOp parses a macro-expand operator.
// Syntax: macro-expand EntityGroup as ScopeName (statements)
func (p *Parser) parseMacroExpandOp(pipePos token.Pos) *ast.MacroExpandOp {
	opPos := p.pos
	p.next() // consume 'macro-expand'

	op := &ast.MacroExpandOp{Pipe: pipePos, MacroExpand: opPos}

	// Parse optional parameters
	op.Params = p.parseOperatorParams()

	// Parse entity group expression
	op.EntityGroup = p.parseExprNoPipe()

	// Parse 'as ScopeName'
	if p.tok == token.AS {
		op.AsPos = p.pos
		p.next()
		op.ScopeName = p.parseIdent()
	}

	// Parse '( statements )'
	if p.tok == token.LPAREN {
		op.Lparen = p.pos
		p.next()

		// Parse statements (simplified - just parse until closing paren)
		for p.tok != token.RPAREN && p.tok != token.EOF {
			stmt := p.parseStmt()
			if stmt != nil {
				op.Statements = append(op.Statements, stmt)
			}
			p.accept(token.SEMI) // optional semicolon between statements
		}

		op.Rparen = p.pos
		p.expect(token.RPAREN)
	}

	return op
}

// parsePartitionByOp parses a partition-by operator.
// Syntax: __partitionby [params] Column [id GUID] (subquery)
func (p *Parser) parsePartitionByOp(pipePos token.Pos) *ast.PartitionByOp {
	opPos := p.pos
	p.next() // consume '__partitionby'

	op := &ast.PartitionByOp{Pipe: pipePos, PartitionBy: opPos}

	// Parse optional parameters
	op.Params = p.parseOperatorParams()

	// Parse column expression (just an identifier, not a full expression)
	// This prevents the following '(' from being interpreted as a function call
	op.Column = p.parseIdent()

	// Parse optional 'id GUID' clause
	if p.tok == token.IDENT && p.lit == "id" {
		p.next() // consume 'id'
		op.IdColumn = p.parseIdent()
	}

	// Parse '( subquery )'
	if p.tok == token.LPAREN {
		op.Lparen = p.pos
		p.next()

		// Parse subquery as a contextual subexpression
		op.SubExpr = p.parseContextualSubExpr()

		op.Rparen = p.pos
		p.expect(token.RPAREN)
	}

	return op
}
