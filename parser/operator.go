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

// parseAsOp parses an as operator.
func (p *Parser) parseAsOp(pipePos token.Pos) *ast.AsOp {
	asPos := p.pos
	p.next() // consume 'as'

	name := p.parseIdent()

	return &ast.AsOp{
		Pipe: pipePos,
		As:   asPos,
		Name: name,
	}
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

	// Parse aggregates until 'on'
	for p.tok != token.ON && p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		expr := p.parseNamedExprSingle()
		op.Aggregates = append(op.Aggregates, expr)
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

	// Parse optional 'in range(...)' - simplified parsing
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
func (p *Parser) parseScanOp(pipePos token.Pos) *ast.ScanOp {
	opPos := p.pos
	p.next() // consume 'scan'

	op := &ast.ScanOp{Pipe: pipePos, Scan: opPos}

	// Scan has complex syntax with 'with' and steps - simplified for now
	if p.tok == token.WITH {
		op.With = p.pos
		p.next()
	}

	// Parse content until next pipe
	for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		expr := p.parseExpr()
		if expr != nil {
			op.Steps = append(op.Steps, expr)
		}
		if !p.accept(token.COMMA) {
			break
		}
	}

	op.EndPos = p.pos
	return op
}

// parseConsumeOp parses a consume operator.
func (p *Parser) parseConsumeOp(pipePos token.Pos) *ast.ConsumeOp {
	opPos := p.pos
	p.next() // consume 'consume'

	return &ast.ConsumeOp{
		Pipe:    pipePos,
		Consume: opPos,
	}
}

// parseEvaluateOp parses an evaluate operator.
func (p *Parser) parseEvaluateOp(pipePos token.Pos) *ast.EvaluateOp {
	opPos := p.pos
	p.next() // consume 'evaluate'

	expr := p.parsePostfixExpr()
	var plugin *ast.CallExpr
	if call, ok := expr.(*ast.CallExpr); ok {
		plugin = call
	} else {
		plugin = &ast.CallExpr{Fun: expr, Lparen: expr.Pos(), Rparen: expr.End()}
	}

	return &ast.EvaluateOp{
		Pipe:     pipePos,
		Evaluate: opPos,
		Plugin:   plugin,
	}
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
func (p *Parser) parseForkOp(pipePos token.Pos) *ast.ForkOp {
	opPos := p.pos
	p.next() // consume 'fork'

	op := &ast.ForkOp{Pipe: pipePos, Fork: opPos}

	// Parse prongs (simplified - fork has complex syntax)
	for p.tok == token.LPAREN {
		prong := &ast.ForkProng{Lparen: p.pos}
		p.next() // consume '('
		// Parse inner query as expression (simplified)
		expr := p.parseExpr()
		if pipe, ok := expr.(*ast.PipeExpr); ok {
			prong.Query = pipe
		}
		prong.Rparen = p.expect(token.RPAREN)
		op.Prongs = append(op.Prongs, prong)
	}

	op.EndPos = p.pos
	return op
}

// parseFacetOp parses a facet operator.
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
		expr := p.parseExpr()
		if pipe, ok := expr.(*ast.PipeExpr); ok {
			op.Query = pipe
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

	// Parse items until 'on'
	for p.tok != token.ON && p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
		item := p.parseNamedExprSingle()
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
			// Parse inner query
			expr := p.parseExpr()
			if pipe, ok := expr.(*ast.PipeExpr); ok {
				op.OnExpr = pipe
			}
			p.expect(token.RPAREN)
		}
	}

	return op
}

// parseFindOp parses a find operator.
func (p *Parser) parseFindOp(pipePos token.Pos) *ast.FindOp {
	opPos := p.pos
	p.next() // consume 'find'

	op := &ast.FindOp{Pipe: pipePos, Find: opPos}

	// Parse optional 'in (tables)'
	if p.tok == token.IN {
		op.InPos = p.pos
		p.next()
		if p.tok == token.LPAREN {
			p.next()
			for p.tok != token.RPAREN && p.tok != token.EOF {
				table := p.parseUnaryExpr()
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

	// Parse optional 'project columns'
	if p.tok == token.PROJECT {
		op.ProjectPos = p.pos
		p.next()
		for p.tok != token.PIPE && p.tok != token.EOF && p.tok != token.SEMI {
			col := p.parseExprNoPipe()
			op.Columns = append(op.Columns, col)
			if !p.accept(token.COMMA) {
				break
			}
		}
	}

	return op
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
func (p *Parser) parseParseKvOp(pipePos token.Pos) *ast.ParseKvOp {
	opPos := p.pos
	p.next() // consume 'parse-kv'

	op := &ast.ParseKvOp{Pipe: pipePos, ParseKv: opPos}

	// Parse source column
	op.Source = p.parseExprNoPipe()

	// Parse optional 'as (columns)'
	if p.tok == token.AS {
		op.AsPos = p.pos
		p.next()
		if p.tok == token.LPAREN {
			p.next()
			for p.tok != token.RPAREN && p.tok != token.EOF {
				col := p.parseExprNoPipe()
				op.Columns = append(op.Columns, col)
				if !p.accept(token.COMMA) {
					break
				}
			}
			p.expect(token.RPAREN)
		}
	}

	// Parse optional 'with (options)'
	if p.tok == token.WITH {
		op.WithPos = p.pos
		p.next()
		if p.tok == token.LPAREN {
			p.next()
			for p.tok != token.RPAREN && p.tok != token.EOF {
				opt := p.parseExprNoPipe()
				op.Options = append(op.Options, opt)
				if !p.accept(token.COMMA) {
					break
				}
			}
			p.expect(token.RPAREN)
		}
	}

	return op
}
