package binder

import (
	"github.com/cloudygreybeard/kqlparser/ast"
	"github.com/cloudygreybeard/kqlparser/symbol"
	"github.com/cloudygreybeard/kqlparser/types"
)

// bindOperator binds a query operator and returns the result schema.
func (b *Binder) bindOperator(op ast.Operator, inputType types.Type) types.Type {
	switch o := op.(type) {
	case *ast.WhereOp:
		return b.bindWhereOp(o, inputType)
	case *ast.ProjectOp:
		return b.bindProjectOp(o, inputType)
	case *ast.ExtendOp:
		return b.bindExtendOp(o, inputType)
	case *ast.SummarizeOp:
		return b.bindSummarizeOp(o, inputType)
	case *ast.SortOp:
		return b.bindSortOp(o, inputType)
	case *ast.TakeOp:
		return b.bindTakeOp(o, inputType)
	case *ast.TopOp:
		return b.bindTopOp(o, inputType)
	case *ast.CountOp:
		return b.bindCountOp(o, inputType)
	case *ast.DistinctOp:
		return b.bindDistinctOp(o, inputType)
	case *ast.JoinOp:
		return b.bindJoinOp(o, inputType)
	case *ast.UnionOp:
		return b.bindUnionOp(o, inputType)
	case *ast.ProjectAwayOp:
		return b.bindProjectAwayOp(o, inputType)
	case *ast.MvExpandOp:
		return b.bindMvExpandOp(o, inputType)
	case *ast.SearchOp:
		return b.bindSearchOp(o, inputType)
	case *ast.RenderOp:
		return b.bindRenderOp(o, inputType)
	case *ast.ParseOp:
		return b.bindParseOp(o, inputType)
	case *ast.ParseWhereOp:
		return b.bindParseWhereOp(o, inputType)
	case *ast.ParseKvOp:
		return b.bindParseKvOp(o, inputType)
	case *ast.GenericOp:
		return b.bindGenericOp(o, inputType)
	case *ast.ProjectRenameOp:
		return b.bindProjectRenameOp(o, inputType)
	case *ast.ProjectReorderOp:
		return b.bindProjectReorderOp(o, inputType)
	case *ast.SampleOp:
		return b.bindSampleOp(o, inputType)
	case *ast.SampleDistinctOp:
		return b.bindSampleDistinctOp(o, inputType)
	case *ast.LookupOp:
		return b.bindLookupOp(o, inputType)
	case *ast.MakeSeriesOp:
		return b.bindMakeSeriesOp(o, inputType)
	case *ast.AsOp:
		return b.bindAsOp(o, inputType)
	case *ast.GetSchemaOp:
		return b.bindGetSchemaOp(o, inputType)
	case *ast.SerializeOp:
		return b.bindSerializeOp(o, inputType)
	case *ast.InvokeOp:
		return b.bindInvokeOp(o, inputType)
	case *ast.ScanOp:
		return b.bindScanOp(o, inputType)
	case *ast.ConsumeOp:
		return b.bindConsumeOp(o, inputType)
	case *ast.EvaluateOp:
		return b.bindEvaluateOp(o, inputType)
	case *ast.ReduceOp:
		return b.bindReduceOp(o, inputType)
	case *ast.ForkOp:
		return b.bindForkOp(o, inputType)
	case *ast.FacetOp:
		return b.bindFacetOp(o, inputType)
	case *ast.ProjectKeepOp:
		return b.bindProjectKeepOp(o, inputType)
	case *ast.TopNestedOp:
		return b.bindTopNestedOp(o, inputType)
	case *ast.TopHittersOp:
		return b.bindTopHittersOp(o, inputType)
	case *ast.MvApplyOp:
		return b.bindMvApplyOp(o, inputType)
	case *ast.FindOp:
		return b.bindFindOp(o, inputType)
	case *ast.ExternalDataOp:
		return b.bindExternalDataOp(o, inputType)
	default:
		return inputType
	}
}

// bindWhereOp binds a where operator. Schema passes through unchanged.
func (b *Binder) bindWhereOp(op *ast.WhereOp, inputType types.Type) types.Type {
	// Bind the predicate
	predType := b.bindExpr(op.Predicate)
	_ = predType // Could check it's bool

	// Where doesn't change schema
	return inputType
}

// bindProjectOp binds a project operator. Schema is replaced by projected columns.
func (b *Binder) bindProjectOp(op *ast.ProjectOp, inputType types.Type) types.Type {
	var columns []*types.Column

	for _, col := range op.Columns {
		colType := b.bindExpr(col.Expr)
		name := b.getColumnName(col)
		columns = append(columns, &types.Column{Name: name, Type: colType})
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindExtendOp binds an extend operator. Adds columns to schema.
func (b *Binder) bindExtendOp(op *ast.ExtendOp, inputType types.Type) types.Type {
	// Start with existing columns
	var columns []*types.Column
	if tab, ok := inputType.(*types.Tabular); ok {
		columns = append(columns, tab.Columns...)
	}

	// Add new columns
	for _, col := range op.Columns {
		colType := b.bindExpr(col.Expr)
		name := b.getColumnName(col)
		columns = append(columns, &types.Column{Name: name, Type: colType})
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindSummarizeOp binds a summarize operator.
func (b *Binder) bindSummarizeOp(op *ast.SummarizeOp, inputType types.Type) types.Type {
	var columns []*types.Column

	// Add group-by columns first
	for _, gb := range op.GroupBy {
		colType := b.bindExpr(gb.Expr)
		name := b.getColumnName(gb)
		columns = append(columns, &types.Column{Name: name, Type: colType})
	}

	// Add aggregate columns
	for _, agg := range op.Aggregates {
		colType := b.bindExpr(agg.Expr)
		name := b.getColumnName(agg)
		columns = append(columns, &types.Column{Name: name, Type: colType})
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindSortOp binds a sort operator. Schema passes through unchanged.
func (b *Binder) bindSortOp(op *ast.SortOp, inputType types.Type) types.Type {
	for _, order := range op.Orders {
		b.bindExpr(order.Expr)
	}
	return inputType
}

// bindTakeOp binds a take/limit operator. Schema passes through unchanged.
func (b *Binder) bindTakeOp(op *ast.TakeOp, inputType types.Type) types.Type {
	b.bindExpr(op.Count)
	return inputType
}

// bindTopOp binds a top operator. Schema passes through unchanged.
func (b *Binder) bindTopOp(op *ast.TopOp, inputType types.Type) types.Type {
	b.bindExpr(op.Count)
	b.bindExpr(op.ByExpr.Expr)
	return inputType
}

// bindCountOp binds a count operator. Returns single column "Count".
func (b *Binder) bindCountOp(op *ast.CountOp, inputType types.Type) types.Type {
	newSchema := types.NewTabular(&types.Column{Name: "Count", Type: types.Typ_Long})
	b.updateRowScope(newSchema)
	return newSchema
}

// bindDistinctOp binds a distinct operator.
func (b *Binder) bindDistinctOp(op *ast.DistinctOp, inputType types.Type) types.Type {
	if len(op.Columns) == 0 {
		// distinct * - keeps all columns
		return inputType
	}

	var columns []*types.Column
	for _, col := range op.Columns {
		colType := b.bindExpr(col)
		// Try to get column name from expression
		name := ""
		if ident, ok := col.(*ast.Ident); ok {
			name = ident.Name
		}
		columns = append(columns, &types.Column{Name: name, Type: colType})
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindJoinOp binds a join operator.
func (b *Binder) bindJoinOp(op *ast.JoinOp, inputType types.Type) types.Type {
	// Bind right side
	rightType := b.bindExpr(op.Right)

	// Bind on conditions
	for _, cond := range op.OnExpr {
		b.bindExpr(cond)
	}

	// Result schema combines left and right
	var columns []*types.Column
	if leftTab, ok := inputType.(*types.Tabular); ok {
		columns = append(columns, leftTab.Columns...)
	}
	if rightTab, ok := rightType.(*types.Tabular); ok {
		// Add right columns (with potential renaming for duplicates)
		columns = append(columns, rightTab.Columns...)
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindUnionOp binds a union operator.
func (b *Binder) bindUnionOp(op *ast.UnionOp, inputType types.Type) types.Type {
	// Bind all tables
	for _, table := range op.Tables {
		b.bindExpr(table)
	}

	// Union typically keeps the schema of the first table
	return inputType
}

// bindProjectAwayOp binds a project-away operator.
func (b *Binder) bindProjectAwayOp(op *ast.ProjectAwayOp, inputType types.Type) types.Type {
	// Build set of columns to remove
	remove := make(map[string]bool)
	for _, col := range op.Columns {
		remove[col.Name] = true
	}

	// Keep columns not in remove set
	var columns []*types.Column
	if tab, ok := inputType.(*types.Tabular); ok {
		for _, col := range tab.Columns {
			if !remove[col.Name] {
				columns = append(columns, col)
			}
		}
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindMvExpandOp binds an mv-expand operator.
func (b *Binder) bindMvExpandOp(op *ast.MvExpandOp, inputType types.Type) types.Type {
	for _, col := range op.Columns {
		b.bindExpr(col.Expr)
		if col.Type != nil {
			b.bindExpr(col.Type)
		}
	}
	// mv-expand keeps schema but expanded columns become scalar
	return inputType
}

// bindSearchOp binds a search operator.
func (b *Binder) bindSearchOp(op *ast.SearchOp, inputType types.Type) types.Type {
	b.bindExpr(op.Predicate)
	return inputType
}

// bindRenderOp binds a render operator.
func (b *Binder) bindRenderOp(op *ast.RenderOp, inputType types.Type) types.Type {
	// Render doesn't change schema
	return inputType
}

// bindParseOp binds a parse operator.
func (b *Binder) bindParseOp(op *ast.ParseOp, inputType types.Type) types.Type {
	b.bindExpr(op.Source)
	b.bindExpr(op.Pattern)
	// Parse adds columns extracted from pattern - we'd need pattern analysis
	return inputType
}

// bindGenericOp binds an unrecognized operator.
func (b *Binder) bindGenericOp(op *ast.GenericOp, inputType types.Type) types.Type {
	for _, content := range op.Content {
		b.bindExpr(content)
	}
	return inputType
}

// getColumnName extracts the name from a named expression.
func (b *Binder) getColumnName(expr *ast.NamedExpr) string {
	if expr.Name != nil {
		return expr.Name.Name
	}
	// Try to infer from expression
	if ident, ok := expr.Expr.(*ast.Ident); ok {
		return ident.Name
	}
	if call, ok := expr.Expr.(*ast.CallExpr); ok {
		if fn, ok := call.Fun.(*ast.Ident); ok {
			return fn.Name
		}
	}
	return ""
}

// updateRowScope updates the row scope to match a new schema.
func (b *Binder) updateRowScope(schema *types.Tabular) {
	b.rowScope = symbol.NewRowScope(nil)
	for _, col := range schema.Columns {
		b.rowScope.AddColumn(symbol.NewColumn(col.Name, col.Type))
	}
}

// bindProjectRenameOp binds a project-rename operator.
func (b *Binder) bindProjectRenameOp(op *ast.ProjectRenameOp, inputType types.Type) types.Type {
	tab, ok := inputType.(*types.Tabular)
	if !ok {
		return inputType
	}

	// Build rename map
	renames := make(map[string]string)
	for _, col := range op.Columns {
		renames[col.OldName.Name] = col.NewName.Name
	}

	// Apply renames
	var columns []*types.Column
	for _, col := range tab.Columns {
		newName := col.Name
		if renamed, ok := renames[col.Name]; ok {
			newName = renamed
		}
		columns = append(columns, &types.Column{Name: newName, Type: col.Type})
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindProjectReorderOp binds a project-reorder operator.
func (b *Binder) bindProjectReorderOp(op *ast.ProjectReorderOp, inputType types.Type) types.Type {
	tab, ok := inputType.(*types.Tabular)
	if !ok {
		return inputType
	}

	// Build column lookup
	colMap := make(map[string]*types.Column)
	for _, col := range tab.Columns {
		colMap[col.Name] = col
	}

	// Reorder: specified columns first, then remaining
	var columns []*types.Column
	seen := make(map[string]bool)
	for _, ident := range op.Columns {
		if col, ok := colMap[ident.Name]; ok {
			columns = append(columns, col)
			seen[ident.Name] = true
		}
	}
	for _, col := range tab.Columns {
		if !seen[col.Name] {
			columns = append(columns, col)
		}
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindSampleOp binds a sample operator.
func (b *Binder) bindSampleOp(op *ast.SampleOp, inputType types.Type) types.Type {
	b.bindExpr(op.Count)
	return inputType
}

// bindSampleDistinctOp binds a sample-distinct operator.
func (b *Binder) bindSampleDistinctOp(op *ast.SampleDistinctOp, inputType types.Type) types.Type {
	b.bindExpr(op.Count)
	colType := b.bindExpr(op.Column)

	// Result is single column with distinct values
	name := ""
	if ident, ok := op.Column.(*ast.Ident); ok {
		name = ident.Name
	}
	newSchema := types.NewTabular(&types.Column{Name: name, Type: colType})
	b.updateRowScope(newSchema)
	return newSchema
}

// bindLookupOp binds a lookup operator.
func (b *Binder) bindLookupOp(op *ast.LookupOp, inputType types.Type) types.Type {
	rightType := b.bindExpr(op.Table)

	for _, cond := range op.OnExpr {
		b.bindExpr(cond)
	}

	// Combine schemas (similar to join)
	var columns []*types.Column
	if leftTab, ok := inputType.(*types.Tabular); ok {
		columns = append(columns, leftTab.Columns...)
	}
	if rightTab, ok := rightType.(*types.Tabular); ok {
		columns = append(columns, rightTab.Columns...)
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindMakeSeriesOp binds a make-series operator.
func (b *Binder) bindMakeSeriesOp(op *ast.MakeSeriesOp, inputType types.Type) types.Type {
	var columns []*types.Column

	// Group by columns first
	for _, gb := range op.GroupBy {
		colType := b.bindExpr(gb.Expr)
		name := b.getColumnName(gb)
		columns = append(columns, &types.Column{Name: name, Type: colType})
	}

	// On column (time axis)
	if op.OnColumn != nil {
		b.bindExpr(op.OnColumn)
		name := ""
		if ident, ok := op.OnColumn.(*ast.Ident); ok {
			name = ident.Name
		}
		// Time column becomes array in make-series
		columns = append(columns, &types.Column{Name: name, Type: types.Typ_Dynamic})
	}

	// Aggregate columns become arrays
	for _, agg := range op.Aggregates {
		b.bindExpr(agg.Expr)
		name := b.getColumnName(agg.Expr)
		columns = append(columns, &types.Column{Name: name, Type: types.Typ_Dynamic})
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindAsOp binds an as operator.
func (b *Binder) bindAsOp(op *ast.AsOp, inputType types.Type) types.Type {
	// 'as' just names the result, doesn't change schema
	return inputType
}

// bindGetSchemaOp binds a getschema operator.
func (b *Binder) bindGetSchemaOp(op *ast.GetSchemaOp, inputType types.Type) types.Type {
	// getschema returns schema metadata
	newSchema := types.NewTabular(
		&types.Column{Name: "ColumnName", Type: types.Typ_String},
		&types.Column{Name: "ColumnOrdinal", Type: types.Typ_Long},
		&types.Column{Name: "DataType", Type: types.Typ_String},
		&types.Column{Name: "ColumnType", Type: types.Typ_String},
	)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindSerializeOp binds a serialize operator.
func (b *Binder) bindSerializeOp(op *ast.SerializeOp, inputType types.Type) types.Type {
	// Start with existing columns
	var columns []*types.Column
	if tab, ok := inputType.(*types.Tabular); ok {
		columns = append(columns, tab.Columns...)
	}

	// Add serialized columns
	for _, col := range op.Columns {
		colType := b.bindExpr(col.Expr)
		name := b.getColumnName(col)
		columns = append(columns, &types.Column{Name: name, Type: colType})
	}

	newSchema := types.NewTabular(columns...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindInvokeOp binds an invoke operator.
func (b *Binder) bindInvokeOp(op *ast.InvokeOp, inputType types.Type) types.Type {
	b.bindExpr(op.Function)
	// Invoke calls a stored function - return type depends on function
	return types.Typ_Unknown
}

// bindScanOp binds a scan operator.
func (b *Binder) bindScanOp(op *ast.ScanOp, inputType types.Type) types.Type {
	// Bind order by expressions
	for _, ord := range op.OrderBy {
		b.bindExpr(ord.Expr)
	}
	// Bind partition by expressions
	for _, expr := range op.PartitionBy {
		b.bindExpr(expr)
	}
	// Bind step conditions and assignments
	for _, step := range op.Steps {
		if step.Condition != nil {
			b.bindExpr(step.Condition)
		}
		for _, assign := range step.Assigns {
			if assign.Value != nil {
				b.bindExpr(assign.Value)
			}
		}
	}
	// Scan is complex - typically preserves schema with added state columns
	return inputType
}

// bindConsumeOp binds a consume operator.
func (b *Binder) bindConsumeOp(op *ast.ConsumeOp, inputType types.Type) types.Type {
	// Consume returns nothing (forces evaluation)
	return types.NewTabular()
}

// bindEvaluateOp binds an evaluate operator.
func (b *Binder) bindEvaluateOp(op *ast.EvaluateOp, inputType types.Type) types.Type {
	b.bindExpr(op.Plugin)
	// Plugin output depends on the specific plugin
	return types.Typ_Unknown
}

// bindReduceOp binds a reduce operator.
func (b *Binder) bindReduceOp(op *ast.ReduceOp, inputType types.Type) types.Type {
	b.bindExpr(op.Column)
	// Reduce returns pattern analysis results
	newSchema := types.NewTabular(
		&types.Column{Name: "Pattern", Type: types.Typ_String},
		&types.Column{Name: "Count", Type: types.Typ_Long},
		&types.Column{Name: "Representative", Type: types.Typ_String},
	)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindForkOp binds a fork operator.
func (b *Binder) bindForkOp(op *ast.ForkOp, inputType types.Type) types.Type {
	// Fork creates multiple result streams - bind each prong
	for _, prong := range op.Prongs {
		if prong.Query != nil {
			b.bindExpr(prong.Query)
		}
	}
	// Fork returns multiple tables - simplified to return input
	return inputType
}

// bindFacetOp binds a facet operator.
func (b *Binder) bindFacetOp(op *ast.FacetOp, inputType types.Type) types.Type {
	// Bind the with query if present
	if op.Query != nil {
		b.bindExpr(op.Query)
	}
	// Facet returns multiple tables - one per facet column plus optional with result
	return inputType
}

// bindProjectKeepOp binds a project-keep operator.
func (b *Binder) bindProjectKeepOp(op *ast.ProjectKeepOp, inputType types.Type) types.Type {
	// project-keep keeps only the specified columns from input
	tab, ok := inputType.(*types.Tabular)
	if !ok {
		return inputType
	}

	// Build new schema with only kept columns
	var cols []*types.Column
	for _, ident := range op.Columns {
		// Find matching column in input
		for _, col := range tab.Columns {
			if col.Name == ident.Name {
				cols = append(cols, col)
				break
			}
		}
	}

	newSchema := types.NewTabular(cols...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindTopNestedOp binds a top-nested operator.
func (b *Binder) bindTopNestedOp(op *ast.TopNestedOp, inputType types.Type) types.Type {
	// Bind clause expressions
	for _, clause := range op.Clauses {
		if clause.Count != nil {
			b.bindExpr(clause.Count)
		}
		if clause.Column != nil {
			b.bindExpr(clause.Column)
		}
		if clause.ByExpr != nil {
			b.bindExpr(clause.ByExpr)
		}
		if clause.Others != nil {
			b.bindExpr(clause.Others)
		}
	}
	// top-nested produces hierarchical output - simplified to return dynamic
	return types.NewTabular(
		&types.Column{Name: "aggregated_column", Type: types.Typ_Dynamic},
	)
}

// bindTopHittersOp binds a top-hitters operator.
func (b *Binder) bindTopHittersOp(op *ast.TopHittersOp, inputType types.Type) types.Type {
	if op.Count != nil {
		b.bindExpr(op.Count)
	}
	if op.Column != nil {
		b.bindExpr(op.Column)
	}
	if op.ByExpr != nil {
		b.bindExpr(op.ByExpr)
	}
	// top-hitters returns column value and approximate count
	return types.NewTabular(
		&types.Column{Name: "column", Type: types.Typ_Dynamic},
		&types.Column{Name: "approximate_count_column", Type: types.Typ_Long},
	)
}

// bindMvApplyOp binds an mv-apply operator.
func (b *Binder) bindMvApplyOp(op *ast.MvApplyOp, inputType types.Type) types.Type {
	// Bind item expressions
	for _, item := range op.Items {
		if item != nil {
			b.bindExpr(item.Expr)
		}
	}
	// Bind the on subquery
	if op.OnExpr != nil {
		b.bindExpr(op.OnExpr)
	}
	// mv-apply expands and applies - returns modified input schema
	return inputType
}

// bindFindOp binds a find operator.
func (b *Binder) bindFindOp(op *ast.FindOp, inputType types.Type) types.Type {
	// Bind table expressions
	for _, table := range op.Tables {
		b.bindExpr(table)
	}
	// Bind predicate
	if op.Predicate != nil {
		b.bindExpr(op.Predicate)
	}
	// Bind projected columns (FindColumn is not an Expr, just extract names)
	for _, col := range op.Columns {
		if col.Name != nil {
			b.bindExpr(col.Name)
		}
	}
	// find returns source_, pack_ and any projected columns
	cols := []*types.Column{
		{Name: "source_", Type: types.Typ_String},
		{Name: "pack_", Type: types.Typ_Dynamic},
	}
	newSchema := types.NewTabular(cols...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindExternalDataOp binds an externaldata operator.
func (b *Binder) bindExternalDataOp(op *ast.ExternalDataOp, inputType types.Type) types.Type {
	// Build schema from column declarations
	var cols []*types.Column
	for _, decl := range op.Columns {
		colType := b.resolveTypeName(decl.Type.Name)
		cols = append(cols, &types.Column{Name: decl.Name.Name, Type: colType})
	}
	// Bind URI expressions
	for _, uri := range op.URIs {
		b.bindExpr(uri)
	}
	// Bind property expressions
	for _, prop := range op.Properties {
		b.bindExpr(prop)
	}
	newSchema := types.NewTabular(cols...)
	b.updateRowScope(newSchema)
	return newSchema
}

// bindParseWhereOp binds a parse-where operator.
func (b *Binder) bindParseWhereOp(op *ast.ParseWhereOp, inputType types.Type) types.Type {
	// Bind source and pattern
	if op.Source != nil {
		b.bindExpr(op.Source)
	}
	if op.Pattern != nil {
		b.bindExpr(op.Pattern)
	}
	// parse-where filters rows and may add columns from pattern capture groups
	// Simplified: return input schema unchanged (pattern extraction would add columns)
	return inputType
}

// bindParseKvOp binds a parse-kv operator.
func (b *Binder) bindParseKvOp(op *ast.ParseKvOp, inputType types.Type) types.Type {
	// Bind source
	if op.Source != nil {
		b.bindExpr(op.Source)
	}
	// Bind key expressions (ColumnDeclExpr names)
	for _, key := range op.Keys {
		if key.Name != nil {
			b.bindExpr(key.Name)
		}
	}
	// Bind option values
	for _, opt := range op.Options {
		if opt.Value != nil {
			b.bindExpr(opt.Value)
		}
	}
	// parse-kv adds columns for each key-value pair extracted
	// Return input type (actual columns depend on data)
	return inputType
}
