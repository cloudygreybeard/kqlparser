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
	case *ast.GenericOp:
		return b.bindGenericOp(o, inputType)
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
		for _, col := range rightTab.Columns {
			columns = append(columns, col)
		}
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
		b.bindExpr(col)
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
