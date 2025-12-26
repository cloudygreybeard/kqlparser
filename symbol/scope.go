package symbol

// Scope represents a lexical scope containing symbol bindings.
type Scope struct {
	parent  *Scope
	symbols map[string]Symbol
}

// NewScope creates a new scope with an optional parent.
func NewScope(parent *Scope) *Scope {
	return &Scope{
		parent:  parent,
		symbols: make(map[string]Symbol),
	}
}

// Parent returns the parent scope.
func (s *Scope) Parent() *Scope {
	return s.parent
}

// Define adds a symbol to the scope.
func (s *Scope) Define(sym Symbol) {
	s.symbols[sym.Name()] = sym
}

// Lookup looks up a symbol by name in this scope only.
func (s *Scope) Lookup(name string) Symbol {
	return s.symbols[name]
}

// Resolve looks up a symbol by name, searching parent scopes.
func (s *Scope) Resolve(name string) Symbol {
	if sym := s.symbols[name]; sym != nil {
		return sym
	}
	if s.parent != nil {
		return s.parent.Resolve(name)
	}
	return nil
}

// Symbols returns all symbols defined in this scope (not parents).
func (s *Scope) Symbols() []Symbol {
	result := make([]Symbol, 0, len(s.symbols))
	for _, sym := range s.symbols {
		result = append(result, sym)
	}
	return result
}

// RowScope represents the current row context with available columns.
type RowScope struct {
	parent  *RowScope
	columns map[string]*ColumnSymbol
}

// NewRowScope creates a new row scope.
func NewRowScope(parent *RowScope) *RowScope {
	return &RowScope{
		parent:  parent,
		columns: make(map[string]*ColumnSymbol),
	}
}

// AddColumn adds a column to the row scope.
func (r *RowScope) AddColumn(col *ColumnSymbol) {
	r.columns[col.Name()] = col
}

// Column looks up a column by name in this scope and parents.
func (r *RowScope) Column(name string) *ColumnSymbol {
	if col := r.columns[name]; col != nil {
		return col
	}
	if r.parent != nil {
		return r.parent.Column(name)
	}
	return nil
}

// Columns returns all columns in this scope (not parents).
func (r *RowScope) Columns() []*ColumnSymbol {
	result := make([]*ColumnSymbol, 0, len(r.columns))
	for _, col := range r.columns {
		result = append(result, col)
	}
	return result
}

// AllColumns returns all columns including from parent scopes.
func (r *RowScope) AllColumns() []*ColumnSymbol {
	seen := make(map[string]bool)
	var result []*ColumnSymbol

	for scope := r; scope != nil; scope = scope.parent {
		for name, col := range scope.columns {
			if !seen[name] {
				seen[name] = true
				result = append(result, col)
			}
		}
	}
	return result
}

// RowScopeFromTabular creates a row scope from a tabular type.
func RowScopeFromTabular(tab *Tabular, parent *RowScope) *RowScope {
	if tab == nil {
		return NewRowScope(parent)
	}
	scope := NewRowScope(parent)
	for _, col := range tab.Schema.Columns {
		scope.AddColumn(NewColumn(col.Name, col.Type))
	}
	return scope
}

// Tabular is an alias for TableSymbol to make RowScopeFromTabular work.
type Tabular = TableSymbol
