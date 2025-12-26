package symbol

import (
	"testing"

	"github.com/cloudygreybeard/kqlparser/types"
)

func TestColumnSymbol(t *testing.T) {
	col := NewColumn("name", types.Typ_String)
	if col.Name() != "name" {
		t.Errorf("Name: got %q, want %q", col.Name(), "name")
	}
	if col.Kind() != SymbolColumn {
		t.Errorf("Kind: got %v, want %v", col.Kind(), SymbolColumn)
	}
	if col.Type() != types.Typ_String {
		t.Errorf("Type: got %v, want %v", col.Type(), types.Typ_String)
	}
}

func TestTableSymbol(t *testing.T) {
	tbl := NewTable("users",
		types.NewColumn("id", types.Typ_Long),
		types.NewColumn("name", types.Typ_String),
		types.NewColumn("email", types.Typ_String),
	)

	if tbl.Name() != "users" {
		t.Errorf("Name: got %q, want %q", tbl.Name(), "users")
	}
	if tbl.Kind() != SymbolTable {
		t.Errorf("Kind: got %v, want %v", tbl.Kind(), SymbolTable)
	}
	if len(tbl.Columns()) != 3 {
		t.Errorf("Columns: got %d, want 3", len(tbl.Columns()))
	}

	col := tbl.Column("name")
	if col == nil {
		t.Fatal("Column 'name' not found")
	}
	if col.Type != types.Typ_String {
		t.Errorf("Column type: got %v, want %v", col.Type, types.Typ_String)
	}

	if tbl.Column("nonexistent") != nil {
		t.Error("Expected nil for nonexistent column")
	}
}

func TestFunctionSymbol(t *testing.T) {
	fn := NewScalarFunction("strlen", types.Typ_Long,
		&Parameter{Name: "s", Type: types.Typ_String},
	)

	if fn.Name() != "strlen" {
		t.Errorf("Name: got %q, want %q", fn.Name(), "strlen")
	}
	if fn.Kind() != SymbolFunction {
		t.Errorf("Kind: got %v, want %v", fn.Kind(), SymbolFunction)
	}
	if len(fn.Signatures) != 1 {
		t.Fatalf("Signatures: got %d, want 1", len(fn.Signatures))
	}
	sig := fn.Signatures[0]
	if sig.ReturnType != types.Typ_Long {
		t.Errorf("ReturnType: got %v, want %v", sig.ReturnType, types.Typ_Long)
	}
	if sig.MinArgs != 1 {
		t.Errorf("MinArgs: got %d, want 1", sig.MinArgs)
	}
}

func TestAggregateSymbol(t *testing.T) {
	agg := NewAggregate("count", types.Typ_Long)
	if agg.Kind() != SymbolAggregate {
		t.Errorf("Kind: got %v, want %v", agg.Kind(), SymbolAggregate)
	}
}

func TestScope(t *testing.T) {
	parent := NewScope(nil)
	parent.Define(NewVariable("x", types.Typ_Long))

	child := NewScope(parent)
	child.Define(NewVariable("y", types.Typ_String))

	// Child can see its own symbols
	if child.Lookup("y") == nil {
		t.Error("Expected to find 'y' in child scope")
	}

	// Child cannot see parent with Lookup
	if child.Lookup("x") != nil {
		t.Error("Lookup should not see parent scope")
	}

	// Child can see parent with Resolve
	if child.Resolve("x") == nil {
		t.Error("Expected to resolve 'x' from parent scope")
	}

	// Parent cannot see child
	if parent.Resolve("y") != nil {
		t.Error("Parent should not see child symbols")
	}
}

func TestRowScope(t *testing.T) {
	parent := NewRowScope(nil)
	parent.AddColumn(NewColumn("a", types.Typ_Long))

	child := NewRowScope(parent)
	child.AddColumn(NewColumn("b", types.Typ_String))

	// Child can see parent columns
	if child.Column("a") == nil {
		t.Error("Expected to find 'a' in parent")
	}
	if child.Column("b") == nil {
		t.Error("Expected to find 'b' in child")
	}

	// AllColumns returns both
	all := child.AllColumns()
	if len(all) != 2 {
		t.Errorf("AllColumns: got %d, want 2", len(all))
	}
}

func TestDatabaseSymbol(t *testing.T) {
	db := NewDatabase("testdb")
	db.AddTable(NewTable("users", types.NewColumn("id", types.Typ_Long)))
	db.AddTable(NewTable("orders", types.NewColumn("id", types.Typ_Long)))

	if db.Table("users") == nil {
		t.Error("Expected to find 'users' table")
	}
	if db.Table("orders") == nil {
		t.Error("Expected to find 'orders' table")
	}
	if db.Table("nonexistent") != nil {
		t.Error("Expected nil for nonexistent table")
	}
}

func TestClusterSymbol(t *testing.T) {
	cluster := NewCluster("help.kusto.windows.net")
	db := NewDatabase("Samples")
	db.AddTable(NewTable("StormEvents", types.NewColumn("EventId", types.Typ_Long)))
	cluster.AddDatabase(db)

	if cluster.Database("Samples") == nil {
		t.Error("Expected to find 'Samples' database")
	}
	if cluster.Database("Samples").Table("StormEvents") == nil {
		t.Error("Expected to find 'StormEvents' table")
	}
}

