package types

import "testing"

func TestScalarTypes(t *testing.T) {
	tests := []struct {
		typ    *Scalar
		name   string
		scalar bool
	}{
		{Typ_Bool, "bool", true},
		{Typ_Int, "int", true},
		{Typ_Long, "long", true},
		{Typ_Real, "real", true},
		{Typ_String, "string", true},
		{Typ_DateTime, "datetime", true},
		{Typ_TimeSpan, "timespan", true},
		{Typ_Guid, "guid", true},
		{Typ_Dynamic, "dynamic", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.typ.String() != tt.name {
				t.Errorf("String: got %q, want %q", tt.typ.String(), tt.name)
			}
			if IsScalar(tt.typ) != tt.scalar {
				t.Errorf("IsScalar: got %v, want %v", IsScalar(tt.typ), tt.scalar)
			}
		})
	}
}

func TestTabularType(t *testing.T) {
	tab := NewTabular(
		NewColumn("id", Typ_Long),
		NewColumn("name", Typ_String),
		NewColumn("timestamp", Typ_DateTime),
	)

	if !IsTabular(tab) {
		t.Error("Expected IsTabular to return true")
	}

	if len(tab.Columns) != 3 {
		t.Errorf("Columns: got %d, want 3", len(tab.Columns))
	}

	if !tab.HasColumn("name") {
		t.Error("Expected HasColumn('name') to return true")
	}

	if tab.HasColumn("nonexistent") {
		t.Error("Expected HasColumn('nonexistent') to return false")
	}

	col := tab.Column("timestamp")
	if col == nil {
		t.Fatal("Expected Column('timestamp') to return non-nil")
	}
	if col.Type != Typ_DateTime {
		t.Errorf("Column type: got %v, want %v", col.Type, Typ_DateTime)
	}

	expected := "(id: long, name: string, timestamp: datetime)"
	if tab.String() != expected {
		t.Errorf("String: got %q, want %q", tab.String(), expected)
	}
}

func TestIsNumeric(t *testing.T) {
	tests := []struct {
		typ    Type
		want   bool
	}{
		{Typ_Int, true},
		{Typ_Long, true},
		{Typ_Real, true},
		{Typ_Decimal, true},
		{Typ_String, false},
		{Typ_Bool, false},
		{Typ_DateTime, false},
	}

	for _, tt := range tests {
		if got := IsNumeric(tt.typ); got != tt.want {
			t.Errorf("IsNumeric(%v): got %v, want %v", tt.typ, got, tt.want)
		}
	}
}

func TestIsTemporal(t *testing.T) {
	tests := []struct {
		typ  Type
		want bool
	}{
		{Typ_DateTime, true},
		{Typ_TimeSpan, true},
		{Typ_Long, false},
		{Typ_String, false},
	}

	for _, tt := range tests {
		if got := IsTemporal(tt.typ); got != tt.want {
			t.Errorf("IsTemporal(%v): got %v, want %v", tt.typ, got, tt.want)
		}
	}
}

func TestEqual(t *testing.T) {
	tests := []struct {
		a, b Type
		want bool
	}{
		{Typ_Long, Typ_Long, true},
		{Typ_Long, Typ_Int, false},
		{Typ_String, Typ_String, true},
		{Typ_Dynamic, Typ_Dynamic, true},
		{NewTabular(NewColumn("a", Typ_Long)), NewTabular(NewColumn("a", Typ_Long)), true},
		{NewTabular(NewColumn("a", Typ_Long)), NewTabular(NewColumn("b", Typ_Long)), false},
	}

	for _, tt := range tests {
		if got := Equal(tt.a, tt.b); got != tt.want {
			t.Errorf("Equal(%v, %v): got %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestCompatible(t *testing.T) {
	tests := []struct {
		a, b Type
		want bool
	}{
		{Typ_Long, Typ_Long, true},
		{Typ_Long, Typ_Int, true},  // Numeric compatible
		{Typ_Long, Typ_Real, true}, // Numeric compatible
		{Typ_Long, Typ_String, false},
		{Typ_Dynamic, Typ_Long, true},   // Dynamic compatible with anything
		{Typ_String, Typ_Dynamic, true}, // Dynamic compatible with anything
	}

	for _, tt := range tests {
		if got := Compatible(tt.a, tt.b); got != tt.want {
			t.Errorf("Compatible(%v, %v): got %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestCommonType(t *testing.T) {
	tests := []struct {
		a, b Type
		want Type
	}{
		{Typ_Long, Typ_Long, Typ_Long},
		{Typ_Int, Typ_Long, Typ_Long},
		{Typ_Int, Typ_Real, Typ_Real},
		{Typ_Real, Typ_Decimal, Typ_Decimal},
		{Typ_Long, Typ_Dynamic, Typ_Dynamic},
		{Typ_String, Typ_Long, Typ_Unknown},
	}

	for _, tt := range tests {
		if got := CommonType(tt.a, tt.b); !Equal(got, tt.want) {
			t.Errorf("CommonType(%v, %v): got %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

