// Package builtin defines the built-in functions and aggregates in KQL.
package builtin

import (
	"github.com/cloudygreybeard/kqlparser/symbol"
	"github.com/cloudygreybeard/kqlparser/types"
)

// param creates a required parameter.
func param(name string, typ types.Type) *symbol.Parameter {
	return &symbol.Parameter{Name: name, Type: typ}
}

// optParam creates an optional parameter.
func optParam(name string, typ types.Type) *symbol.Parameter {
	return &symbol.Parameter{Name: name, Type: typ, IsOptional: true}
}

// ScalarFunctions contains all built-in scalar functions.
var ScalarFunctions = []*symbol.FunctionSymbol{
	// String functions
	symbol.NewScalarFunction("strlen", types.Typ_Long, param("s", types.Typ_String)),
	symbol.NewVariadicFunction("strcat", types.Typ_String, param("s1", types.Typ_String)),
	symbol.NewScalarFunction("substring", types.Typ_String,
		param("source", types.Typ_String),
		param("startIndex", types.Typ_Long),
		optParam("length", types.Typ_Long)),
	symbol.NewScalarFunction("toupper", types.Typ_String, param("s", types.Typ_String)),
	symbol.NewScalarFunction("tolower", types.Typ_String, param("s", types.Typ_String)),
	symbol.NewScalarFunction("trim", types.Typ_String, param("s", types.Typ_String)),
	symbol.NewScalarFunction("trim_start", types.Typ_String, param("s", types.Typ_String)),
	symbol.NewScalarFunction("trim_end", types.Typ_String, param("s", types.Typ_String)),
	symbol.NewScalarFunction("replace", types.Typ_String,
		param("source", types.Typ_String),
		param("lookup", types.Typ_String),
		param("rewrite", types.Typ_String)),
	symbol.NewScalarFunction("split", types.Typ_Dynamic,
		param("source", types.Typ_String),
		param("delimiter", types.Typ_String)),
	symbol.NewScalarFunction("parse_json", types.Typ_Dynamic, param("json", types.Typ_String)),
	symbol.NewScalarFunction("tostring", types.Typ_String, param("expr", types.Typ_Dynamic)),
	symbol.NewScalarFunction("extract", types.Typ_String,
		param("regex", types.Typ_String),
		param("captureGroup", types.Typ_Long),
		param("source", types.Typ_String)),
	symbol.NewScalarFunction("indexof", types.Typ_Long,
		param("source", types.Typ_String),
		param("lookup", types.Typ_String)),
	symbol.NewScalarFunction("countof", types.Typ_Long,
		param("source", types.Typ_String),
		param("search", types.Typ_String)),
	symbol.NewScalarFunction("reverse", types.Typ_String, param("s", types.Typ_String)),
	symbol.NewScalarFunction("base64_encode_tostring", types.Typ_String, param("s", types.Typ_String)),
	symbol.NewScalarFunction("base64_decode_tostring", types.Typ_String, param("s", types.Typ_String)),
	symbol.NewScalarFunction("url_encode", types.Typ_String, param("url", types.Typ_String)),
	symbol.NewScalarFunction("url_decode", types.Typ_String, param("url", types.Typ_String)),

	// DateTime functions
	symbol.NewScalarFunction("now", types.Typ_DateTime),
	symbol.NewScalarFunction("ago", types.Typ_DateTime, param("timespan", types.Typ_TimeSpan)),
	symbol.NewScalarFunction("datetime", types.Typ_DateTime, param("value", types.Typ_String)),
	symbol.NewScalarFunction("datetime_add", types.Typ_DateTime,
		param("part", types.Typ_String),
		param("value", types.Typ_Long),
		param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("datetime_diff", types.Typ_Long,
		param("part", types.Typ_String),
		param("datetime1", types.Typ_DateTime),
		param("datetime2", types.Typ_DateTime)),
	symbol.NewScalarFunction("startofday", types.Typ_DateTime, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("startofweek", types.Typ_DateTime, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("startofmonth", types.Typ_DateTime, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("startofyear", types.Typ_DateTime, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("endofday", types.Typ_DateTime, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("endofweek", types.Typ_DateTime, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("endofmonth", types.Typ_DateTime, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("endofyear", types.Typ_DateTime, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("dayofweek", types.Typ_TimeSpan, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("dayofmonth", types.Typ_Long, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("dayofyear", types.Typ_Long, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("weekofyear", types.Typ_Long, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("monthofyear", types.Typ_Long, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("getyear", types.Typ_Long, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("getmonth", types.Typ_Long, param("datetime", types.Typ_DateTime)),
	symbol.NewScalarFunction("format_datetime", types.Typ_String,
		param("datetime", types.Typ_DateTime),
		param("format", types.Typ_String)),
	symbol.NewScalarFunction("bin", types.Typ_DateTime,
		param("value", types.Typ_DateTime),
		param("roundTo", types.Typ_TimeSpan)),

	// TimeSpan functions
	symbol.NewScalarFunction("timespan", types.Typ_TimeSpan, param("value", types.Typ_String)),
	symbol.NewScalarFunction("totimespan", types.Typ_TimeSpan, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("format_timespan", types.Typ_String,
		param("timespan", types.Typ_TimeSpan),
		param("format", types.Typ_String)),

	// Math functions
	symbol.NewScalarFunction("abs", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("ceil", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("floor", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("round", types.Typ_Real,
		param("x", types.Typ_Real),
		optParam("precision", types.Typ_Long)),
	symbol.NewScalarFunction("sqrt", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("pow", types.Typ_Real, param("base", types.Typ_Real), param("exp", types.Typ_Real)),
	symbol.NewScalarFunction("log", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("log10", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("log2", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("exp", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("exp10", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("exp2", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("sin", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("cos", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("tan", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("asin", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("acos", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("atan", types.Typ_Real, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("atan2", types.Typ_Real, param("y", types.Typ_Real), param("x", types.Typ_Real)),
	symbol.NewScalarFunction("sign", types.Typ_Long, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("isnan", types.Typ_Bool, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("isinf", types.Typ_Bool, param("x", types.Typ_Real)),
	symbol.NewScalarFunction("isfinite", types.Typ_Bool, param("x", types.Typ_Real)),

	// Type conversion
	symbol.NewScalarFunction("toint", types.Typ_Int, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("tolong", types.Typ_Long, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("todouble", types.Typ_Real, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("toreal", types.Typ_Real, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("todecimal", types.Typ_Decimal, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("tobool", types.Typ_Bool, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("todatetime", types.Typ_DateTime, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("toguid", types.Typ_Guid, param("value", types.Typ_Dynamic)),
	symbol.NewScalarFunction("todynamic", types.Typ_Dynamic, param("value", types.Typ_String)),

	// Null/empty handling
	symbol.NewScalarFunction("isnull", types.Typ_Bool, param("expr", types.Typ_Dynamic)),
	symbol.NewScalarFunction("isnotnull", types.Typ_Bool, param("expr", types.Typ_Dynamic)),
	symbol.NewScalarFunction("isempty", types.Typ_Bool, param("expr", types.Typ_String)),
	symbol.NewScalarFunction("isnotempty", types.Typ_Bool, param("expr", types.Typ_String)),
	symbol.NewScalarFunction("coalesce", types.Typ_Dynamic,
		param("expr1", types.Typ_Dynamic),
		param("expr2", types.Typ_Dynamic)),
	symbol.NewScalarFunction("iff", types.Typ_Dynamic,
		param("condition", types.Typ_Bool),
		param("ifTrue", types.Typ_Dynamic),
		param("ifFalse", types.Typ_Dynamic)),
	symbol.NewScalarFunction("iif", types.Typ_Dynamic, // Alias
		param("condition", types.Typ_Bool),
		param("ifTrue", types.Typ_Dynamic),
		param("ifFalse", types.Typ_Dynamic)),
	symbol.NewScalarFunction("case", types.Typ_Dynamic,
		param("condition", types.Typ_Bool),
		param("result", types.Typ_Dynamic)),

	// Array/Dynamic functions
	symbol.NewScalarFunction("array_length", types.Typ_Long, param("arr", types.Typ_Dynamic)),
	symbol.NewScalarFunction("array_concat", types.Typ_Dynamic,
		param("arr1", types.Typ_Dynamic),
		param("arr2", types.Typ_Dynamic)),
	symbol.NewScalarFunction("array_slice", types.Typ_Dynamic,
		param("arr", types.Typ_Dynamic),
		param("start", types.Typ_Long),
		param("end", types.Typ_Long)),
	symbol.NewScalarFunction("array_sort_asc", types.Typ_Dynamic, param("arr", types.Typ_Dynamic)),
	symbol.NewScalarFunction("array_sort_desc", types.Typ_Dynamic, param("arr", types.Typ_Dynamic)),
	symbol.NewScalarFunction("array_reverse", types.Typ_Dynamic, param("arr", types.Typ_Dynamic)),
	symbol.NewScalarFunction("pack", types.Typ_Dynamic),
	symbol.NewScalarFunction("pack_all", types.Typ_Dynamic),
	symbol.NewScalarFunction("bag_keys", types.Typ_Dynamic, param("bag", types.Typ_Dynamic)),
	symbol.NewScalarFunction("bag_has_key", types.Typ_Bool,
		param("bag", types.Typ_Dynamic),
		param("key", types.Typ_String)),

	// Hash functions
	symbol.NewScalarFunction("hash", types.Typ_Long, param("source", types.Typ_Dynamic)),
	symbol.NewScalarFunction("hash_sha256", types.Typ_String, param("source", types.Typ_String)),
	symbol.NewScalarFunction("hash_md5", types.Typ_String, param("source", types.Typ_String)),

	// GUID functions
	symbol.NewScalarFunction("new_guid", types.Typ_Guid),
	symbol.NewScalarFunction("guid", types.Typ_Guid, param("value", types.Typ_String)),

	// Geo functions (subset)
	symbol.NewScalarFunction("geo_point_to_s2cell", types.Typ_String,
		param("longitude", types.Typ_Real),
		param("latitude", types.Typ_Real),
		optParam("level", types.Typ_Long)),
	symbol.NewScalarFunction("geo_distance_2points", types.Typ_Real,
		param("p1_longitude", types.Typ_Real),
		param("p1_latitude", types.Typ_Real),
		param("p2_longitude", types.Typ_Real),
		param("p2_latitude", types.Typ_Real)),

	// Special functions
	symbol.NewScalarFunction("current_principal", types.Typ_String),
	symbol.NewScalarFunction("current_cluster_endpoint", types.Typ_String),
	symbol.NewScalarFunction("ingestion_time", types.Typ_DateTime),
	symbol.NewScalarFunction("cursor_after", types.Typ_Bool, param("cursor", types.Typ_String)),
	symbol.NewScalarFunction("extent_id", types.Typ_Guid),
	symbol.NewScalarFunction("extent_tags", types.Typ_Dynamic),
}

// Aggregates contains all built-in aggregate functions.
var Aggregates = []*symbol.AggregateSymbol{
	symbol.NewAggregate("count", types.Typ_Long),
	symbol.NewAggregate("countif", types.Typ_Long, param("predicate", types.Typ_Bool)),
	symbol.NewAggregate("dcount", types.Typ_Long,
		param("expr", types.Typ_Dynamic),
		optParam("accuracy", types.Typ_Long)),
	symbol.NewAggregate("dcountif", types.Typ_Long,
		param("expr", types.Typ_Dynamic),
		param("predicate", types.Typ_Bool)),

	symbol.NewAggregate("sum", types.Typ_Real, param("expr", types.Typ_Real)),
	symbol.NewAggregate("sumif", types.Typ_Real,
		param("expr", types.Typ_Real),
		param("predicate", types.Typ_Bool)),
	symbol.NewAggregate("avg", types.Typ_Real, param("expr", types.Typ_Real)),
	symbol.NewAggregate("avgif", types.Typ_Real,
		param("expr", types.Typ_Real),
		param("predicate", types.Typ_Bool)),

	symbol.NewAggregate("min", types.Typ_Dynamic, param("expr", types.Typ_Dynamic)),
	symbol.NewAggregate("minif", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		param("predicate", types.Typ_Bool)),
	symbol.NewAggregate("max", types.Typ_Dynamic, param("expr", types.Typ_Dynamic)),
	symbol.NewAggregate("maxif", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		param("predicate", types.Typ_Bool)),

	symbol.NewAggregate("stdev", types.Typ_Real, param("expr", types.Typ_Real)),
	symbol.NewAggregate("stdevp", types.Typ_Real, param("expr", types.Typ_Real)),
	symbol.NewAggregate("variance", types.Typ_Real, param("expr", types.Typ_Real)),
	symbol.NewAggregate("variancep", types.Typ_Real, param("expr", types.Typ_Real)),

	symbol.NewAggregate("percentile", types.Typ_Real,
		param("expr", types.Typ_Real),
		param("percentile", types.Typ_Real)),
	symbol.NewAggregate("percentiles", types.Typ_Dynamic,
		param("expr", types.Typ_Real),
		param("percentiles", types.Typ_Dynamic)),
	symbol.NewAggregate("percentiles_array", types.Typ_Dynamic,
		param("expr", types.Typ_Real),
		param("percentiles", types.Typ_Dynamic)),

	symbol.NewAggregate("make_list", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		optParam("maxSize", types.Typ_Long)),
	symbol.NewAggregate("make_list_if", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		param("predicate", types.Typ_Bool)),
	symbol.NewAggregate("make_set", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		optParam("maxSize", types.Typ_Long)),
	symbol.NewAggregate("make_set_if", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		param("predicate", types.Typ_Bool)),
	symbol.NewAggregate("make_bag", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		optParam("maxSize", types.Typ_Long)),
	symbol.NewAggregate("make_bag_if", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		param("predicate", types.Typ_Bool)),

	symbol.NewAggregate("any", types.Typ_Dynamic, param("expr", types.Typ_Dynamic)),
	symbol.NewAggregate("anyif", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		param("predicate", types.Typ_Bool)),
	symbol.NewAggregate("take_any", types.Typ_Dynamic, param("expr", types.Typ_Dynamic)),
	symbol.NewAggregate("take_anyif", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		param("predicate", types.Typ_Bool)),

	symbol.NewAggregate("arg_max", types.Typ_Dynamic,
		param("exprToMaximize", types.Typ_Dynamic),
		param("exprToReturn", types.Typ_Dynamic)),
	symbol.NewAggregate("arg_min", types.Typ_Dynamic,
		param("exprToMinimize", types.Typ_Dynamic),
		param("exprToReturn", types.Typ_Dynamic)),

	symbol.NewAggregate("binary_all_and", types.Typ_Long, param("expr", types.Typ_Long)),
	symbol.NewAggregate("binary_all_or", types.Typ_Long, param("expr", types.Typ_Long)),
	symbol.NewAggregate("binary_all_xor", types.Typ_Long, param("expr", types.Typ_Long)),

	symbol.NewAggregate("hll", types.Typ_Dynamic,
		param("expr", types.Typ_Dynamic),
		optParam("accuracy", types.Typ_Long)),
	symbol.NewAggregate("hll_merge", types.Typ_Dynamic, param("hll", types.Typ_Dynamic)),
	symbol.NewAggregate("tdigest", types.Typ_Dynamic, param("expr", types.Typ_Real)),
	symbol.NewAggregate("tdigest_merge", types.Typ_Dynamic, param("tdigest", types.Typ_Dynamic)),
}

// DefaultScope returns a scope with all built-in functions and aggregates.
func DefaultScope() *symbol.Scope {
	scope := symbol.NewScope(nil)
	for _, fn := range ScalarFunctions {
		scope.Define(fn)
	}
	for _, agg := range Aggregates {
		scope.Define(agg)
	}
	return scope
}

