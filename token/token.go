package token

import "strconv"

// Token represents a lexical token type.
type Token int

// Token constants
const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	COMMENT

	literalBeg
	// Literals
	IDENT    // identifier
	INT      // 123, 0x1F
	REAL     // 1.23, 1.23e10
	STRING   // "abc", 'abc', @"abc"
	DATETIME // datetime(2023-01-01)
	TIMESPAN // 1d, 2h, 3m, timespan(1.02:03:04)
	GUID     // guid(...)
	BOOL     // true, false
	DYNAMIC  // dynamic([1,2,3])
	TYPE     // typeof(string)
	literalEnd

	operatorBeg
	// Operators and delimiters
	ADD // +
	SUB // -
	MUL // *
	QUO // /
	REM // %

	EQL    // ==
	NEQ    // != or <>
	LSS    // <
	GTR    // >
	LEQ    // <=
	GEQ    // >=
	TILDE  // =~
	NTILDE // !~

	PIPE   // |
	ASSIGN // =
	COLON  // :
	SEMI   // ;
	COMMA  // ,
	DOT    // .
	DOTDOT     // ..
	ARROW      // =>
	DASHDASH   // -- (undirected edge)
	DASHGT     // --> (directed edge forward)
	LTDASH     // <-- (directed edge backward)
	DASHLBRACK // -[ (edge name start)
	LTDASHLBRACK // <-[ (edge name start backward)
	RBRACKDASH   // ]- (edge name end undirected)
	RBRACKDASHGT // ]-> (edge name end forward)

	LPAREN   // (
	RPAREN   // )
	LBRACKET // [
	RBRACKET // ]
	LBRACE   // {
	RBRACE   // }
	operatorEnd

	keywordBeg
	// Keywords - Query operators
	AS
	BY
	CONSUME
	COUNT
	DISTINCT
	EVALUATE
	EXTEND
	FACET
	FILTER
	FIND
	FORK
	GETSCHEMA
	INVOKE
	GRAPHMATCH
	GRAPHMARKCOMPONENTS
	GRAPHSHORTESTPATHS
	GRAPHTOTABLE
	GRAPHWHEREEDGES
	GRAPHWHERENODES
	JOIN
	LIMIT
	LOOKUP
	MAKEGRAPH
	MAKESERIES
	MVAPPLY
	MVEXPAND
	ORDER
	PARSE
	PARSEKV
	PARSEWHERE
	PARTITION
	PRINT
	PROJECT
	PROJECTAWAY
	PROJECTKEEP
	PROJECTRENAME
	PROJECTREORDER
	RANGE
	REDUCE
	RENDER
	SAMPLE
	SAMPLEDISTINCT
	SCAN
	SEARCH
	SERIALIZE
	SORT
	SUMMARIZE
	TAKE
	TOP
	TOPHITTERS
	TOPNESTED
	UNION
	WHERE

	// Keywords - Statements
	ALIAS
	DECLARE
	LET
	PATTERN
	RESTRICT
	SET

	// Keywords - Clauses
	ACCESS
	ASC
	BETWEEN
	DATABASE
	DATASCOPE
	DATATABLE
	DEFAULT
	DESC
	EDGES
	EXTERNALDATA
	FIRST
	FROM
	HOTCACHE
	IN
	KIND
	LAST
	MATERIALIZE
	NODES
	NOOPTIMIZATION
	NULLS
	OF
	ON
	PARTITIONEDBY
	STEP
	TO
	TOSCALAR
	TOTABLE
	VIEW
	WITHNODEID
	WITH
	WITHSOURCE

	// Keywords - Logical
	AND
	NOT
	OR

	// Keywords - Types
	BOOLTYPE
	DATETIMETYPE
	DECIMALTYPE
	DYNAMICTYPE
	GUIDTYPE
	INTTYPE
	LONGTYPE
	REALTYPE
	STRINGTYPE
	TIMESPANTYPE

	// Keywords - String operators (positive)
	CONTAINS
	CONTAINSCS
	ENDSWITH
	ENDSWITHCS
	HAS
	HASALL
	HASANY
	HASCS
	HASPREFIX
	HASPREFIXCS
	HASSUFFIX
	HASSUFFIXCS
	LIKE
	LIKECS
	MATCHESREGEX
	STARTSWITH
	STARTSWITHCS

	// Keywords - String operators (negated)
	NOTCONTAINS    // !contains
	NOTCONTAINSCS  // !contains_cs
	NOTENDSWITH    // !endswith
	NOTENDSWITHCS  // !endswith_cs
	NOTHAS         // !has
	NOTHASCS       // !has_cs
	NOTHASPREFIX   // !hasprefix
	NOTHASPREFIXCS // !hasprefix_cs
	NOTHASSUFFIX   // !hassuffix
	NOTHASSUFFIXCS // !hassuffix_cs
	NOTLIKE        // notlike
	NOTLIKECS      // notlikecs
	NOTSTARTSWITH  // !startswith
	NOTSTARTSWITCS // !startswith_cs

	// Keywords - Negated list/range operators
	NOTBETWEEN // !between
	NOTIN      // !in
	NOTINCI    // !in~ (case-insensitive)

	// Keywords - Misc
	CLUSTER
	NULL
	PACK
	TYPEOF
	keywordEnd
)

var tokenStrings = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	COMMENT: "COMMENT",

	IDENT:    "IDENT",
	INT:      "INT",
	REAL:     "REAL",
	STRING:   "STRING",
	DATETIME: "DATETIME",
	TIMESPAN: "TIMESPAN",
	GUID:     "GUID",
	BOOL:     "BOOL",
	DYNAMIC:  "DYNAMIC",
	TYPE:     "TYPE",

	ADD: "+",
	SUB: "-",
	MUL: "*",
	QUO: "/",
	REM: "%",

	EQL:    "==",
	NEQ:    "!=",
	LSS:    "<",
	GTR:    ">",
	LEQ:    "<=",
	GEQ:    ">=",
	TILDE:  "=~",
	NTILDE: "!~",

	PIPE:   "|",
	ASSIGN: "=",
	COLON:  ":",
	SEMI:   ";",
	COMMA:  ",",
	DOT:          ".",
	DOTDOT:       "..",
	ARROW:        "=>",
	DASHDASH:     "--",
	DASHGT:       "-->",
	LTDASH:       "<--",
	DASHLBRACK:   "-[",
	LTDASHLBRACK: "<-[",
	RBRACKDASH:   "]-",
	RBRACKDASHGT: "]->",

	LPAREN:   "(",
	RPAREN:   ")",
	LBRACKET: "[",
	RBRACKET: "]",
	LBRACE:   "{",
	RBRACE:   "}",

	AS:             "as",
	BY:             "by",
	CONSUME:        "consume",
	COUNT:          "count",
	DISTINCT:       "distinct",
	EVALUATE:       "evaluate",
	EXTEND:         "extend",
	FACET:          "facet",
	FILTER:         "filter",
	FIND:           "find",
	FORK:           "fork",
	GETSCHEMA:      "getschema",
	INVOKE:         "invoke",
	JOIN:           "join",
	GRAPHMATCH:          "graph-match",
	GRAPHMARKCOMPONENTS: "graph-mark-components",
	GRAPHSHORTESTPATHS:  "graph-shortest-paths",
	GRAPHTOTABLE:        "graph-to-table",
	GRAPHWHEREEDGES:     "graph-where-edges",
	GRAPHWHERENODES:     "graph-where-nodes",
	LIMIT:               "limit",
	LOOKUP:              "lookup",
	MAKEGRAPH:           "make-graph",
	MAKESERIES:          "make-series",
	MVAPPLY:             "mv-apply",
	MVEXPAND:            "mv-expand",
	ORDER:          "order",
	PARSE:          "parse",
	PARSEKV:        "parse-kv",
	PARSEWHERE:     "parse-where",
	PARTITION:      "partition",
	PRINT:          "print",
	PROJECT:        "project",
	PROJECTAWAY:    "project-away",
	PROJECTKEEP:    "project-keep",
	PROJECTRENAME:  "project-rename",
	PROJECTREORDER: "project-reorder",
	RANGE:          "range",
	REDUCE:         "reduce",
	RENDER:         "render",
	SAMPLE:         "sample",
	SAMPLEDISTINCT: "sample-distinct",
	SCAN:           "scan",
	SEARCH:         "search",
	SERIALIZE:      "serialize",
	SORT:           "sort",
	SUMMARIZE:      "summarize",
	TAKE:           "take",
	TOP:            "top",
	TOPHITTERS:     "top-hitters",
	TOPNESTED:      "top-nested",
	UNION:          "union",
	WHERE:          "where",
	ALIAS:          "alias",
	DECLARE:        "declare",
	LET:            "let",
	PATTERN:        "pattern",
	RESTRICT:       "restrict",
	SET:            "set",
	ACCESS:         "access",
	ASC:            "asc",
	BETWEEN:        "between",
	DATABASE:       "database",
	DATASCOPE:      "datascope",
	DATATABLE:      "datatable",
	DEFAULT:        "default",
	DESC:           "desc",
	EDGES:          "edges",
	EXTERNALDATA:   "externaldata",
	FIRST:          "first",
	FROM:           "from",
	HOTCACHE:       "hotcache",
	IN:             "in",
	KIND:           "kind",
	LAST:           "last",
	MATERIALIZE:    "materialize",
	NODES:          "nodes",
	NOOPTIMIZATION: "nooptimization",
	NULLS:          "nulls",
	OF:             "of",
	ON:             "on",
	PARTITIONEDBY:  "partitionedby",
	STEP:           "step",
	TO:             "to",
	TOSCALAR:       "toscalar",
	TOTABLE:        "totable",
	WITHNODEID:     "with_node_id",
	VIEW:           "view",
	WITH:           "with",
	WITHSOURCE:     "withsource",
	AND:            "and",
	NOT:            "not",
	OR:             "or",
	BOOLTYPE:       "bool",
	DATETIMETYPE:   "datetime",
	DECIMALTYPE:    "decimal",
	DYNAMICTYPE:    "dynamic",
	GUIDTYPE:       "guid",
	INTTYPE:        "int",
	LONGTYPE:       "long",
	REALTYPE:       "real",
	STRINGTYPE:     "string",
	TIMESPANTYPE:   "timespan",
	CONTAINS:       "contains",
	CONTAINSCS:     "contains_cs",
	ENDSWITH:       "endswith",
	ENDSWITHCS:     "endswith_cs",
	HAS:            "has",
	HASALL:         "has_all",
	HASANY:         "has_any",
	HASCS:          "has_cs",
	HASPREFIX:      "hasprefix",
	HASPREFIXCS:    "hasprefix_cs",
	HASSUFFIX:      "hassuffix",
	HASSUFFIXCS:    "hassuffix_cs",
	LIKE:           "like",
	LIKECS:         "likecs",
	MATCHESREGEX:   "matches regex",
	STARTSWITH:     "startswith",
	STARTSWITHCS:   "startswith_cs",

	// Negated string operators
	NOTCONTAINS:    "!contains",
	NOTCONTAINSCS:  "!contains_cs",
	NOTENDSWITH:    "!endswith",
	NOTENDSWITHCS:  "!endswith_cs",
	NOTHAS:         "!has",
	NOTHASCS:       "!has_cs",
	NOTHASPREFIX:   "!hasprefix",
	NOTHASPREFIXCS: "!hasprefix_cs",
	NOTHASSUFFIX:   "!hassuffix",
	NOTHASSUFFIXCS: "!hassuffix_cs",
	NOTLIKE:        "notlike",
	NOTLIKECS:      "notlikecs",
	NOTSTARTSWITH:  "!startswith",
	NOTSTARTSWITCS: "!startswith_cs",

	// Negated list/range operators
	NOTBETWEEN: "!between",
	NOTIN:      "!in",
	NOTINCI:    "!in~",

	CLUSTER: "cluster",
	NULL:           "null",
	PACK:           "pack",
	TYPEOF:         "typeof",
}

// String returns the string representation of the token.
func (t Token) String() string {
	if 0 <= t && int(t) < len(tokenStrings) {
		s := tokenStrings[t]
		if s != "" {
			return s
		}
	}
	return "token(" + strconv.Itoa(int(t)) + ")"
}

// IsLiteral reports whether the token is a literal.
func (t Token) IsLiteral() bool {
	return literalBeg < t && t < literalEnd
}

// IsOperator reports whether the token is an operator or delimiter.
func (t Token) IsOperator() bool {
	return operatorBeg < t && t < operatorEnd
}

// IsKeyword reports whether the token is a keyword.
func (t Token) IsKeyword() bool {
	return keywordBeg < t && t < keywordEnd
}

// keywords maps keyword strings to token types.
var keywords map[string]Token

func init() {
	keywords = make(map[string]Token, keywordEnd-keywordBeg)
	for t := keywordBeg + 1; t < keywordEnd; t++ {
		keywords[tokenStrings[t]] = t
	}
	// Add alternative spellings
	keywords["mvapply"] = MVAPPLY
	keywords["mvexpand"] = MVEXPAND
	keywords["makegraph"] = MAKEGRAPH
	keywords["graphmatch"] = GRAPHMATCH
	keywords["graphshortestpaths"] = GRAPHSHORTESTPATHS
	keywords["graphmarkcomponents"] = GRAPHMARKCOMPONENTS
	keywords["graphtotable"] = GRAPHTOTABLE
	keywords["graphwherenodes"] = GRAPHWHERENODES
	keywords["graphwhereedges"] = GRAPHWHEREEDGES
	keywords["external_data"] = EXTERNALDATA
	keywords["with_source"] = WITHSOURCE
	keywords["boolean"] = BOOLTYPE
	keywords["date"] = DATETIMETYPE
	keywords["time"] = TIMESPANTYPE
	keywords["int64"] = LONGTYPE

	// Alternative spellings for legacy notcontains/notlike (without !)
	keywords["notcontains"] = NOTCONTAINS
	keywords["notcontains_cs"] = NOTCONTAINSCS
	keywords["notcontainscs"] = NOTCONTAINSCS
}

// Lookup returns the token type for the given identifier.
// If the identifier is a keyword, the corresponding keyword token is returned.
// Otherwise, IDENT is returned.
func Lookup(ident string) Token {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}

// Precedence returns the operator precedence for binary operators.
// Returns 0 for non-binary operators.
func (t Token) Precedence() int {
	switch t {
	case OR:
		return 1
	case AND:
		return 2
	case EQL, NEQ, LSS, GTR, LEQ, GEQ, TILDE, NTILDE,
		// Positive string operators
		CONTAINS, CONTAINSCS, STARTSWITH, STARTSWITHCS,
		ENDSWITH, ENDSWITHCS, HAS, HASCS, HASALL, HASANY,
		HASPREFIX, HASPREFIXCS, HASSUFFIX, HASSUFFIXCS,
		LIKE, LIKECS, MATCHESREGEX,
		// Negated string operators
		NOTCONTAINS, NOTCONTAINSCS, NOTSTARTSWITH, NOTSTARTSWITCS,
		NOTENDSWITH, NOTENDSWITHCS, NOTHAS, NOTHASCS,
		NOTHASPREFIX, NOTHASPREFIXCS, NOTHASSUFFIX, NOTHASSUFFIXCS,
		NOTLIKE, NOTLIKECS,
		// List/range operators
		BETWEEN, NOTBETWEEN, IN, NOTIN, NOTINCI:
		return 3
	case ADD, SUB:
		return 4
	case MUL, QUO, REM:
		return 5
	default:
		return 0
	}
}
