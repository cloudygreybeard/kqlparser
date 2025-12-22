package lexer

import (
	"testing"

	"github.com/cloudygreybeard/kqlparser/token"
)

func TestScanTokens(t *testing.T) {
	tests := []struct {
		src    string
		tokens []token.Token
	}{
		// Simple identifiers and keywords
		{"foo", []token.Token{token.IDENT, token.EOF}},
		{"where", []token.Token{token.WHERE, token.EOF}},
		{"project", []token.Token{token.PROJECT, token.EOF}},
		{"summarize", []token.Token{token.SUMMARIZE, token.EOF}},

		// Hyphenated keywords
		{"project-away", []token.Token{token.PROJECT_AWAY, token.EOF}},
		{"make-series", []token.Token{token.MAKESERIES, token.EOF}},
		{"mv-expand", []token.Token{token.MVEXPAND, token.EOF}},

		// Operators
		{"+", []token.Token{token.ADD, token.EOF}},
		{"-", []token.Token{token.SUB, token.EOF}},
		{"*", []token.Token{token.MUL, token.EOF}},
		{"/", []token.Token{token.QUO, token.EOF}},
		{"|", []token.Token{token.PIPE, token.EOF}},
		{"==", []token.Token{token.EQL, token.EOF}},
		{"!=", []token.Token{token.NEQ, token.EOF}},
		{"<>", []token.Token{token.NEQ, token.EOF}},
		{"<=", []token.Token{token.LEQ, token.EOF}},
		{">=", []token.Token{token.GEQ, token.EOF}},
		{"=~", []token.Token{token.TILDE, token.EOF}},
		{"!~", []token.Token{token.NTILDE, token.EOF}},
		{"..", []token.Token{token.DOTDOT, token.EOF}},
		{"=>", []token.Token{token.ARROW, token.EOF}},

		// Numbers
		{"123", []token.Token{token.INT, token.EOF}},
		{"0x1F", []token.Token{token.INT, token.EOF}},
		{"1.23", []token.Token{token.REAL, token.EOF}},
		{"1.23e10", []token.Token{token.REAL, token.EOF}},
		{"1e10", []token.Token{token.REAL, token.EOF}},

		// Timespan literals
		{"1d", []token.Token{token.TIMESPAN, token.EOF}},
		{"2h", []token.Token{token.TIMESPAN, token.EOF}},
		{"30m", []token.Token{token.TIMESPAN, token.EOF}},
		{"1.5d", []token.Token{token.TIMESPAN, token.EOF}},
		{"100ms", []token.Token{token.TIMESPAN, token.EOF}},

		// Strings
		{`"hello"`, []token.Token{token.STRING, token.EOF}},
		{`'hello'`, []token.Token{token.STRING, token.EOF}},
		{`"hello\nworld"`, []token.Token{token.STRING, token.EOF}},
		{`@"hello"`, []token.Token{token.STRING, token.EOF}},

		// Query structure
		{"T | where x > 10", []token.Token{
			token.IDENT, token.PIPE, token.WHERE, token.IDENT, token.GTR, token.INT, token.EOF,
		}},

		// Function call
		{"count()", []token.Token{
			token.COUNT, token.LPAREN, token.RPAREN, token.EOF,
		}},

		// Comments are skipped
		{"foo // comment\nbar", []token.Token{token.IDENT, token.IDENT, token.EOF}},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			l := New("test", tt.src)
			var got []token.Token
			for {
				tok := l.Scan()
				got = append(got, tok.Type)
				if tok.Type == token.EOF {
					break
				}
			}
			if len(got) != len(tt.tokens) {
				t.Errorf("token count: got %d, want %d", len(got), len(tt.tokens))
				t.Errorf("got: %v", got)
				return
			}
			for i, want := range tt.tokens {
				if got[i] != want {
					t.Errorf("token[%d]: got %v, want %v", i, got[i], want)
				}
			}
			if errs := l.Errors(); len(errs) > 0 {
				t.Errorf("unexpected errors: %v", errs)
			}
		})
	}
}

func TestScanLiterals(t *testing.T) {
	tests := []struct {
		src     string
		tokType token.Token
		lit     string
	}{
		{"hello", token.IDENT, "hello"},
		{"_foo", token.IDENT, "_foo"},
		{"$bar", token.IDENT, "$bar"},
		{"123", token.INT, "123"},
		{"0x1F", token.INT, "0x1F"},
		{"1.5", token.REAL, "1.5"},
		{"1e10", token.REAL, "1e10"},
		{"1.5e-10", token.REAL, "1.5e-10"},
		{`"hello"`, token.STRING, `"hello"`},
		{`'world'`, token.STRING, `'world'`},
		{`"hello\nworld"`, token.STRING, `"hello\nworld"`},
		{`@"c:\path"`, token.STRING, `@"c:\path"`},
		{"7d", token.TIMESPAN, "7d"},
		{"24h", token.TIMESPAN, "24h"},
		{"30m", token.TIMESPAN, "30m"},
		{"1.5d", token.TIMESPAN, "1.5d"},
	}

	for _, tt := range tests {
		t.Run(tt.src, func(t *testing.T) {
			l := New("test", tt.src)
			tok := l.Scan()
			if tok.Type != tt.tokType {
				t.Errorf("type: got %v, want %v", tok.Type, tt.tokType)
			}
			if tok.Lit != tt.lit {
				t.Errorf("lit: got %q, want %q", tok.Lit, tt.lit)
			}
		})
	}
}

func TestPositions(t *testing.T) {
	src := "foo\nbar baz"
	l := New("test.kql", src)

	tok1 := l.Scan() // foo
	pos1 := l.File().Position(tok1.Pos)
	if pos1.Line != 1 || pos1.Column != 1 {
		t.Errorf("foo position: got %v, want 1:1", pos1)
	}

	tok2 := l.Scan() // bar
	pos2 := l.File().Position(tok2.Pos)
	if pos2.Line != 2 || pos2.Column != 1 {
		t.Errorf("bar position: got %v, want 2:1", pos2)
	}

	tok3 := l.Scan() // baz
	pos3 := l.File().Position(tok3.Pos)
	if pos3.Line != 2 || pos3.Column != 5 {
		t.Errorf("baz position: got %v, want 2:5", pos3)
	}
}

func TestMatchesRegex(t *testing.T) {
	l := New("test", "x matches regex y")
	tokens := []struct {
		typ token.Token
		lit string
	}{
		{token.IDENT, "x"},
		{token.MATCHES_REGEX, "matches regex"},
		{token.IDENT, "y"},
		{token.EOF, ""},
	}

	for i, want := range tokens {
		tok := l.Scan()
		if tok.Type != want.typ {
			t.Errorf("token[%d] type: got %v, want %v", i, tok.Type, want.typ)
		}
		if tok.Lit != want.lit {
			t.Errorf("token[%d] lit: got %q, want %q", i, tok.Lit, want.lit)
		}
	}
}

