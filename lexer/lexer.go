// Package lexer implements a lexical scanner for KQL.
package lexer

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cloudygreybeard/kqlparser/token"
)

// Lexer holds the state of the scanner.
type Lexer struct {
	file *token.File // Source file for position info
	src  string      // Source text

	// Scanning state
	ch       rune // Current character
	offset   int  // Current byte offset
	rdOffset int  // Reading offset (position after current char)

	// Error handling
	errors ErrorList
}

// Error represents a lexer error.
type Error struct {
	Pos token.Position
	Msg string
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Pos, e.Msg)
}

// ErrorList is a list of lexer errors.
type ErrorList []Error

func (el ErrorList) Error() string {
	switch len(el) {
	case 0:
		return "no errors"
	case 1:
		return el[0].Error()
	default:
		return fmt.Sprintf("%s (and %d more errors)", el[0], len(el)-1)
	}
}

// Err returns an error if the list is non-empty, nil otherwise.
func (el ErrorList) Err() error {
	if len(el) == 0 {
		return nil
	}
	return el
}

// Token represents a scanned token with its position and value.
type Token struct {
	Type token.Token
	Pos  token.Pos
	Lit  string
}

// New creates a new Lexer for the given source text.
func New(filename, src string) *Lexer {
	l := &Lexer{
		file: token.NewFile(filename, src),
		src:  src,
	}
	l.next() // Initialize first character
	return l
}

// Errors returns any errors encountered during scanning.
func (l *Lexer) Errors() ErrorList {
	return l.errors
}

// File returns the source file.
func (l *Lexer) File() *token.File {
	return l.file
}

// Offset returns the current byte offset in the source.
func (l *Lexer) Offset() int {
	return l.offset
}

// Reset resets the lexer to the given byte offset.
// This is used for lookahead operations.
func (l *Lexer) Reset(offset int) {
	l.offset = offset
	l.rdOffset = offset
	if offset < len(l.src) {
		r, w := utf8.DecodeRuneInString(l.src[offset:])
		l.ch = r
		l.rdOffset = offset + w
	} else {
		l.ch = -1 // EOF
	}
}

const eof = -1

// next reads the next Unicode character into l.ch.
func (l *Lexer) next() {
	if l.rdOffset >= len(l.src) {
		l.offset = len(l.src)
		l.ch = eof
		return
	}
	l.offset = l.rdOffset
	r, w := utf8.DecodeRuneInString(l.src[l.rdOffset:])
	if r == utf8.RuneError && w == 1 {
		l.error(l.offset, "invalid UTF-8 encoding")
	}
	l.rdOffset += w
	l.ch = r
}

// peek returns the next character without consuming it.
func (l *Lexer) peek() rune {
	if l.rdOffset >= len(l.src) {
		return eof
	}
	r, _ := utf8.DecodeRuneInString(l.src[l.rdOffset:])
	return r
}

func (l *Lexer) error(offset int, msg string) {
	pos := l.file.Position(l.file.Pos(offset))
	l.errors = append(l.errors, Error{Pos: pos, Msg: msg})
}

// skipWhitespace skips whitespace and comments.
func (l *Lexer) skipWhitespace() {
	for {
		switch l.ch {
		case ' ', '\t', '\r', '\n':
			l.next()
		case '/':
			if l.peek() == '/' {
				l.scanComment()
			} else {
				return
			}
		default:
			return
		}
	}
}

// scanComment scans a // comment.
func (l *Lexer) scanComment() {
	// Skip //
	l.next()
	l.next()
	for l.ch != '\n' && l.ch != eof {
		l.next()
	}
}

// Scan scans the next token and returns it.
func (l *Lexer) Scan() Token {
	l.skipWhitespace()

	pos := l.file.Pos(l.offset)
	ch := l.ch

	// Check for h/H prefix strings BEFORE isLetter check
	if ch == 'h' || ch == 'H' {
		if next := l.peek(); next == '"' || next == '\'' || next == '`' || next == '~' {
			startOffset := l.offset // remember start for multi-line strings
			l.next()                // consume h
			if l.ch == '`' || l.ch == '~' {
				return l.scanMultiLineStringFrom(pos, startOffset)
			}
			return l.scanString(pos, l.ch)
		}
	}

	switch {
	case isLetter(ch):
		return l.scanIdentifier(pos)
	case isDigit(ch):
		return l.scanNumber(pos)
	case ch == '"' || ch == '\'':
		return l.scanString(pos, ch)
	case ch == '@' && (l.peek() == '"' || l.peek() == '\''):
		l.next() // consume @
		return l.scanVerbatimString(pos, l.ch)
	case ch == '`':
		return l.scanMultiLineString(pos)
	case ch == '~':
		return l.scanMultiLineString(pos)
	default:
		return l.scanOperator(pos)
	}
}

// scanIdentifier scans an identifier or keyword.
func (l *Lexer) scanIdentifier(pos token.Pos) Token {
	start := l.offset
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.next()
	}
	lit := l.src[start:l.offset]

	// Check for special keyword forms
	tok := token.Lookup(lit)

	// Handle keywords with hyphens (e.g., "make-series", "project-away")
	if l.ch == '-' {
		// Peek ahead to see if this could be a hyphenated keyword
		combined := lit + "-"
		savedOffset := l.offset
		savedRdOffset := l.rdOffset
		savedCh := l.ch
		l.next() // consume -

		// Scan the rest
		wordStart := l.offset
		for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
			l.next()
		}
		if l.offset > wordStart {
			combined += l.src[wordStart:l.offset]
			if kwTok := token.Lookup(combined); kwTok != token.IDENT {
				return Token{Type: kwTok, Pos: pos, Lit: combined}
			}
		}
		// Not a hyphenated keyword, restore state
		l.offset = savedOffset
		l.rdOffset = savedRdOffset
		l.ch = savedCh
	}

	// Handle "matches regex" (two-word keyword)
	if lit == "matches" && l.skipSpacesAndCheck("regex") {
		return Token{Type: token.MATCHESREGEX, Pos: pos, Lit: "matches regex"}
	}

	return Token{Type: tok, Pos: pos, Lit: lit}
}

// skipSpacesAndCheck checks if the next word after optional spaces matches the expected string.
func (l *Lexer) skipSpacesAndCheck(expected string) bool {
	savedOffset := l.offset
	savedRdOffset := l.rdOffset
	savedCh := l.ch

	// Skip spaces
	for l.ch == ' ' || l.ch == '\t' {
		l.next()
	}

	// Check next word
	start := l.offset
	for isLetter(l.ch) {
		l.next()
	}
	word := l.src[start:l.offset]

	if strings.EqualFold(word, expected) {
		return true
	}

	// Restore state
	l.offset = savedOffset
	l.rdOffset = savedRdOffset
	l.ch = savedCh
	return false
}

// scanNumber scans an integer or real number literal.
func (l *Lexer) scanNumber(pos token.Pos) Token {
	start := l.offset
	tok := token.INT

	// Check for hex
	if l.ch == '0' && (l.peek() == 'x' || l.peek() == 'X') {
		l.next() // 0
		l.next() // x
		for isHexDigit(l.ch) {
			l.next()
		}
		return Token{Type: tok, Pos: pos, Lit: l.src[start:l.offset]}
	}

	// Integer part
	for isDigit(l.ch) {
		l.next()
	}

	// Check for timespan suffix
	if isTimespanSuffix(l.ch) {
		l.next()
		// Handle "ms", "min", "minute", "sec", "second", "hr", "hour", "day", etc.
		for isLetter(l.ch) {
			l.next()
		}
		return Token{Type: token.TIMESPAN, Pos: pos, Lit: l.src[start:l.offset]}
	}

	// Fractional part
	if l.ch == '.' && isDigit(l.peek()) {
		tok = token.REAL
		l.next() // consume .
		for isDigit(l.ch) {
			l.next()
		}

		// Check for timespan suffix after decimal (e.g., 1.5d)
		if isTimespanSuffix(l.ch) {
			l.next()
			for isLetter(l.ch) {
				l.next()
			}
			return Token{Type: token.TIMESPAN, Pos: pos, Lit: l.src[start:l.offset]}
		}
	}

	// Exponent
	if l.ch == 'e' || l.ch == 'E' {
		tok = token.REAL
		l.next()
		if l.ch == '+' || l.ch == '-' {
			l.next()
		}
		for isDigit(l.ch) {
			l.next()
		}
	}

	return Token{Type: tok, Pos: pos, Lit: l.src[start:l.offset]}
}

// isTimespanSuffix returns true if the character could start a timespan suffix.
func isTimespanSuffix(ch rune) bool {
	switch ch {
	case 'd', 'h', 'm', 's', 't': // day, hour, minute/ms/milli/micro, second, tick
		return true
	}
	return false
}

// scanString scans a quoted string literal.
func (l *Lexer) scanString(pos token.Pos, quote rune) Token {
	start := l.offset
	l.next() // consume opening quote

	for {
		switch l.ch {
		case quote:
			l.next() // consume closing quote
			return Token{Type: token.STRING, Pos: pos, Lit: l.src[start:l.offset]}
		case '\\':
			l.next() // consume backslash
			if l.ch != eof {
				l.next() // consume escaped char
			}
		case '\n', eof:
			l.error(l.offset, "unterminated string literal")
			return Token{Type: token.STRING, Pos: pos, Lit: l.src[start:l.offset]}
		default:
			l.next()
		}
	}
}

// scanVerbatimString scans a @"..." or @'...' verbatim string.
func (l *Lexer) scanVerbatimString(pos token.Pos, quote rune) Token {
	start := l.offset - 1 // Include the @ we already consumed
	l.next()              // consume opening quote

	for {
		switch l.ch {
		case quote:
			// Check for doubled quote (escape)
			if l.peek() == quote {
				l.next() // consume first quote
				l.next() // consume second quote
				continue
			}
			l.next() // consume closing quote
			return Token{Type: token.STRING, Pos: pos, Lit: l.src[start:l.offset]}
		case '\n', eof:
			l.error(l.offset, "unterminated verbatim string literal")
			return Token{Type: token.STRING, Pos: pos, Lit: l.src[start:l.offset]}
		default:
			l.next()
		}
	}
}

// scanMultiLineString scans a multi-line string (``` ... ``` or ~~~ ... ~~~).
func (l *Lexer) scanMultiLineString(pos token.Pos) Token {
	return l.scanMultiLineStringFrom(pos, l.offset)
}

// scanMultiLineStringFrom scans a multi-line string starting from a specific offset.
// This is used when the string has a prefix like 'h' that was already consumed.
func (l *Lexer) scanMultiLineStringFrom(pos token.Pos, start int) Token {
	delimiter := l.ch // ` or ~

	// Check for triple delimiter
	if l.ch != delimiter {
		l.error(l.offset, "expected multi-line string delimiter")
		return Token{Type: token.ILLEGAL, Pos: pos, Lit: string(l.ch)}
	}
	l.next() // first char

	if l.ch != delimiter {
		// Not a triple delimiter, treat as error
		l.error(l.offset, "expected triple delimiter for multi-line string")
		return Token{Type: token.ILLEGAL, Pos: pos, Lit: l.src[start:l.offset]}
	}
	l.next() // second char

	if l.ch != delimiter {
		// Not a triple delimiter
		l.error(l.offset, "expected triple delimiter for multi-line string")
		return Token{Type: token.ILLEGAL, Pos: pos, Lit: l.src[start:l.offset]}
	}
	l.next() // third char - now we're inside the string

	// Scan until we find the closing triple delimiter
	for {
		if l.ch == eof {
			l.error(l.offset, "unterminated multi-line string literal")
			return Token{Type: token.STRING, Pos: pos, Lit: l.src[start:l.offset]}
		}

		if l.ch == delimiter && l.peek() == delimiter {
			// Potential closing delimiter, check for third
			savedOffset := l.offset
			l.next() // first delimiter
			if l.ch == delimiter {
				l.next() // second delimiter
				if l.ch == delimiter {
					l.next() // third delimiter - end of string
					return Token{Type: token.STRING, Pos: pos, Lit: l.src[start:l.offset]}
				}
				// Not triple, continue
			}
			// Restore and continue if not triple
			l.offset = savedOffset
			l.rdOffset = savedOffset + 1
			l.ch = rune(l.src[savedOffset])
		}

		l.next()
	}
}

// scanOperator scans an operator or delimiter.
func (l *Lexer) scanOperator(pos token.Pos) Token {
	ch := l.ch
	l.next()

	switch ch {
	case eof:
		return Token{Type: token.EOF, Pos: pos}
	case '+':
		return Token{Type: token.ADD, Pos: pos, Lit: "+"}
	case '-':
		return Token{Type: token.SUB, Pos: pos, Lit: "-"}
	case '*':
		return Token{Type: token.MUL, Pos: pos, Lit: "*"}
	case '/':
		return Token{Type: token.QUO, Pos: pos, Lit: "/"}
	case '%':
		return Token{Type: token.REM, Pos: pos, Lit: "%"}
	case '|':
		return Token{Type: token.PIPE, Pos: pos, Lit: "|"}
	case ':':
		return Token{Type: token.COLON, Pos: pos, Lit: ":"}
	case ';':
		return Token{Type: token.SEMI, Pos: pos, Lit: ";"}
	case ',':
		return Token{Type: token.COMMA, Pos: pos, Lit: ","}
	case '(':
		return Token{Type: token.LPAREN, Pos: pos, Lit: "("}
	case ')':
		return Token{Type: token.RPAREN, Pos: pos, Lit: ")"}
	case '[':
		return Token{Type: token.LBRACKET, Pos: pos, Lit: "["}
	case ']':
		return Token{Type: token.RBRACKET, Pos: pos, Lit: "]"}
	case '{':
		return Token{Type: token.LBRACE, Pos: pos, Lit: "{"}
	case '}':
		return Token{Type: token.RBRACE, Pos: pos, Lit: "}"}
	case '.':
		if l.ch == '.' {
			l.next()
			return Token{Type: token.DOTDOT, Pos: pos, Lit: ".."}
		}
		return Token{Type: token.DOT, Pos: pos, Lit: "."}
	case '=':
		switch l.ch {
		case '=':
			l.next()
			return Token{Type: token.EQL, Pos: pos, Lit: "=="}
		case '~':
			l.next()
			return Token{Type: token.TILDE, Pos: pos, Lit: "=~"}
		case '>':
			l.next()
			return Token{Type: token.ARROW, Pos: pos, Lit: "=>"}
		}
		return Token{Type: token.ASSIGN, Pos: pos, Lit: "="}
	case '!':
		switch l.ch {
		case '=':
			l.next()
			return Token{Type: token.NEQ, Pos: pos, Lit: "!="}
		case '~':
			l.next()
			return Token{Type: token.NTILDE, Pos: pos, Lit: "!~"}
		}
		// Check for negated operators: !has, !contains, !between, !in, etc.
		if isLetter(l.ch) {
			return l.scanNegatedOperator(pos)
		}
		l.error(l.offset-1, "unexpected character '!'")
		return Token{Type: token.ILLEGAL, Pos: pos, Lit: "!"}
	case '<':
		switch l.ch {
		case '=':
			l.next()
			return Token{Type: token.LEQ, Pos: pos, Lit: "<="}
		case '>':
			l.next()
			return Token{Type: token.NEQ, Pos: pos, Lit: "<>"}
		}
		return Token{Type: token.LSS, Pos: pos, Lit: "<"}
	case '>':
		if l.ch == '=' {
			l.next()
			return Token{Type: token.GEQ, Pos: pos, Lit: ">="}
		}
		return Token{Type: token.GTR, Pos: pos, Lit: ">"}
	default:
		l.error(l.offset-1, fmt.Sprintf("unexpected character %q", ch))
		return Token{Type: token.ILLEGAL, Pos: pos, Lit: string(ch)}
	}
}

// scanNegatedOperator scans a negated operator like !has, !contains, !between, etc.
// The '!' has already been consumed.
func (l *Lexer) scanNegatedOperator(pos token.Pos) Token {
	start := l.offset
	for isLetter(l.ch) || l.ch == '_' {
		l.next()
	}
	keyword := l.src[start:l.offset]

	// Check for case-sensitive variants with _cs suffix
	if l.ch == '_' {
		savedOffset := l.offset
		savedRdOffset := l.rdOffset
		savedCh := l.ch
		l.next() // consume _
		suffixStart := l.offset
		for isLetter(l.ch) {
			l.next()
		}
		suffix := l.src[suffixStart:l.offset]
		if suffix == "cs" {
			keyword += "_cs"
		} else {
			// Not a _cs suffix, restore state
			l.offset = savedOffset
			l.rdOffset = savedRdOffset
			l.ch = savedCh
		}
	}

	// Check for case-insensitive suffix ~ (e.g., !in~)
	if l.ch == '~' {
		keyword += "~"
		l.next()
	}

	// Map keyword to negated token
	fullLit := "!" + keyword
	tok := negatedOperatorLookup(keyword)
	if tok != token.ILLEGAL {
		return Token{Type: tok, Pos: pos, Lit: fullLit}
	}

	// Unknown negated operator
	l.error(int(pos), fmt.Sprintf("unknown negated operator '!%s'", keyword))
	return Token{Type: token.ILLEGAL, Pos: pos, Lit: fullLit}
}

// negatedOperatorLookup maps a keyword (without !) to its negated token.
func negatedOperatorLookup(keyword string) token.Token {
	switch keyword {
	case "has":
		return token.NOTHAS
	case "has_cs":
		return token.NOTHASCS
	case "hasprefix":
		return token.NOTHASPREFIX
	case "hasprefix_cs":
		return token.NOTHASPREFIXCS
	case "hassuffix":
		return token.NOTHASSUFFIX
	case "hassuffix_cs":
		return token.NOTHASSUFFIXCS
	case "contains":
		return token.NOTCONTAINS
	case "contains_cs":
		return token.NOTCONTAINSCS
	case "startswith":
		return token.NOTSTARTSWITH
	case "startswith_cs":
		return token.NOTSTARTSWITCS
	case "endswith":
		return token.NOTENDSWITH
	case "endswith_cs":
		return token.NOTENDSWITHCS
	case "between":
		return token.NOTBETWEEN
	case "in":
		return token.NOTIN
	case "in~":
		return token.NOTINCI
	default:
		return token.ILLEGAL
	}
}

// Helper functions

func isLetter(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_' || ch == '$'
}

func isDigit(ch rune) bool {
	return '0' <= ch && ch <= '9'
}

func isHexDigit(ch rune) bool {
	return isDigit(ch) || ('a' <= ch && ch <= 'f') || ('A' <= ch && ch <= 'F')
}
