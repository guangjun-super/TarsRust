package generator

import (
	"fmt"
	"strings"
	"unicode"
)

// Split both underscore names and acronym boundaries (HTTPServer -> http_server).
func words(name string) []string {
	var parts []string
	var word []rune
	runes := []rune(name)
	for i, r := range runes {
		if r == '_' || r == '.' {
			if len(word) > 0 {
				parts = append(parts, string(word))
				word = nil
			}
			continue
		}
		if len(word) > 0 && unicode.IsUpper(r) && (unicode.IsLower(runes[i-1]) || unicode.IsDigit(runes[i-1]) || (unicode.IsUpper(runes[i-1]) && i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
			parts = append(parts, string(word))
			word = nil
		}
		word = append(word, r)
	}
	if len(word) > 0 {
		parts = append(parts, string(word))
	}
	return parts
}

func snake(name string) string { return strings.ToLower(strings.Join(words(name), "_")) }

func pascal(name string) string {
	var out strings.Builder
	for _, part := range words(name) {
		part = strings.ToLower(part)
		out.WriteString(strings.ToUpper(part[:1]) + part[1:])
	}
	s := out.String()
	if s == "" {
		s = "_"
	}
	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	return ident(s)
}

func ident(s string) string {
	if s == "" {
		return "__"
	}
	if s[0] >= '0' && s[0] <= '9' {
		s = "_" + s
	}
	switch s {
	case "self", "Self", "super", "crate", "_":
		return s + "_"
	}
	const keywords = " as async await break const continue dyn else enum extern false fn for gen if impl in let loop match mod move mut pub ref return static struct trait true type unsafe use where while abstract become box do final macro override priv typeof unsized virtual yield try "
	if strings.Contains(keywords, " "+s+" ") {
		return "r#" + s
	}
	return s
}

func rustQuote(s string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if unicode.IsControl(r) {
				fmt.Fprintf(&out, `\u{%x}`, r)
			} else {
				out.WriteRune(r)
			}
		}
	}
	out.WriteByte('"')
	return out.String()
}
