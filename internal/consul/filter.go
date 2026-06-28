/*
 Copyright © 2026 Alexey Shulutkov <github@shulutkov.ru>

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 	http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package consul

import (
	"fmt"
	"strings"

	"github.com/ks-tool/yellow-pages/internal/model"
)

// filterExpr evaluates a parsed ?filter against a service entry.
type filterExpr func(view) bool

// view is the subset of an entry the minimal ?filter can match on.
type view struct {
	tags     []string
	meta     map[string]string
	statuses []string // statuses of the entry's synthetic checks
}

func viewOf(e model.ServiceEntry) view {
	statuses := make([]string, 0, len(synthChecks(e)))
	for _, c := range synthChecks(e) {
		statuses = append(statuses, c.Status)
	}
	return view{tags: e.Service.Tags, meta: e.Service.Meta, statuses: statuses}
}

// parseFilter parses the minimal Consul ?filter subset: selectors ServiceTags,
// ServiceMeta.<key>, Checks.Status with operators ==/!=/in/contains and the
// boolean connectives and/or/not (and parentheses).
func parseFilter(s string) (filterExpr, error) {
	p := &filterParser{tokens: tokenizeFilter(s)}
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.tokens) {
		return nil, fmt.Errorf("filter: unexpected %q", p.tokens[p.pos])
	}
	return expr, nil
}

type filterParser struct {
	tokens []string
	pos    int
}

func (p *filterParser) peek() string {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return ""
}

func (p *filterParser) next() string {
	t := p.peek()
	p.pos++
	return t
}

func (p *filterParser) parseOr() (filterExpr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for strings.EqualFold(p.peek(), "or") {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		l, r := left, right
		left = func(v view) bool { return l(v) || r(v) }
	}
	return left, nil
}

func (p *filterParser) parseAnd() (filterExpr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for strings.EqualFold(p.peek(), "and") {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		l, r := left, right
		left = func(v view) bool { return l(v) && r(v) }
	}
	return left, nil
}

func (p *filterParser) parseNot() (filterExpr, error) {
	if strings.EqualFold(p.peek(), "not") {
		p.next()
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return func(v view) bool { return !inner(v) }, nil
	}
	return p.parsePrimary()
}

func (p *filterParser) parsePrimary() (filterExpr, error) {
	if p.peek() == "(" {
		p.next()
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if p.next() != ")" {
			return nil, fmt.Errorf("filter: missing )")
		}
		return inner, nil
	}
	return p.parsePredicate()
}

// parsePredicate handles both `selector op value` and `value in selector`.
func (p *filterParser) parsePredicate() (filterExpr, error) {
	left := p.next()
	op := p.next()
	right := p.next()
	if left == "" || op == "" {
		return nil, fmt.Errorf("filter: incomplete predicate")
	}

	switch strings.ToLower(op) {
	case "==":
		sel := left
		val := unquote(right)
		return func(v view) bool { return scalarEq(v, sel, val) }, nil
	case "!=":
		sel := left
		val := unquote(right)
		return func(v view) bool { return !scalarEq(v, sel, val) }, nil
	case "contains":
		sel := left
		val := unquote(right)
		return func(v view) bool { return listHas(v, sel, val) }, nil
	case "in":
		val := unquote(left)
		sel := right
		return func(v view) bool { return listHas(v, sel, val) }, nil
	default:
		return nil, fmt.Errorf("filter: unsupported operator %q", op)
	}
}

func scalarEq(v view, selector, val string) bool {
	switch {
	case strings.EqualFold(selector, "Checks.Status"):
		return contains(v.statuses, val)
	case hasPrefixFold(selector, "ServiceMeta."):
		return v.meta[selector[len("ServiceMeta."):]] == val
	default:
		return false
	}
}

func listHas(v view, selector, val string) bool {
	switch {
	case strings.EqualFold(selector, "ServiceTags"):
		return contains(v.tags, val)
	case strings.EqualFold(selector, "Checks.Status"):
		return contains(v.statuses, val)
	default:
		return false
	}
}

func tokenizeFilter(s string) []string {
	var tokens []string
	i := 0
	for i < len(s) {
		switch c := s[i]; c {
		case ' ', '\t':
			i++
		case '(', ')':
			tokens = append(tokens, string(c))
			i++
		case '"':
			j := i + 1
			for j < len(s) && s[j] != '"' {
				j++
			}
			tokens = append(tokens, s[i:min(j+1, len(s))])
			i = j + 1
		case '=', '!':
			if i+1 < len(s) && s[i+1] == '=' {
				tokens = append(tokens, s[i:i+2])
				i += 2
			} else {
				tokens = append(tokens, string(c))
				i++
			}
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" \t()\"", rune(s[j])) {
				j++
			}
			tokens = append(tokens, s[i:j])
			i = j
		}
	}
	return tokens
}

func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}
