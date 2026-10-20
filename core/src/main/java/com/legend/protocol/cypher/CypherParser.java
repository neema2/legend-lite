// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.cypher;

import com.legend.protocol.cypher.CypherAst.BinaryOp;
import com.legend.protocol.cypher.CypherAst.Clause;
import com.legend.protocol.cypher.CypherAst.Direction;
import com.legend.protocol.cypher.CypherAst.Expr;
import com.legend.protocol.cypher.CypherAst.Item;
import com.legend.protocol.cypher.CypherAst.NodePattern;
import com.legend.protocol.cypher.CypherAst.PathPattern;
import com.legend.protocol.cypher.CypherAst.PatternElement;
import com.legend.protocol.cypher.CypherAst.RelPattern;
import com.legend.protocol.cypher.CypherAst.SortItem;
import com.legend.protocol.cypher.CypherAst.UnaryOp;
import com.legend.protocol.cypher.CypherLexer.Token;
import com.legend.protocol.cypher.CypherLexer.Type;
import com.legend.protocol.sql.SqlAst.Pos;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Cypher's grammar, as Neo4j's manual for the pinned release spells it (docs/CYPHER_STUDY_2026_10_15.md §5.1), read
 * by recursive descent with Pratt precedence for expressions. Every clause keyword is recognised: the ones the
 * translator reads become their records, the others an {@link CypherAst.OtherClause} the translator refuses by name,
 * so that a write or a procedure call is refused as what it is, never as a syntax error. The token cursor {@code at}
 * is the SqlParser's shape, scoped to one parse ({@code CodeShapeGuardrailTest}).
 *
 * <p>Precedence, from loosest: {@code OR}, {@code XOR}, {@code AND}, {@code NOT}, the comparisons (chained:
 * {@code a < b < c} is {@code a < b AND b < c}), the string, list and null predicates ({@code STARTS WITH},
 * {@code ENDS WITH}, {@code CONTAINS}, {@code IN}, {@code =~}, {@code IS [NOT] NULL}), {@code + -}, {@code * / %},
 * unary {@code + -}, {@code ^} (right-associative), then property access and calls.
 */
public final class CypherParser {

    /** The keywords that open a clause; a {@code WITH}, {@code RETURN} or {@code MATCH} item list ends at one. */
    private static final Set<String> CLAUSE_WORDS = Set.of("MATCH", "OPTIONAL", "WITH", "RETURN", "USE", "UNWIND",
            "CREATE", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "CALL", "UNION", "FOREACH", "LOAD", "FILTER",
            "LET", "NEXT", "FINISH", "SHOW", "INSERT", "WHERE", "ORDER", "SKIP", "LIMIT", "OFFSET");

    /** The clause keywords the translator does not read yet, kept as {@link CypherAst.OtherClause}. */
    private static final Set<String> OTHER_CLAUSE_WORDS = Set.of("UNWIND", "CREATE", "MERGE", "SET", "DELETE",
            "DETACH", "REMOVE", "CALL", "UNION", "FOREACH", "LOAD", "FILTER", "LET", "NEXT", "FINISH", "SHOW",
            "INSERT");

    /** The functions whose argument is a variable over a list ({@code all(x IN list WHERE ...)}), refused by name
     *  until lists land. */
    private static final Set<String> QUANTIFIERS = Set.of("all", "any", "none", "single");

    private static final Set<String> LIST_PREDICATES = Set.of("all", "any", "none", "single", "filter", "extract",
            "reduce");

    /** The clauses that update the graph: none may stand inside an EXISTS subquery (Neo4j's InvalidClauseComposition). */
    private static final Set<String> UPDATES = Set.of("CREATE", "MERGE", "SET", "REMOVE", "DELETE", "DETACH", "FOREACH");

    private static final Set<String> RESERVED = Set.of("AND", "OR", "XOR", "NOT", "IS", "NULL", "TRUE", "FALSE",
            "IN", "STARTS", "ENDS", "CONTAINS", "AS", "ASC", "ASCENDING", "DESC", "DESCENDING", "DISTINCT", "CASE",
            "WHEN", "THEN", "ELSE", "CYPHER");

    private final String src;
    private final List<Token> tokens;
    private int at;

    private CypherParser(String src) {
        this.src = src;
        this.tokens = CypherLexer.tokenize(src);
    }

    /** The statement {@code text} is. */
    public static CypherAst.Statement parse(String text) {
        CypherParser p = new CypherParser(text);
        CypherAst.Statement s = p.statement();
        if (!p.peek().is(Type.EOF)) {
            throw p.error("unexpected '" + p.peek().text() + "'");
        }
        return s;
    }

    // ------------------------------------------------------------------ tokens

    private Token peek() {
        return tokens.get(at);
    }

    private Token peek(int ahead) {
        int i = Math.min(at + ahead, tokens.size() - 1);
        return tokens.get(i);
    }

    private Token advance() {
        Token t = tokens.get(at);
        if (!t.is(Type.EOF)) {
            at++;
        }
        return t;
    }

    private boolean atWord(String word) {
        return peek().isWord(word);
    }

    private boolean matchWord(String word) {
        if (atWord(word)) {
            advance();
            return true;
        }
        return false;
    }

    private Token expectWord(String word) {
        if (!atWord(word)) {
            throw error("expected " + word + " but found '" + peek().text() + "'");
        }
        return advance();
    }

    private boolean match(Type t) {
        if (peek().is(t)) {
            advance();
            return true;
        }
        return false;
    }

    private Token expect(Type t, String what) {
        if (!peek().is(t)) {
            throw error("expected " + what + " but found '" + (peek().is(Type.EOF) ? "end of input"
                    : peek().text()) + "'");
        }
        return advance();
    }

    private RuntimeException error(String message) {
        return CypherErrors.syntax(message, peek().pos());
    }

    /** An identifier (a name, backticked or not), as written. */
    private String identifier(String what) {
        Token t = peek();
        if (!t.is(Type.IDENT) || (!t.backticked() && RESERVED.contains(t.text().toUpperCase(java.util.Locale.ROOT)))) {
            throw error("expected " + what + " but found '" + t.text() + "'");
        }
        advance();
        return t.text();
    }

    private boolean atClauseWord() {
        Token t = peek();
        return t.is(Type.IDENT) && !t.backticked()
                && CLAUSE_WORDS.contains(t.text().toUpperCase(java.util.Locale.ROOT));
    }

    // ------------------------------------------------------------------ statement and clauses

    private CypherAst.Statement statement() {
        Pos pos = peek().pos();
        Integer version = null;
        if (atWord("CYPHER")) {
            Pos p = advance().pos();
            if (peek().is(Type.INTEGER)) {
                version = Integer.parseInt(advance().text());
                if (version != 5 && version != 25) {
                    throw CypherErrors.invalid("Cypher version " + version + " is not supported: 5 or 25", p);
                }
            }
            // CYPHER runtime=parallel and the like: Neo4j's own options, which the island does not read
            if (peek().is(Type.IDENT) && peek(1).is(Type.EQ)) {
                throw CypherErrors.unsupported("a CYPHER option (" + peek().text() + "=...) is not supported", p);
            }
        }
        List<Clause> clauses = new ArrayList<>();
        while (!peek().is(Type.EOF)) {
            if (match(Type.SEMI)) {
                if (!peek().is(Type.EOF)) {
                    throw error("one statement per island: unexpected '" + peek().text() + "' after ';'");
                }
                break;
            }
            clauses.add(clause());
        }
        if (clauses.isEmpty()) {
            throw CypherErrors.syntax("an empty query", pos);
        }
        return new CypherAst.Statement(version, clauses, pos);
    }

    private Clause clause() {
        Token t = peek();
        Pos pos = t.pos();
        if (atWord("USE")) {
            advance();
            return new CypherAst.Use(qualifiedPath(), pos);
        }
        if (atWord("OPTIONAL")) {
            advance();
            expectWord("MATCH");
            return match(true, pos);
        }
        if (atWord("MATCH")) {
            advance();
            return match(false, pos);
        }
        if (atWord("WITH")) {
            advance();
            return with(pos);
        }
        if (atWord("RETURN")) {
            advance();
            return returnClause(pos);
        }
        if (atWord("UNWIND")) {
            advance();
            Expr list = expression();
            expectWord("AS");
            return new CypherAst.Unwind(list, identifier("a variable"), pos);
        }
        if (atWord("CALL")) {
            advance();
            if (peek().is(Type.LBRACE)) {
                throw CypherErrors.unsupported("a CALL subquery (CALL { ... }) is not supported yet", pos);
            }
            return procedureCall(pos);
        }
        if (t.is(Type.IDENT) && !t.backticked()
                && OTHER_CLAUSE_WORDS.contains(t.text().toUpperCase(java.util.Locale.ROOT))) {
            return otherClause(t);
        }
        throw error("expected a clause but found '" + t.text() + "'");
    }

    /** {@code CALL name[(args)] [YIELD * | item [AS alias], ...]}: the procedure's dotted name, its arguments when
     *  written in parentheses, its yielded outputs. */
    private Clause procedureCall(Pos pos) {
        StringBuilder name = new StringBuilder(identifier("a procedure name"));
        while (match(Type.DOT)) {
            name.append('.').append(identifier("a procedure name"));
        }
        List<Expr> args = null;
        if (match(Type.LPAREN)) {
            args = new ArrayList<>();
            if (!peek().is(Type.RPAREN)) {
                do {
                    args.add(expression());
                } while (match(Type.COMMA));
            }
            expect(Type.RPAREN, "')'");
        }
        List<CypherAst.YieldItem> yields = null;
        boolean all = false;
        if (matchWord("YIELD")) {
            if (match(Type.STAR)) {
                all = true;
            } else {
                yields = new ArrayList<>();
                do {
                    Pos at = peek().pos();
                    String output = identifier("a procedure output");
                    String alias = matchWord("AS") ? identifier("an alias") : null;
                    yields.add(new CypherAst.YieldItem(output, alias, at));
                } while (match(Type.COMMA));
            }
        }
        return new CypherAst.ProcedureCall(name.toString(), args, yields, all, pos);
    }

    /** A path the query names as its graph: {@code demo::MovieSpace} or a backticked text. */
    private String qualifiedPath() {
        Token t = peek();
        if (t.is(Type.IDENT) && t.backticked()) {
            advance();
            return t.text();
        }
        StringBuilder sb = new StringBuilder(identifier("a graph name"));
        while (peek().is(Type.COLON) && peek(1).is(Type.COLON)) {
            advance();
            advance();
            sb.append("::").append(identifier("a graph name"));
        }
        return sb.toString();
    }

    /** A clause the translator does not read: its keyword and its text up to the next clause at bracket depth 0. */
    private Clause otherClause(Token keyword) {
        String word = keyword.text().toUpperCase(java.util.Locale.ROOT);
        int start = keyword.start();
        advance();
        if (word.equals("DETACH")) {
            matchWord("DELETE");
            word = "DETACH DELETE";
        } else if (word.equals("LOAD")) {
            matchWord("CSV");
            word = "LOAD CSV";
        } else if (word.equals("UNION") && matchWord("ALL")) {
            word = "UNION ALL";
        }
        int depth = 0;
        int end = keyword.end();
        Token before = keyword;
        Token beforeThat = keyword;
        while (!peek().is(Type.EOF)) {
            Token t = peek();
            // MERGE's ON MATCH SET and ON CREATE SET actions are the MERGE's, not new clauses
            boolean action = before.isWord("ON") && (t.isWord("MATCH") || t.isWord("CREATE"))
                    || t.isWord("SET") && beforeThat.isWord("ON") && (before.isWord("MATCH") || before.isWord("CREATE"));
            if (depth == 0 && atClauseWord() && !action && !t.isWord("WHERE") && !t.isWord("ORDER")
                    && !t.isWord("SKIP") && !t.isWord("LIMIT") && !t.isWord("OFFSET")) {
                break;
            }
            beforeThat = before;
            before = t;
            if (t.is(Type.LPAREN) || t.is(Type.LBRACKET) || t.is(Type.LBRACE)) {
                depth++;
            } else if (t.is(Type.RPAREN) || t.is(Type.RBRACKET) || t.is(Type.RBRACE)) {
                depth--;
            }
            end = t.end();
            advance();
        }
        return new CypherAst.OtherClause(word, src.substring(start, end), keyword.pos());
    }

    private CypherAst.Match match(boolean optional, Pos pos) {
        List<PathPattern> patterns = new ArrayList<>();
        patterns.add(pathPattern());
        while (match(Type.COMMA)) {
            patterns.add(pathPattern());
        }
        Expr where = null;
        if (matchWord("WHERE")) {
            where = expression();
        }
        return new CypherAst.Match(optional, patterns, where, pos);
    }

    private CypherAst.With with(Pos pos) {
        boolean distinct = matchWord("DISTINCT");
        List<Item> items = items();
        List<SortItem> orderBy = orderBy();
        Expr skip = matchWord("SKIP") || matchWord("OFFSET") ? expression() : null;
        Expr limit = matchWord("LIMIT") ? expression() : null;
        Expr where = matchWord("WHERE") ? expression() : null;
        return new CypherAst.With(distinct, items, orderBy, skip, limit, where, pos);
    }

    private CypherAst.Return returnClause(Pos pos) {
        boolean distinct = matchWord("DISTINCT");
        List<Item> items = items();
        List<SortItem> orderBy = orderBy();
        Expr skip = matchWord("SKIP") || matchWord("OFFSET") ? expression() : null;
        Expr limit = matchWord("LIMIT") ? expression() : null;
        return new CypherAst.Return(distinct, items, orderBy, skip, limit, pos);
    }

    private List<Item> items() {
        List<Item> items = new ArrayList<>();
        do {
            Token start = peek();
            if (start.is(Type.STAR)) {
                advance();
                items.add(new Item(null, null, "*", start.pos()));
                continue;
            }
            Expr e = expression();
            int end = tokens.get(at - 1).end();
            String alias = null;
            if (matchWord("AS")) {
                alias = identifier("an alias");
            }
            items.add(new Item(e, alias, src.substring(start.start(), end).trim(), start.pos()));
        } while (match(Type.COMMA));
        return items;
    }

    private List<SortItem> orderBy() {
        List<SortItem> out = new ArrayList<>();
        if (!matchWord("ORDER")) {
            return out;
        }
        expectWord("BY");
        do {
            Pos pos = peek().pos();
            Expr e = expression();
            boolean descending = false;
            if (matchWord("DESC") || matchWord("DESCENDING")) {
                descending = true;
            } else if (matchWord("ASC") || matchWord("ASCENDING")) {
                descending = false;
            }
            out.add(new SortItem(e, descending, pos));
        } while (match(Type.COMMA));
        return out;
    }

    // ------------------------------------------------------------------ patterns

    private PathPattern pathPattern() {
        Pos pos = peek().pos();
        String variable = null;
        if (peek().is(Type.IDENT) && peek(1).is(Type.EQ)) {
            variable = identifier("a path variable");
            advance();
        }
        if (atWord("SHORTEST") || atWord("ANY") || atWord("ALL") || atWord("shortestPath")
                || atWord("allShortestPaths")) {
            throw CypherErrors.unsupported("a path selector (" + peek().text() + ") is not supported yet", pos);
        }
        List<PatternElement> elements = new ArrayList<>();
        elements.add(nodePattern());
        while (peek().is(Type.MINUS) || peek().is(Type.ARROW_LEFT)) {
            elements.add(relPattern());
            elements.add(nodePattern());
        }
        return new PathPattern(variable, elements, pos);
    }

    private NodePattern nodePattern() {
        Pos pos = peek().pos();
        expect(Type.LPAREN, "'('");
        String variable = null;
        if (peek().is(Type.IDENT) && !peek().isWord("WHERE")) {
            variable = identifier("a node variable");
        }
        List<String> labels = new ArrayList<>();
        while (match(Type.COLON)) {
            labels.add(labelName());
            if (peek().is(Type.PIPE) || (peek().is(Type.IDENT) && peek().text().equals("&"))) {
                throw CypherErrors.unsupported("a label expression (|, &, !, %) is not supported yet", pos);
            }
        }
        if (peek().is(Type.PERCENT)) {
            throw CypherErrors.unsupported("a label expression (|, &, !, %) is not supported yet", pos);
        }
        Map<String, Expr> properties = peek().is(Type.LBRACE) ? mapLiteral().entries() : Map.of();
        if (atWord("WHERE")) {
            throw CypherErrors.unsupported("a WHERE inside a node pattern is not supported yet", pos);
        }
        expect(Type.RPAREN, "')'");
        return new NodePattern(variable, labels, properties, pos);
    }

    private String labelName() {
        Token t = peek();
        if (t.is(Type.IDENT)) {
            advance();
            return t.text();
        }
        if (t.is(Type.LPAREN) || (t.is(Type.PARAM))) {
            throw CypherErrors.unsupported("a dynamic label or type is not supported: the model is the schema",
                    t.pos());
        }
        throw error("expected a label but found '" + t.text() + "'");
    }

    private RelPattern relPattern() {
        Pos pos = peek().pos();
        boolean left = match(Type.ARROW_LEFT);
        if (!left) {
            expect(Type.MINUS, "'-' or '<-'");
        }
        String variable = null;
        List<String> types = new ArrayList<>();
        Map<String, Expr> properties = Map.of();
        CypherAst.LengthRange range = null;
        if (match(Type.LBRACKET)) {
            if (peek().is(Type.IDENT)) {
                variable = identifier("a relationship variable");
            }
            if (match(Type.COLON)) {
                types.add(labelName());
                while (match(Type.PIPE)) {
                    match(Type.COLON);
                    types.add(labelName());
                }
            }
            if (match(Type.STAR)) {
                int lower = -1;
                int upper = -1;
                if (peek().is(Type.INTEGER)) {
                    lower = Integer.parseInt(advance().text());
                    upper = lower;
                }
                if (match(Type.DOTDOT)) {
                    upper = peek().is(Type.INTEGER) ? Integer.parseInt(advance().text()) : -1;
                } else if (lower >= 0) {
                    upper = lower;
                }
                range = new CypherAst.LengthRange(lower, upper);
            }
            if (peek().is(Type.LBRACE)) {
                properties = mapLiteral().entries();
            }
            if (atWord("WHERE")) {
                throw CypherErrors.unsupported("a WHERE inside a relationship pattern is not supported yet", pos);
            }
            expect(Type.RBRACKET, "']'");
        }
        boolean right = match(Type.ARROW_RIGHT);
        if (!right) {
            expect(Type.MINUS, "'-' or '->'");
        }
        if (peek().is(Type.LBRACE)) {
            throw CypherErrors.unsupported("a quantifier after a relationship pattern is not supported yet", pos);
        }
        // <-[]-> points both ways: Neo4j reads it as undirected
        Direction d = left && !right ? Direction.IN : right && !left ? Direction.OUT : Direction.BOTH;
        return new RelPattern(variable, types, d, properties, range, pos);
    }

    // ------------------------------------------------------------------ expressions

    private Expr expression() {
        return or();
    }

    private Expr or() {
        Expr e = xor();
        while (atWord("OR")) {
            Pos pos = advance().pos();
            e = new CypherAst.Binary(BinaryOp.OR, e, xor(), pos);
        }
        return e;
    }

    private Expr xor() {
        Expr e = and();
        while (atWord("XOR")) {
            Pos pos = advance().pos();
            e = new CypherAst.Binary(BinaryOp.XOR, e, and(), pos);
        }
        return e;
    }

    private Expr and() {
        Expr e = not();
        while (atWord("AND")) {
            Pos pos = advance().pos();
            e = new CypherAst.Binary(BinaryOp.AND, e, not(), pos);
        }
        return e;
    }

    private Expr not() {
        if (atWord("NOT")) {
            Pos pos = advance().pos();
            return new CypherAst.Unary(UnaryOp.NOT, not(), pos);
        }
        return comparison();
    }

    private Expr comparison() {
        Expr left = predicate();
        Expr chain = null;
        while (true) {
            BinaryOp op = comparisonOp();
            boolean negatedRight = false;
            if (op == null && peek().is(Type.ARROW_LEFT)) {
                // 1<-1 in an expression: < then a negated operand (the lexer reads <- as an arrow everywhere)
                op = BinaryOp.LT;
                negatedRight = true;
            }
            if (op == null) {
                return chain == null ? left : chain;
            }
            Pos pos = advance().pos();
            Expr right = negatedRight ? new CypherAst.Unary(UnaryOp.NEG, predicate(), pos) : predicate();
            Expr c = new CypherAst.Binary(op, left, right, pos);
            // a < b < c is a < b AND b < c: Neo4j chains comparisons
            chain = chain == null ? c : new CypherAst.Binary(BinaryOp.AND, chain, c, pos);
            left = right;
        }
    }

    private CypherAst.@com.legend.base.Nullable BinaryOp comparisonOp() {
        return switch (peek().type()) {
            case EQ -> BinaryOp.EQ;
            case NE -> BinaryOp.NE;
            case LT -> BinaryOp.LT;
            case LE -> BinaryOp.LE;
            case GT -> BinaryOp.GT;
            case GE -> BinaryOp.GE;
            default -> null;
        };
    }

    /** The string, list and null predicates: tighter than a comparison, looser than arithmetic. */
    private Expr predicate() {
        Expr e = additive();
        while (true) {
            Pos pos = peek().pos();
            if (atWord("STARTS") && peek(1).isWord("WITH")) {
                advance();
                advance();
                e = new CypherAst.Binary(BinaryOp.STARTS_WITH, e, additive(), pos);
            } else if (atWord("ENDS") && peek(1).isWord("WITH")) {
                advance();
                advance();
                e = new CypherAst.Binary(BinaryOp.ENDS_WITH, e, additive(), pos);
            } else if (atWord("CONTAINS")) {
                advance();
                e = new CypherAst.Binary(BinaryOp.CONTAINS, e, additive(), pos);
            } else if (atWord("IN")) {
                advance();
                e = new CypherAst.Binary(BinaryOp.IN, e, additive(), pos);
            } else if (peek().is(Type.REGEX_EQ)) {
                advance();
                e = new CypherAst.Binary(BinaryOp.REGEX, e, additive(), pos);
            } else if (atWord("IS")) {
                advance();
                boolean not = matchWord("NOT");
                if (matchWord("NULL")) {
                    e = new CypherAst.Unary(not ? UnaryOp.IS_NOT_NULL : UnaryOp.IS_NULL, e, pos);
                } else {
                    throw CypherErrors.unsupported("IS " + (not ? "NOT " : "") + peek().text()
                            + " (a type predicate or IS NORMALIZED) is not supported yet", pos);
                }
            } else {
                return e;
            }
        }
    }

    private Expr additive() {
        Expr e = multiplicative();
        while (peek().is(Type.PLUS) || peek().is(Type.MINUS)) {
            Token t = advance();
            e = new CypherAst.Binary(t.is(Type.PLUS) ? BinaryOp.PLUS : BinaryOp.MINUS, e, multiplicative(), t.pos());
        }
        return e;
    }

    private Expr multiplicative() {
        Expr e = power();
        while (peek().is(Type.STAR) || peek().is(Type.SLASH) || peek().is(Type.PERCENT)) {
            Token t = advance();
            BinaryOp op = t.is(Type.STAR) ? BinaryOp.TIMES : t.is(Type.SLASH) ? BinaryOp.DIVIDE : BinaryOp.MODULO;
            e = new CypherAst.Binary(op, e, power(), t.pos());
        }
        return e;
    }

    /** {@code ^}: tighter than {@code * / %}, looser than unary minus, LEFT-associative, as Neo4j reads it (the
     *  TCK's Precedence2: {@code -3 ^ 2} is 9, {@code 4 ^ (3 / 2) ^ 3} is 64; verified on Neo4j 2026.09.0). */
    private Expr power() {
        Expr e = unary();
        while (peek().is(Type.CARET)) {
            Pos pos = advance().pos();
            e = new CypherAst.Binary(BinaryOp.POWER, e, unary(), pos);
        }
        return e;
    }

    private Expr unary() {
        if (peek().is(Type.MINUS)) {
            Pos pos = advance().pos();
            if (peek().is(Type.INTEGER) && !peek(1).is(Type.DOT) && !peek(1).is(Type.CARET)) {
                // the sign folded into the literal, as Neo4j reads -9223372036854775808 (the smallest integer)
                Token t = advance();
                return new CypherAst.IntLit(parseInteger(t, true), "-" + t.text(), pos);
            }
            return new CypherAst.Unary(UnaryOp.NEG, unary(), pos);
        }
        if (peek().is(Type.PLUS)) {
            Pos pos = advance().pos();
            return new CypherAst.Unary(UnaryOp.POS, unary(), pos);
        }
        return postfix();
    }

    private Expr postfix() {
        Expr e = atom();
        while (true) {
            if (peek().is(Type.DOT)) {
                Pos pos = advance().pos();
                e = new CypherAst.Prop(e, identifier("a property name"), pos);
            } else if (peek().is(Type.LBRACKET)) {
                Pos pos = advance().pos();
                if (match(Type.DOTDOT)) {
                    // [..to]
                    Expr to = peek().is(Type.RBRACKET) ? null : expression();
                    expect(Type.RBRACKET, "']'");
                    e = new CypherAst.Slice(e, null, to, pos);
                    continue;
                }
                Expr first = expression();
                if (match(Type.DOTDOT)) {
                    Expr to = peek().is(Type.RBRACKET) ? null : expression();
                    expect(Type.RBRACKET, "']'");
                    e = new CypherAst.Slice(e, first, to, pos);
                    continue;
                }
                expect(Type.RBRACKET, "']'");
                e = new CypherAst.Index(e, first, pos);
            } else if (peek().is(Type.COLON) && peek(1).is(Type.IDENT)) {
                // n:Label:Other, a label predicate
                Pos at = peek().pos();
                List<String> labels = new ArrayList<>();
                while (peek().is(Type.COLON) && peek(1).is(Type.IDENT)) {
                    advance();
                    labels.add(advance().text());
                }
                e = new CypherAst.LabelPredicate(e, labels, at);
            } else {
                return e;
            }
        }
    }

    /** Whether a node pattern closes at {@code k} and a relationship pattern follows ({@code )-[}, {@code )-(},
     *  {@code )--}, {@code )-->}, {@code )<-}): what tells a pattern inside brackets from a parenthesized
     *  expression ({@code [(a) - 1]}). */
    /** At {@code n:A:B ...} with its first colon {@code k} tokens ahead: whether the labelled node starts a pattern
     *  -- a property map, or a relationship after its `)` -- rather than a parenthesized label predicate,
     *  {@code (n:Foo)} (Return2 [8]), in a list too. */
    private boolean labelledNodeStartsPattern(int k) {
        while (peek(k).is(Type.COLON) && peek(k + 1).is(Type.IDENT)) {
            k += 2;
        }
        return peek(k).is(Type.LBRACE) || relationshipAt(k);
    }

    /** Whether a relationship follows the node closing at {@code k}: {@code )<-}, {@code )-[}, {@code )--},
     *  {@code )-->} ({@code )-(} is arithmetic over a parenthesized value, never a relationship). */
    private boolean relationshipAt(int k) {
        return peek(k).is(Type.RPAREN) && (peek(k + 1).is(Type.ARROW_LEFT) || peek(k + 1).is(Type.MINUS)
                && (peek(k + 2).is(Type.LBRACKET) || peek(k + 2).is(Type.MINUS) || peek(k + 2).is(Type.ARROW_RIGHT)));
    }

    /** Whether a node pattern opens at {@code k} (its {@code (}): a label, a property map or a relationship follows
     *  its variable, or it is empty and a relationship follows. */
    private boolean nodeStartsPattern(int k) {
        if (peek(k + 1).is(Type.RPAREN)) {
            return relationshipAt(k + 1);
        }
        if (peek(k + 1).is(Type.COLON)) {
            return labelledNodeStartsPattern(k + 1);
        }
        if (peek(k + 1).is(Type.LBRACE)) {
            return mapNodeStartsPattern(k + 1);
        }
        return peek(k + 1).is(Type.IDENT) && (peek(k + 2).is(Type.COLON) && labelledNodeStartsPattern(k + 2)
                || peek(k + 2).is(Type.LBRACE) && mapNodeStartsPattern(k + 2) || relationshipAt(k + 2));
    }

    /** Whether the property map opening at {@code k} closes a node pattern that a relationship follows (a
     *  parenthesized map literal, {@code ({a: 1})}, is a value). */
    private boolean mapNodeStartsPattern(int k) {
        int depth = 0;
        int i = k;
        while (!peek(i).is(Type.EOF)) {
            if (peek(i).is(Type.LBRACE)) {
                depth++;
            } else if (peek(i).is(Type.RBRACE)) {
                depth--;
                if (depth == 0) {
                    return relationshipAt(i + 1);
                }
            }
            i++;
        }
        return false;
    }

    private Expr atom() {
        Token t = peek();
        Pos pos = t.pos();
        switch (t.type()) {
            case INTEGER -> {
                advance();
                return new CypherAst.IntLit(parseInteger(t), t.text(), pos);
            }
            case FLOAT -> {
                advance();
                double value = Double.parseDouble(t.text());
                if (Double.isInfinite(value)) {
                    // (Neo4j's FloatingPointOverflow, the study's §12.84)
                    throw CypherErrors.syntax("floating point number is too large", pos);
                }
                return new CypherAst.FloatLit(value, t.text(), pos);
            }
            case STRING -> {
                advance();
                return new CypherAst.StringLit(t.text(), pos);
            }
            case PARAM -> {
                advance();
                return new CypherAst.Param(t.text(), pos);
            }
            case LPAREN -> {
                if (nodeStartsPattern(0)) {
                    // a pattern as a predicate: (n)-[:T]->(), (n)<-[]-(), ()-->(n), (:A)-->(n), ({k: 1})-->(n)
                    return new CypherAst.PatternPredicate(pathPattern(), pos);
                }
                advance();
                Expr inner = expression();
                expect(Type.RPAREN, "')'");
                return new CypherAst.Paren(inner, pos);
            }
            case LBRACKET -> {
                advance();
                if (peek().is(Type.LPAREN) && nodeStartsPattern(0)
                        || peek().is(Type.IDENT) && peek(1).is(Type.EQ) && peek(2).is(Type.LPAREN)) {
                    // [p = (a)-[:T]->(b) WHERE predicate | expression]: a pattern comprehension
                    PathPattern pattern = pathPattern();
                    Expr where = matchWord("WHERE") ? expression() : null;
                    expect(Type.PIPE, "'|'");
                    Expr map = expression();
                    expect(Type.RBRACKET, "']'");
                    return new CypherAst.PatternComprehension(pattern, where, map, pos);
                }
                if (peek().is(Type.IDENT) && !peek().backticked() && peek(1).isWord("IN")
                        && !RESERVED.contains(peek().text().toUpperCase(java.util.Locale.ROOT))
                        || peek().is(Type.IDENT) && peek().backticked() && peek(1).isWord("IN")) {
                    // [x IN list WHERE predicate | expression]
                    String variable = advance().text();
                    advance();
                    Expr list = expression();
                    Expr where = matchWord("WHERE") ? expression() : null;
                    Expr map = match(Type.PIPE) ? expression() : null;
                    expect(Type.RBRACKET, "']'");
                    return new CypherAst.ListComprehension(variable, list, where, map, pos);
                }
                List<Expr> elements = new ArrayList<>();
                if (!peek().is(Type.RBRACKET)) {
                    do {
                        elements.add(expression());
                    } while (match(Type.COMMA));
                }
                if (peek().is(Type.PIPE) || atWord("IN")) {
                    throw error("expected ']' but found '" + peek().text() + "'");
                }
                expect(Type.RBRACKET, "']'");
                return new CypherAst.ListLit(elements, pos);
            }
            case LBRACE -> {
                return mapLiteral();
            }
            case IDENT -> {
                return word(t);
            }
            default -> throw error("expected an expression but found '" + (t.is(Type.EOF) ? "end of input"
                    : t.text()) + "'");
        }
    }

    private long parseInteger(Token t) {
        return parseInteger(t, false);
    }

    private long parseInteger(Token t, boolean negated) {
        String text = t.text();
        java.math.BigInteger value;
        if (text.length() > 1 && text.charAt(0) == '0' && Character.isDigit(text.charAt(1))) {
            // Neo4j: an integer literal with a leading zero is invalid (the review's finding 2, 2026-10-15)
            throw CypherErrors.syntax("invalid integer literal (a leading zero): " + text, t.pos());
        }
        try {
            if (text.startsWith("0x") || text.startsWith("0X")) {
                value = new java.math.BigInteger(text.substring(2), 16);
            } else if (text.startsWith("0o") || text.startsWith("0O")) {
                value = new java.math.BigInteger(text.substring(2), 8);
            } else {
                value = new java.math.BigInteger(text);
            }
        } catch (NumberFormatException e) {
            throw CypherErrors.syntax("not an integer: " + text, t.pos());
        }
        if (negated) {
            value = value.negate();
        }
        if (value.bitLength() > 63) {
            throw CypherErrors.invalid("integer is too large: " + (negated ? "-" : "") + text, t.pos());
        }
        return value.longValue();
    }

    private CypherAst.MapLit mapLiteral() {
        Pos pos = peek().pos();
        expect(Type.LBRACE, "'{'");
        Map<String, Expr> entries = new LinkedHashMap<>();
        if (!peek().is(Type.RBRACE)) {
            do {
                Token k = peek();
                if (!k.is(Type.IDENT)) {
                    throw error("expected a map key but found '" + k.text() + "'");
                }
                advance();
                expect(Type.COLON, "':'");
                entries.put(k.text(), expression());
            } while (match(Type.COMMA));
        }
        expect(Type.RBRACE, "'}'");
        return new CypherAst.MapLit(entries, pos);
    }

    /** An identifier at the start of an atom: a keyword literal, a function call, or a variable. */
    private Expr word(Token t) {
        Pos pos = t.pos();
        if (!t.backticked()) {
            if (t.isWord("NULL")) {
                advance();
                return new CypherAst.NullLit(pos);
            }
            if (t.isWord("TRUE")) {
                advance();
                return new CypherAst.BoolLit(true, pos);
            }
            if (t.isWord("FALSE")) {
                advance();
                return new CypherAst.BoolLit(false, pos);
            }
            if (t.isWord("CASE")) {
                advance();
                return caseExpression(pos);
            }
            if (t.isWord("EXISTS") && peek(1).is(Type.LBRACE)) {
                // EXISTS { [MATCH] pattern [WHERE predicate] [RETURN items] }: one pattern, the items unread (a
                // row's existence is what counts)
                advance();
                advance();
                if (UPDATES.contains(peek().text().toUpperCase(java.util.Locale.ROOT))) {
                    throw CypherErrors.invalid("An Exists Expression cannot contain any updates", pos);
                }
                if (peek().isWord("OPTIONAL") || peek().isWord("WITH") || peek().isWord("UNWIND")
                        || peek().isWord("CALL")) {
                    throw CypherErrors.unsupported("an EXISTS subquery starting with " + peek().text().toUpperCase(
                            java.util.Locale.ROOT) + " is not supported yet", pos);
                }
                matchWord("MATCH");
                PathPattern pattern = pathPattern();
                if (peek().is(Type.COMMA)) {
                    throw CypherErrors.unsupported("an EXISTS subquery over several patterns is not supported yet",
                            pos);
                }
                Expr where = matchWord("WHERE") ? expression() : null;
                CypherAst.Counted counted = peek().isWord("WITH") ? counted() : null;
                List<Expr> items = new ArrayList<>();
                if (matchWord("RETURN")) {
                    matchWord("DISTINCT");
                    if (!match(Type.STAR)) {
                        items.add(expression());
                        if (matchWord("AS")) {
                            advance();
                        }
                        while (match(Type.COMMA)) {
                            items.add(expression());
                            if (matchWord("AS")) {
                                advance();
                            }
                        }
                    }
                }
                if (UPDATES.contains(peek().text().toUpperCase(java.util.Locale.ROOT))) {
                    // (Neo4j's InvalidClauseComposition, the study's §12.84)
                    throw CypherErrors.invalid("An Exists Expression cannot contain any updates", pos);
                }
                if (!peek().is(Type.RBRACE)) {
                    throw CypherErrors.unsupported("an EXISTS subquery with more than one pattern, a WHERE and a RETURN"
                            + " (a " + peek().text() + " clause) is not supported yet", pos);
                }
                advance();
                return nested(new CypherAst.ExistsSubquery(pattern, where, items, counted, pos));
            }
            if ((t.isWord("COUNT") || t.isWord("COLLECT")) && peek(1).is(Type.LBRACE)) {
                throw CypherErrors.unsupported(t.text().toUpperCase(java.util.Locale.ROOT)
                        + " { ... } subqueries are not supported yet", pos);
            }
            if (RESERVED.contains(t.text().toUpperCase(java.util.Locale.ROOT))) {
                throw error("expected an expression but found '" + t.text() + "'");
            }
        }
        advance();
        // a namespaced function name: duration.between, string.join
        String name = t.text();
        while (!t.backticked() && peek().is(Type.DOT) && peek(1).is(Type.IDENT) && peek(2).is(Type.LPAREN)) {
            advance();
            name = name + "." + advance().text();
        }
        if (peek().is(Type.LPAREN)) {
            advance();
            if (peek().is(Type.STAR) && peek(1).is(Type.RPAREN) && name.equalsIgnoreCase("count")) {
                advance();
                advance();
                return new CypherAst.CountStar(pos);
            }
            String lower = name.toLowerCase(java.util.Locale.ROOT);
            if (lower.equals("reduce") && peek().is(Type.IDENT) && peek(1).is(Type.EQ)) {
                throw CypherErrors.unsupported("reduce(... = ..., ... IN ...) is not supported yet", pos);
            }
            if (LIST_PREDICATES.contains(lower) && peek().is(Type.IDENT) && peek(1).isWord("IN")) {
                if (!QUANTIFIERS.contains(lower)) {
                    // filter(...) and extract(...) left Cypher with Neo4j 4: a list comprehension spells them
                    throw CypherErrors.syntax(name + "(... IN ...) is not supported: use a list comprehension", pos);
                }
                String variable = advance().text();
                advance();
                Expr list = expression();
                if (!matchWord("WHERE")) {
                    throw error("expected WHERE in " + name + "(... IN ...) but found '" + peek().text() + "'");
                }
                Expr where = expression();
                expect(Type.RPAREN, "')'");
                return new CypherAst.Quantifier(name, variable, list, where, pos);
            }
            boolean distinct = matchWord("DISTINCT");
            List<Expr> args = new ArrayList<>();
            if (!peek().is(Type.RPAREN)) {
                do {
                    args.add(expression());
                } while (match(Type.COMMA));
            }
            expect(Type.RPAREN, "')'");
            return new CypherAst.Call(name, distinct, args, pos);
        }
        return new CypherAst.Var(t.text(), pos);
    }

    private Expr caseExpression(Pos pos) {
        if (atWord("END")) {
            throw error("a CASE needs at least one WHEN");
        }
        Expr operand = atWord("WHEN") ? null : expression();
        List<CypherAst.When> whens = new ArrayList<>();
        while (matchWord("WHEN")) {
            Expr c = expression();
            expectWord("THEN");
            whens.add(new CypherAst.When(c, expression()));
        }
        if (whens.isEmpty()) {
            throw error("a CASE needs at least one WHEN");
        }
        Expr elseResult = matchWord("ELSE") ? expression() : null;
        expectWord("END");
        return new CypherAst.Case(operand, whens, elseResult, pos);
    }

    /** An EXISTS subquery's {@code WITH v1, ..., count(*) AS c [WHERE having]} (the study's §12.77): variables and
     *  one aliased {@code count(*)}, in any order; any other WITH is refused by name. */
    private CypherAst.Counted counted() {
        Token with = advance();
        List<String> keys = new ArrayList<>();
        String alias = null;
        do {
            Token t = peek();
            if (t.isWord("COUNT") && peek(1).is(Type.LPAREN) && peek(2).is(Type.STAR) && peek(3).is(Type.RPAREN)
                    && peek(4).isWord("AS") && peek(5).is(Type.IDENT) && alias == null) {
                for (int i = 0; i < 5; i++) {
                    advance();
                }
                alias = advance().text();
            } else if (t.is(Type.IDENT) && (peek(1).is(Type.COMMA) || peek(1).isWord("WHERE")
                    || peek(1).isWord("RETURN") || peek(1).is(Type.RBRACE))) {
                keys.add(advance().text());
            } else {
                throw CypherErrors.unsupported("an EXISTS subquery's WITH other than its variables and one count(*)"
                        + " is not supported yet", with.pos());
            }
        } while (match(Type.COMMA));
        if (alias == null) {
            throw CypherErrors.unsupported("an EXISTS subquery's WITH other than its variables and one count(*) is"
                    + " not supported yet", with.pos());
        }
        Expr having = matchWord("WHERE") ? expression() : null;
        return new CypherAst.Counted(List.copyOf(keys), alias, having, with.pos());
    }

    /**
     * An EXISTS subquery's normal form (docs/CYPHER_STUDY_2026_10_15.md §12.77): {@code EXISTS { MATCH (m) WHERE
     * EXISTS { pattern } }} and {@code EXISTS { MATCH (m) WHERE pattern }}, the outer match a lone node {@code m}
     * (its labels and properties, if any, moved onto the inner pattern's {@code m}) whose variable the inner pattern
     * binds, are the inner pattern's own existence: a lone node has no relationship, so the two matches' uniqueness
     * rules agree, and the rows the outer match adds are the inner pattern's {@code m}. The shape is rearranged,
     * nothing computed; any other subquery stays as written.
     */
    static CypherAst.ExistsSubquery nested(CypherAst.ExistsSubquery ex) {
        if (ex.counted() != null) {
            return ex;
        }
        PathPattern outer = ex.pattern();
        if (outer.variable() != null || outer.elements().size() != 1
                || !(outer.elements().get(0) instanceof CypherAst.NodePattern lone) || lone.variable() == null) {
            return ex;
        }
        Expr where = ex.where();
        while (where instanceof CypherAst.Paren paren) {
            where = paren.inner();
        }
        PathPattern inner;
        Expr innerWhere;
        if (where instanceof CypherAst.ExistsSubquery sub && sub.counted() == null) {
            inner = sub.pattern();
            innerWhere = sub.where();
        } else if (where instanceof CypherAst.PatternPredicate pp) {
            inner = pp.pattern();
            innerWhere = null;
        } else {
            return ex;
        }
        if (inner.variable() != null) {
            return ex;
        }
        List<PatternElement> elements = new ArrayList<>();
        boolean bound = false;
        for (PatternElement e : inner.elements()) {
            if (!bound && e instanceof CypherAst.NodePattern n && lone.variable().equals(n.variable())) {
                if (!lone.labels().isEmpty() && !n.labels().isEmpty()
                        || !lone.properties().isEmpty() && !n.properties().isEmpty()) {
                    // (both levels constrain the node: kept as written)
                    return ex;
                }
                bound = true;
                elements.add(new CypherAst.NodePattern(n.variable(), n.labels().isEmpty() ? lone.labels()
                        : n.labels(), n.properties().isEmpty() ? lone.properties() : n.properties(), n.pos()));
                continue;
            }
            elements.add(e);
        }
        if (!bound) {
            // (an inner pattern that does not bind the node: the outer match counts every node, not the same)
            return ex;
        }
        return nested(new CypherAst.ExistsSubquery(new PathPattern(null, elements, inner.pos()), innerWhere,
                ex.items(), ex.pos()));
    }
}
