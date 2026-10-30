// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.ts;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlLexer;
import com.legend.protocol.sql.SqlLexer.Kind;
import com.legend.protocol.sql.SqlLexer.Token;
import com.legend.protocol.sql.SqlParser;
import com.legend.protocol.sql.SqlSyntaxException;
import com.legend.protocol.sql.SqlTranslationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * THE {@code #TS{...}#} ISLAND'S TEXT to {@link TsAst} (docs/TIMESERIES_LANGUAGE_2026_10_29.md §3, R1; v0 of
 * docs/TS_LANGUAGE_BUILD_2026_10_30.md): {@code [WITH name AS (source), ...] SELECT item, ... FROM source [alias]
 * ON date [BY instrument] [FILTER condition]}. The text is read by Postgres's lexer ({@link SqlLexer}); the clauses
 * are found among its tokens outside parentheses, and every expression in them is read by Postgres's grammar
 * ({@link SqlParser#expression}) where the island's text has it, so a refusal names its place. Two marks are made
 * first, in place: a variable of the enclosing Pure ({@code $quotes}, which Postgres reads as the start of a quoted
 * string) is read as a name, and {@code all} as a window ({@code sum(x, all)}, a word Postgres reserves) as a column
 * of the name {@link #ALL}; each is known by its position. What v1 adds (subscripts, spans, {@code SAMPLE BY},
 * {@code FILL}, several sources) is refused by name; what is not the language is refused with its fix.
 */
public final class TsParser {

    /** The name {@code all} as a window is read as, at the positions {@link TsAst.Query#alls} holds. */
    public static final String ALL = "__ts_all";

    /** The function a subscript is read as (R2): {@code close['SPX', venue = 'NYSE']} is
     *  {@code __ts_pick(close, 'SPX', venue = 'NYSE')}, its line first, then its matches. */
    public static final String PICK = "__ts_pick";

    /** The clauses that end the FROM clause: the language's own, and SQL's that it refuses with their fix. */
    private static final Set<String> AFTER_FROM = Set.of("where", "filter", "sample", "fill", "group", "order",
            "limit", "offset", "having", "window", "union", "except", "intersect", "fetch");

    /** SQL's clauses the language does not have, each refused with what the language says instead. */
    private static final Map<String, String> NOT_CLAUSES = Map.of(
            "group", "#TS has no GROUP BY: a source's instruments are its BY column (FROM prices ON date BY symbol),"
                    + " and a period's figures are SAMPLE BY's (v1)",
            "order", "#TS has no ORDER BY: its answer is in date order, then the instrument's",
            "limit", "#TS has no LIMIT: FILTER the answer's dates (FILTER date >= '2024-01-01')",
            "offset", "#TS has no OFFSET: FILTER the answer's dates (FILTER date >= '2024-01-01')",
            "having", "#TS has no HAVING: FILTER filters the answer (FILTER ma20 > 100)",
            "window", "#TS has no WINDOW clause: a function along time takes its window as an argument, avg(x, 20)",
            "fetch", "#TS has no FETCH: FILTER the answer's dates (FILTER date >= '2024-01-01')");

    /** The spans of §5: {@code 30d}, {@code 3mo}, {@code 1y}; {@code m} too, which it refuses. */
    private static final Set<String> SPANS = Set.of("d", "w", "mo", "q", "y", "h", "min", "s", "m");

    /** The calendar periods SAMPLE BY takes (R5). */
    private static final Set<String> PERIODS = Set.of("d", "w", "mo", "q", "y");

    /** FILL's ways (R6). */
    private static final Set<String> FILLS = Set.of("PREV", "LINEAR", "NULL", "ZERO");

    private final List<Token> tokens;
    private final Set<SqlAst.Pos> variables;
    private final Set<SqlAst.Pos> alls;
    private final Map<SqlAst.Pos, TsAst.Span> spans;
    private final Set<SqlAst.Pos> read = new java.util.LinkedHashSet<>();
    private int at;

    private TsParser(List<Token> tokens, Set<SqlAst.Pos> variables, Set<SqlAst.Pos> alls,
            Map<SqlAst.Pos, TsAst.Span> spans) {
        this.tokens = tokens;
        this.variables = variables;
        this.alls = alls;
        this.spans = spans;
    }

    /** The query {@code text} holds; a {@link SqlTranslationException} for text that is not one, placed in it. */
    public static TsAst.Query parse(String text) {
        Set<SqlAst.Pos> variables = new java.util.LinkedHashSet<>();
        Map<SqlAst.Pos, TsAst.Span> spans = new java.util.LinkedHashMap<>();
        String marked = marked(text, variables, spans);
        List<Token> lexed;
        try {
            lexed = SqlLexer.tokenize(marked);
        } catch (SqlSyntaxException e) {
            throw syntax(e);
        }
        Set<SqlAst.Pos> alls = new java.util.LinkedHashSet<>();
        List<Token> tokens = new ArrayList<>(lexed.size());
        for (int i = 0; i < lexed.size(); i++) {
            Token t = lexed.get(i);
            if (t.is("all") && i > 0 && lexed.get(i - 1).kind() == Kind.COMMA && i + 1 < lexed.size()
                    && (lexed.get(i + 1).kind() == Kind.RPAREN || lexed.get(i + 1).kind() == Kind.COMMA)) {
                // a window of everything to date (R4): read as a column the builder knows by its place
                tokens.add(new Token(Kind.IDENT, ALL, t.pos()));
                alls.add(t.pos());
            } else {
                tokens.add(t);
            }
        }
        TsParser p = new TsParser(tokens, variables, alls, spans);
        TsAst.Query q = p.query();
        for (SqlAst.Pos v : variables) {
            if (!p.read.contains(v)) {
                throw unsupported("a variable of the enclosing Pure is read as a source, FROM $quotes ON date"
                        + " BY symbol; in an expression it is v1's", v);
            }
        }
        return q;
    }

    // ------------------------------------------------------------------ the marks, in place

    /** {@code text} with each {@code $name} made {@code _name} (its position in {@code variables}) and each span's
     *  digits made {@code _} ({@code 3mo} a name {@code _mo}, the span in {@code spans} by its position); every other
     *  character where it was, so a token's position is the text's. */
    private static String marked(String text, Set<SqlAst.Pos> variables, Map<SqlAst.Pos, TsAst.Span> spans) {
        StringBuilder out = new StringBuilder(text);
        int line = 1;
        int column = 1;
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            int skip = skipped(text, i);
            if (skip > i) {
                for (int k = i; k < skip; k++) {
                    if (text.charAt(k) == '\n') {
                        line++;
                        column = 1;
                    } else {
                        column++;
                    }
                }
                i = skip;
                continue;
            }
            if (c == '$' && i + 1 < text.length() && identStart(text.charAt(i + 1))) {
                out.setCharAt(i, '_');
                variables.add(new SqlAst.Pos(line, column));
            } else if (isDigit(c) && (i == 0 || !identPart(text.charAt(i - 1)) && text.charAt(i - 1) != '.')) {
                int j = i;
                while (j < text.length() && (isDigit(text.charAt(j)) || text.charAt(j) == '_')) {
                    j++;
                }
                int k = j;
                while (k < text.length() && Character.isLetter(text.charAt(k))) {
                    k++;
                }
                String unit = text.substring(j, k).toLowerCase(Locale.ROOT);
                if (k > j && SPANS.contains(unit) && (k >= text.length() || !identPart(text.charAt(k)))) {
                    SqlAst.Pos at = new SqlAst.Pos(line, column);
                    if ("m".equals(unit)) {
                        throw invalid("m is minutes or months: write " + text.substring(i, j) + "min or "
                                + text.substring(i, j) + "mo (§5)", at);
                    }
                    String digits = text.substring(i, j).replace("_", "");
                    if (digits.length() > 6) {
                        throw invalid("a span of " + digits + " " + unit + " is too long", at);
                    }
                    spans.put(at, new TsAst.Span(Integer.parseInt(digits), unit, at));
                    for (int d = i; d < j; d++) {
                        out.setCharAt(d, '_');
                    }
                }
            }
            if (c == '\n') {
                line++;
                column = 1;
            } else {
                column++;
            }
            i++;
        }
        return out.toString();
    }

    /** Where a string, a quoted name or a comment starting at {@code i} ends (past its close), or {@code i}. */
    private static int skipped(String text, int i) {
        char c = text.charAt(i);
        if (c == '\'' || c == '"') {
            int j = i + 1;
            while (j < text.length()) {
                if (text.charAt(j) == c) {
                    if (j + 1 < text.length() && text.charAt(j + 1) == c) {
                        j += 2;
                        continue;
                    }
                    return j + 1;
                }
                j++;
            }
            return text.length();
        }
        if (c == '-' && i + 1 < text.length() && text.charAt(i + 1) == '-') {
            int end = text.indexOf('\n', i);
            return end < 0 ? text.length() : end;
        }
        if (c == '/' && i + 1 < text.length() && text.charAt(i + 1) == '*') {
            int depth = 0;
            int j = i;
            while (j < text.length()) {
                if (text.startsWith("/*", j)) {
                    depth++;
                    j += 2;
                } else if (text.startsWith("*/", j)) {
                    depth--;
                    j += 2;
                    if (depth == 0) {
                        return j;
                    }
                } else {
                    j++;
                }
            }
            return text.length();
        }
        return i;
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean identStart(char c) {
        return Character.isLetter(c) || c == '_';
    }

    private static boolean identPart(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    // ------------------------------------------------------------------ the query

    private TsAst.Query query() {
        List<TsAst.With> withs = new ArrayList<>();
        if (peek().is("with")) {
            withs = withs();
        }
        if (!peek().is("select")) {
            throw invalid(peek().kind() == Kind.EOF ? "a #TS query is SELECT item, ... FROM source ON date"
                    : "a #TS query begins with SELECT (or WITH), not \"" + peek().text() + "\"", peek().pos());
        }
        at++;
        int itemsEnd = depthZero(at, tokens.size() - 1, t -> t.is("from"));
        if (itemsEnd < 0) {
            throw invalid("a #TS query reads FROM a source: SELECT ... FROM prices ON date BY symbol", peek().pos());
        }
        List<TsAst.Item> items = items(at, itemsEnd, false);
        at = itemsEnd + 1;
        int fromEnd = depthZero(at, tokens.size() - 1, t -> AFTER_FROM.stream().anyMatch(t::is));
        int sourcesEnd = fromEnd < 0 ? tokens.size() - 1 : fromEnd;
        List<TsAst.From> sources = new ArrayList<>();
        for (int[] slice : split(at, sourcesEnd)) {
            sources.add(from(slice[0], slice[1]));
        }
        at = sourcesEnd;
        SqlAst.Expr filter = null;
        TsAst.Span sample = null;
        TsAst.Fill fill = null;
        while (peek().kind() != Kind.EOF) {
            Token t = peek();
            String word = t.text().toLowerCase(Locale.ROOT);
            if (t.is("where")) {
                throw invalid("to filter the data, put the WHERE with its source: FROM (prices WHERE ...) ON date;"
                        + " to filter the answer, use FILTER", t.pos());
            }
            if (t.is("sample") && sample == null && fill == null && filter == null) {
                Token by = tokens.get(at + 1);
                Token span = tokens.get(at + 2);
                TsAst.Span s = by.is("by") ? spans.get(span.pos()) : null;
                if (s == null || span.kind() != Kind.IDENT) {
                    throw invalid("SAMPLE BY takes a calendar period: SAMPLE BY 1w (R5)", t.pos());
                }
                if (s.n() != 1 || !PERIODS.contains(s.unit())) {
                    throw invalid("SAMPLE BY takes one calendar period: 1d, 1w, 1mo, 1q or 1y (R5)", span.pos());
                }
                sample = s;
                at += 3;
                continue;
            }
            if (t.is("fill") && fill == null && filter == null) {
                Token open = tokens.get(at + 1);
                Token how = tokens.get(at + 2);
                Token close = tokens.get(at + 3);
                String mode = how.kind() == Kind.INTEGER && "0".equals(how.text()) ? "ZERO"
                        : how.kind() == Kind.IDENT ? how.text().toUpperCase(Locale.ROOT) : "";
                if (open.kind() != Kind.LPAREN || close.kind() != Kind.RPAREN || !FILLS.contains(mode)) {
                    throw invalid("FILL is FILL(PREV), FILL(LINEAR), FILL(NULL) or FILL(0) (R6)", t.pos());
                }
                fill = new TsAst.Fill(mode, t.pos());
                at += 4;
                continue;
            }
            if (t.is("sample") || t.is("fill")) {
                throw invalid("after the sources come SAMPLE BY, then FILL, then FILTER, each once", t.pos());
            }
            if (t.kind() == Kind.IDENT && NOT_CLAUSES.containsKey(word)) {
                throw invalid(NOT_CLAUSES.get(word), t.pos());
            }
            if (t.is("filter") && filter == null) {
                // its condition up to a clause after it (refused below: FILTER is the last)
                int stop = depthZero(at + 1, tokens.size() - 1, x -> x.kind() == Kind.IDENT
                        && (x.is("sample") || x.is("fill") || NOT_CLAUSES.containsKey(x.text().toLowerCase(Locale.ROOT))));
                int end = stop < 0 ? tokens.size() - 1 : stop;
                if (at + 1 >= end) {
                    throw invalid("FILTER takes a condition over the answer's columns: FILTER date >= '2024-01-01'",
                            t.pos());
                }
                filter = expression(at + 1, end, "FILTER");
                at = end;
                continue;
            }
            throw invalid("syntax error at or near \"" + t.text() + "\"", t.pos());
        }
        return new TsAst.Query(withs, items, sources, sample, fill, filter, alls, spans);
    }

    /** {@code WITH name AS (source), ...}, up to its SELECT. */
    private List<TsAst.With> withs() {
        List<TsAst.With> out = new ArrayList<>();
        Token with = tokens.get(at++);
        if (peek().is("recursive")) {
            throw invalid("#TS has no WITH RECURSIVE: a WITH names a source, WITH quotes AS (SELECT ... FROM ...)",
                    peek().pos());
        }
        while (true) {
            Token name = tokens.get(at);
            if (name.kind() != Kind.IDENT && name.kind() != Kind.QUOTED_IDENT || !tokens.get(at + 1).is("as")
                    || tokens.get(at + 2).kind() != Kind.LPAREN) {
                throw invalid("a WITH names a source: WITH quotes AS (SELECT ... FROM MarketQuote)", name.pos());
            }
            int close = closing(at + 2);
            out.add(new TsAst.With(ident(name), parenthesized(at + 3, close, tokens.get(at + 2).pos())));
            at = close + 1;
            if (peek().kind() != Kind.COMMA) {
                break;
            }
            at++;
        }
        if (out.isEmpty()) {
            throw invalid("a WITH names a source: WITH quotes AS (SELECT ... FROM MarketQuote)", with.pos());
        }
        return out;
    }

    /** The items of a select list in tokens {@code [from, to)}; {@code star}: whether {@code *} may be one (a
     *  subquery's). */
    private List<TsAst.Item> items(int from, int to, boolean star) {
        if (from >= to) {
            throw invalid("SELECT names its items: SELECT avg(close, 20) AS ma20", tokens.get(from).pos());
        }
        List<TsAst.Item> out = new ArrayList<>();
        for (int[] slice : split(from, to)) {
            int a = slice[0];
            int b = slice[1];
            if (a >= b) {
                throw invalid("an item is missing: SELECT avg(close, 20) AS ma20, ...", tokens.get(a).pos());
            }
            Token first = tokens.get(a);
            if (b - a == 1 && first.isOp("*")) {
                if (!star) {
                    throw invalid("an item of #TS is a line, expression AS name (R7): SELECT * is a source's,"
                            + " FROM (SELECT *, price * qty AS value FROM trades) ON date", first.pos());
                }
                out.add(new TsAst.Item(null, null, first.pos()));
                continue;
            }
            SqlAst.Ident alias = null;
            int end = b;
            if (b - a >= 3 && tokens.get(b - 2).is("as") && name(tokens.get(b - 1))) {
                alias = ident(tokens.get(b - 1));
                end = b - 2;
            }
            SqlAst.Expr e;
            try {
                e = expression(a, end, "SELECT");
            } catch (SqlTranslationException failed) {
                // an item named without AS (Postgres's bare alias): said with its fix
                if (alias == null && b - a >= 2 && name(tokens.get(b - 1)) && parses(a, b - 1)) {
                    throw invalid("name an item with AS: ... AS " + tokens.get(b - 1).text(), tokens.get(b - 1).pos());
                }
                throw failed;
            }
            out.add(new TsAst.Item(e, alias, first.pos()));
        }
        return out;
    }

    /** A FROM source in tokens {@code [from, to)}: {@code source [[AS] alias] ON date [BY instrument]}. */
    private TsAst.From from(int from, int to) {
        if (from >= to) {
            throw invalid("FROM names a source: FROM prices ON date BY symbol", tokens.get(from).pos());
        }
        Token first = tokens.get(from);
        int i;
        TsAst.Source source;
        if (first.kind() == Kind.LPAREN) {
            int close = closing(from);
            if (close >= to) {
                throw invalid("syntax error at end of input", tokens.get(to).pos());
            }
            source = parenthesized(from + 1, close, first.pos());
            i = close + 1;
        } else {
            int[] end = new int[1];
            source = named(from, to, end);
            i = end[0];
        }
        SqlAst.Ident alias = null;
        if (i < to && tokens.get(i).is("as")) {
            i++;
        }
        if (i < to && name(tokens.get(i)) && !tokens.get(i).is("on") && !tokens.get(i).is("by")) {
            alias = ident(tokens.get(i));
            i++;
        }
        if (i >= to || !tokens.get(i).is("on")) {
            throw invalid("a source names its date column: FROM prices ON date [BY symbol]",
                    i < tokens.size() ? tokens.get(i).pos() : first.pos());
        }
        int[] next = new int[1];
        SqlAst.ColumnRef on = path(i + 1, to, next, "ON");
        i = next[0];
        SqlAst.ColumnRef by = null;
        if (i < to && tokens.get(i).is("by")) {
            by = path(i + 1, to, next, "BY");
            i = next[0];
        }
        if (i < to) {
            throw invalid("syntax error at or near \"" + tokens.get(i).text() + "\": a source is FROM prices"
                    + " [alias] ON date [BY symbol]", tokens.get(i).pos());
        }
        return new TsAst.From(source, alias, on, by, first.pos());
    }

    /** The source in parentheses in tokens {@code [from, to)}: {@code (SELECT items FROM source [WHERE condition])} or
     *  {@code (source WHERE condition)}. */
    private TsAst.Source parenthesized(int from, int to, SqlAst.Pos pos) {
        if (from >= to) {
            throw invalid("a source in parentheses is (prices WHERE ...) or (SELECT ... FROM prices)", pos);
        }
        if (tokens.get(from).is("select")) {
            int fromAt = depthZero(from + 1, to, t -> t.is("from"));
            if (fromAt < 0) {
                throw invalid("a subquery reads FROM a source: (SELECT ... FROM MarketQuote)", tokens.get(from).pos());
            }
            List<TsAst.Item> items = items(from + 1, fromAt, true);
            int[] end = new int[1];
            if (fromAt + 1 >= to) {
                throw invalid("FROM names a source: (SELECT ... FROM MarketQuote)", tokens.get(fromAt).pos());
            }
            TsAst.Source of = named(fromAt + 1, to, end);
            SqlAst.Expr where = null;
            int i = end[0];
            if (i < to && tokens.get(i).is("where")) {
                if (i + 1 >= to) {
                    throw invalid("WHERE takes a condition: WHERE volume > 0", tokens.get(i).pos());
                }
                where = expression(i + 1, to, "WHERE");
            } else if (i < to) {
                Token t = tokens.get(i);
                throw unsupported("a source's subquery in #TS is SELECT ... FROM source [WHERE ...]: \"" + t.text()
                        + "\" is not one of its clauses yet", t.pos());
            }
            return new TsAst.Flat(items, of, where, pos);
        }
        int[] end = new int[1];
        TsAst.Source of = named(from, to, end);
        int i = end[0];
        if (i >= to || !tokens.get(i).is("where")) {
            throw invalid("a source in parentheses is (prices WHERE condition) or (SELECT ... FROM prices)",
                    i < to ? tokens.get(i).pos() : pos);
        }
        if (i + 1 >= to) {
            throw invalid("WHERE takes a condition: (prices WHERE volume > 0)", tokens.get(i).pos());
        }
        return new TsAst.Filtered(of, expression(i + 1, to, "WHERE"), pos);
    }

    /** A source by its name, or a variable of the enclosing Pure, from token {@code from}; {@code end[0]} set past it.
     *  A name is words joined by {@code ::} (a package) and {@code .} (a table's schema). */
    private TsAst.Source named(int from, int to, int[] end) {
        Token first = tokens.get(from);
        if (first.kind() == Kind.IDENT && variables.contains(first.pos())) {
            read.add(first.pos());
            end[0] = from + 1;
            return new TsAst.Variable(first.text().substring(1), first.pos());
        }
        if (!name(first)) {
            throw invalid("FROM names a source, a class, a table or $variable, not \"" + first.text() + "\"",
                    first.pos());
        }
        StringBuilder name = new StringBuilder(first.text());
        int i = from + 1;
        while (i + 1 < to && (tokens.get(i).isOp("::") || tokens.get(i).kind() == Kind.DOT)
                && name(tokens.get(i + 1))) {
            name.append(tokens.get(i).isOp("::") ? "::" : ".").append(tokens.get(i + 1).text());
            i += 2;
        }
        end[0] = i;
        return new TsAst.Named(name.toString(), first.pos());
    }

    /** A column or a dotted path from token {@code from}: {@code date}, {@code instrument.ticker}; {@code end[0]} set
     *  past it. */
    private SqlAst.ColumnRef path(int from, int to, int[] end, String clause) {
        if (from >= to || !name(tokens.get(from))) {
            throw invalid(clause + " names a column: ON date BY symbol", tokens.get(Math.min(from, to)).pos());
        }
        List<SqlAst.Ident> parts = new ArrayList<>(List.of(ident(tokens.get(from))));
        int i = from + 1;
        while (i + 1 < to && tokens.get(i).kind() == Kind.DOT && name(tokens.get(i + 1))) {
            parts.add(ident(tokens.get(i + 1)));
            i += 2;
        }
        end[0] = i;
        return new SqlAst.ColumnRef(parts, tokens.get(from).pos());
    }

    // ------------------------------------------------------------------ tokens

    /** The expression in tokens {@code [from, to)}, read by Postgres's grammar, each subscript as a pick
     *  ({@link #PICK}). */
    private SqlAst.Expr expression(int from, int to, String clause) {
        for (int i = from; i < to; i++) {
            Token t = tokens.get(i);
            if (t.is("filter") && i + 2 < to && tokens.get(i + 1).kind() == Kind.LPAREN
                    && tokens.get(i + 2).is("where")) {
                throw invalid("#TS picks a line's rows by its subscript (R8): write sum(qty['AAPL', side = 'buy'])",
                        t.pos());
            }
        }
        List<Token> slice = picked(new ArrayList<>(tokens.subList(from, to)));
        slice.add(new Token(Kind.EOF, "", tokens.get(to).pos()));
        try {
            return SqlParser.expression(slice);
        } catch (SqlSyntaxException e) {
            throw syntax(e);
        }
    }

    /** {@code slice} with each subscript, innermost first, read as a pick: {@code line[a, b]} as
     *  {@code __ts_pick(line, a, b)}, its line a column or a dotted path ({@code px.close}); each token where it was. */
    private static List<Token> picked(List<Token> slice) {
        while (true) {
            int open = -1;
            int close = -1;
            for (int i = 0; i < slice.size(); i++) {
                if (slice.get(i).kind() == Kind.LBRACKET) {
                    open = i;
                } else if (slice.get(i).kind() == Kind.RBRACKET && open >= 0) {
                    close = i;
                    break;
                }
            }
            if (open < 0) {
                return slice;
            }
            if (close < 0) {
                throw syntaxAt("syntax error: a subscript is not closed, close['SPX']", slice.get(open).pos());
            }
            int start = open - 1;
            if (start < 0 || !name(slice.get(start))) {
                throw invalid("a subscript picks a column's line: close['SPX'] (R2)", slice.get(open).pos());
            }
            while (start >= 2 && slice.get(start - 1).kind() == Kind.DOT && name(slice.get(start - 2))) {
                start -= 2;
            }
            if (close == open + 1) {
                throw invalid("a subscript names an instrument or a match: close['SPX'] (R2)", slice.get(open).pos());
            }
            SqlAst.Pos at = slice.get(start).pos();
            slice.set(close, new Token(Kind.RPAREN, ")", slice.get(close).pos()));
            slice.set(open, new Token(Kind.COMMA, ",", slice.get(open).pos()));
            slice.add(start, new Token(Kind.LPAREN, "(", at));
            slice.add(start, new Token(Kind.QUOTED_IDENT, PICK, at));
        }
    }

    /** Whether tokens {@code [from, to)} read as an expression. */
    private boolean parses(int from, int to) {
        try {
            expression(from, to, "SELECT");
            return true;
        } catch (SqlTranslationException e) {
            return false;
        }
    }

    /** The first token in {@code [from, to)} outside parentheses and brackets that {@code stop} matches, or -1. */
    private int depthZero(int from, int to, java.util.function.Predicate<Token> stop) {
        int depth = 0;
        for (int i = from; i < to; i++) {
            Token t = tokens.get(i);
            if (t.kind() == Kind.LPAREN || t.kind() == Kind.LBRACKET) {
                depth++;
            } else if (t.kind() == Kind.RPAREN || t.kind() == Kind.RBRACKET) {
                depth--;
            } else if (depth == 0 && stop.test(t)) {
                return i;
            }
        }
        return -1;
    }

    /** The slices of {@code [from, to)} between its commas outside parentheses and brackets, each {@code [a, b)}. */
    private List<int[]> split(int from, int to) {
        List<int[]> out = new ArrayList<>();
        int start = from;
        while (true) {
            int comma = depthZero(start, to, t -> t.kind() == Kind.COMMA);
            if (comma < 0) {
                out.add(new int[] {start, to});
                return out;
            }
            out.add(new int[] {start, comma});
            start = comma + 1;
        }
    }

    /** The parenthesis closing the one at {@code open}. */
    private int closing(int open) {
        int depth = 0;
        for (int i = open; i < tokens.size(); i++) {
            Kind k = tokens.get(i).kind();
            if (k == Kind.LPAREN) {
                depth++;
            } else if (k == Kind.RPAREN && --depth == 0) {
                return i;
            }
        }
        throw syntaxAt("syntax error at end of input: a parenthesis is not closed", tokens.get(open).pos());
    }

    private Token peek() {
        return tokens.get(at);
    }

    private static boolean name(Token t) {
        return t.kind() == Kind.IDENT || t.kind() == Kind.QUOTED_IDENT;
    }

    private static SqlAst.Ident ident(Token t) {
        return new SqlAst.Ident(t.text(), t.kind() == Kind.QUOTED_IDENT, t.pos());
    }

    private static SqlTranslationException syntax(SqlSyntaxException e) {
        return new SqlTranslationException(e.unsupported() ? SqlTranslationException.Kind.UNSUPPORTED
                : SqlTranslationException.Kind.SYNTAX, e.reason(), e.line(), e.column());
    }

    private static SqlTranslationException syntaxAt(String message, SqlAst.Pos pos) {
        return new SqlTranslationException(SqlTranslationException.Kind.SYNTAX, message, pos.line(), pos.column());
    }

    static SqlTranslationException invalid(String message, SqlAst.Pos pos) {
        return SqlTranslationException.invalid(message, pos);
    }

    static SqlTranslationException unsupported(String message, SqlAst.Pos pos) {
        return SqlTranslationException.unsupported(message, pos);
    }
}
