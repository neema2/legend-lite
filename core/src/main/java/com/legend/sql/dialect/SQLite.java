// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DateFmt;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;

import static com.legend.sql.dialect.ExplosionMarkers.explodedRead;
import static com.legend.sql.dialect.ExplosionMarkers.isMarker;
import static com.legend.sql.dialect.ExplosionMarkers.markerColumn;
import static com.legend.sql.dialect.ExplosionMarkers.nullList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The SQLite EXECUTION dialect (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §12): a sibling of {@link BigQuery}
 * and {@link Postgres} over {@link AnsiSqlRenderer}, replacing the lexicon row SQLite was (remediation T3.2: it
 * differed then only lexically; the PCT lane measured otherwise). Every spelling here was measured on the product's own
 * driver (xerial sqlite-jdbc 3.47.1.0, its build of SQLite 3.47.1 with the math functions and the driver's statistics
 * extension) before it was trusted; the platform's semantics are DuckDB's.
 *
 * <p>What differs from DuckDB, measured:
 * <ul>
 *   <li>a value has no declared type: INTEGER, REAL, TEXT, BLOB or NULL, per value. A boolean is 1 or 0, a date and a
 *       date-time are their ISO text ({@code 'YYYY-MM-DD'}, {@code 'YYYY-MM-DD HH:MM:SS[.f]'}), and a CAST to a type
 *       name casts by its affinity only ({@code CAST('2024-01-01' AS DATE)} is 2024): every cast is decided here;</li>
 *   <li>there is no exact decimal: a decimal is a REAL, and a root decimal column is its text at its scale
 *       ({@code printf('%.2f', x)}), read back exact ({@link #normalize});</li>
 *   <li>a list, a struct, a map and a Variant are JSON text (JSON1): {@code json_array}, {@code json_object}; a list
 *       operation is a correlated subquery over {@code json_each(xs)} (its {@code key} the 0-based position), a fold a
 *       correlated {@code WITH RECURSIVE} (both measured to correlate inside a scalar subquery); a value that leaves
 *       a JSON function through a column loses its JSON subtype, so a nested value is put back with {@code json()}, a
 *       boolean as JSON true/false;</li>
 *   <li>no VALUES alias list ({@code AS t(a, b)}), no quantified comparison, no QUALIFY, no LATERAL (only a
 *       table-valued function correlates: {@code json_each(t.xs)}), no ORDER BY or LIMIT on a UNION branch;</li>
 *   <li>no regular-expression function (REGEXP is an application-defined function, and none is registered: the
 *       database executes, never the host): a literal pattern is compiled here to its automaton's transition table,
 *       which a recursive query walks ({@link SqliteRegex}); base64 and Levenshtein are recursive walks too; no hash,
 *       digest or Jaro-Winkler function: walls, by name;</li>
 *   <li>an integer is 64 bits, and its arithmetic overflows to a REAL silently: a big integer's root column raises
 *       past 64 bits; a statement is at most 1,000,000 bytes, so a repeated large subexpression is written once
 *       ({@link CommonSubexpressions});</li>
 *   <li>LIKE is case-insensitive by default ({@code PRAGMA case_sensitive_like} is the session's);
 *       {@code upper}/{@code lower} fold ASCII only; {@code x / 0} and {@code x % 0} are NULL;</li>
 *   <li>a raised error: SQLite has no RAISE outside a trigger; a bad JSON path is an error carrying its text
 *       ({@code json_extract('{}', <message>)}: "bad JSON path: '...'"), so a message is raised as one.</li>
 * </ul>
 *
 * <p>Every {@link SqlFn} and {@link SqlAgg.Fn} is decided HERE by an exhaustive switch: its spelling, a shared
 * helper of the base named in its arm, or a {@link DialectCapability} wall.
 */
public final class SQLite extends AnsiSqlRenderer {

    /**
     * SQLite (<a href="https://sqlite.org/lang_keywords.html">lang_keywords</a>)
     * &mdash; the ANSI baseline rendering with SQLite's reserved-word list.
     * SQLite accepts the relational core the base renderer emits (SELECT/
     * JOIN/WHERE/GROUP BY/ORDER BY/LIMIT-OFFSET, TRUE/FALSE since 3.23).
     * KNOWN GAPS that fail LOUD at execution, pending SQLite-specific
     * passes: aliased VALUES sources (TDS literals &mdash; SQLite lacks
     * {@code (VALUES ...) AS t(c1, c2)}), typed DATE/TIMESTAMP literals,
     * and everything with no ANSI encoding at all (list lambdas, QUALIFY,
     * ASOF, native PIVOT) on the base renderer's loud defaults.
     */
    public static final Lexicon LEXICON = new Lexicon('"', Set.of(
            "abort", "action", "add", "after", "all", "alter", "analyze", "and", "as",
            "asc", "attach", "autoincrement", "before", "begin", "between", "by",
            "cascade", "case", "cast", "check", "collate", "column", "commit",
            "conflict", "constraint", "create", "cross", "current_date",
            "current_time", "current_timestamp", "database", "default", "deferrable",
            "deferred", "delete", "desc", "detach", "distinct", "drop", "each",
            "else", "end", "escape", "except", "exclusive", "exists", "explain",
            "fail", "for", "foreign", "from", "full", "glob", "group", "having",
            "if", "ignore", "immediate", "in", "index", "indexed", "initially",
            "inner", "insert", "instead", "intersect", "into", "is", "isnull",
            "join", "key", "left", "like", "limit", "match", "natural", "no", "not",
            "notnull", "null", "of", "offset", "on", "or", "order", "outer", "plan",
            "pragma", "primary", "query", "raise", "recursive", "references",
            "regexp", "reindex", "release", "rename", "replace", "restrict", "right",
            "rollback", "row", "savepoint", "select", "set", "table", "temp",
            "temporary", "then", "to", "transaction", "trigger", "union", "unique",
            "update", "using", "vacuum", "values", "view", "virtual", "when",
            "where", "with", "without"));

    /** SQLite (2026-10-11): the portable strategies over its JSON lists (json_each correlates, measured); FULL
     *  OUTER is native (3.39). */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(false, true, true, true);

    public SQLite() {
        super(LEXICON);
    }

    // ==================================================================
    // Session and structure
    // ==================================================================

    /** The engine's H2 text is refused here: SQLite has no translation of it (decision D2: raw text runs on the
     *  database its connection declares; only the engine's H2 text is translated, for H2, DuckDB and Postgres). */
    @Override
    public String fromEngineText(String statement) {
        throw new DialectCapability("the engine's H2 text reached SQLite, which has no translation of it");
    }

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN, INTEGER, BIGINT, HUGEINT -> "INTEGER";
            case DOUBLE -> "REAL";
            case VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ, TIME, JSON, LITERAL, TEMPORAL_TEXT -> "TEXT";
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** No STRUCT type here: a struct cast is refused. */
    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        throw new DialectCapability("a STRUCT type reached a dialect without struct support");
    }

    /** A date format's parts: the base's strftime codes, {@code strftimeText} (its own strftime helper spells the
     *  parts it formats itself; the dialect program's Phase 1, 2026-10-13: each dialect declares its date-format
     *  vocabulary). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        return strftimeText(fl);
    }

    /** LIKE is case-insensitive unless the session says otherwise (measured: 'ABC' LIKE 'a%' is 1). */
    @Override
    public List<String> sessionSetup() {
        return List.of("PRAGMA case_sensitive_like = ON");
    }

    /** No native pivot: a static pivot is emulated by the carrier strategies, so a dynamic one is discovered first. */
    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    /** The portable carrier strategies, star exclusions spelled out (no EXCLUDE), the substring start clamp, whole-
     *  partition ordered sets as correlated subqueries, explosions placed as markers in FROM (a json_each), QUALIFY as
     *  a wrapping subselect, a constant GROUP BY or ORDER BY key as an expression (an integer one is a position), and a
     *  large repeated subexpression written once (a statement is capped at 1,000,000 bytes, measured). */
    /** A Float's literal spelling over the double's text bound ONCE (the portable spelling reads the text some fifty
     *  times, each read this dialect's shortest-digits expression, which no common-subexpression binding counts as
     *  large: a five-element mixed list's sort was 1.6 MB, past the 1 MB statement cap -- measured 2026-10-11). */
    @Override
    protected boolean bindsFloatText() {
        return true;
    }

    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS), new StarExceptToColumns(),
                new SubstringClamp(), new WholePartitionOrderedSets(), new ExplosionMarkers.LateralUnnestAsMarker(),
                new ExplosionMarkers.SelectListUnnestAsLateral(), new ExplosionMarkers.StarOverExplosionUnwrapped(),
                new QualifyToSubselect(true), new ConstantKeysAsExpressions(), new CommonSubexpressions());
    }

    @Override
    protected String starExceptKeyword() {
        throw new DialectCapability("a star exclusion reached SQLite unexpanded (StarExceptToColumns)");
    }

    @Override
    protected String asOfJoinClause() {
        throw new DialectCapability("an as-of join reached SQLite, which has none (the carrier strategies emulate"
                + " the witnessed shapes)");
    }

    @Override
    protected SqlWriter pivotSource(SqlWriter writer, SqlSource.Pivot p, int depth) {
        throw new DialectCapability("a pivot reached SQLite unemulated: it has no PIVOT");
    }

    /** A CTE is evaluated once when the plan says so (MATERIALIZED, SQLite 3.35). */
    @Override
    protected String cteAs(com.legend.sql.SqlWith.Cte c) {
        return c.materialized() ? " AS MATERIALIZED (" : " AS (";
    }

    /** A UNION branch takes no ORDER BY, LIMIT or parentheses here ("ORDER BY clause should come after UNION ALL not
     *  before", measured): such a branch, or a union, is {@code SELECT * FROM (branch)}. */
    @Override
    protected SqlWriter query(SqlWriter writer, SqlQuery q, int depth) {
        if (pendingRoot()) {
            rootSelects.pop();
            rootSelects.push(selectsOf(q));
        }
        if (!(q instanceof SqlUnion u)) {
            return super.query(writer, q, depth);
        }
        String op = u.all() ? "UNION ALL" : "UNION";
        for (int i = 0; i < u.branches().size(); i++) {
            if (i > 0) {
                nl(writer, depth).append(op);
                nl(writer, depth);
            }
            SqlQuery b = u.branches().get(i);
            boolean wrap = !(b instanceof SqlSelect s) || !s.orderBy().isEmpty() || s.limit() != null
                    || s.offset() != null;
            if (wrap) {
                writer.append("SELECT * FROM (");
            }
            query(writer, b, depth);
            if (wrap) {
                writer.append(")");
            }
        }
        return writer;
    }

    /** The explosion markers in scope of the select being written: alias, then each exploded column's list. */
    private final Deque<Map<String, Map<String, SqlExpr>>> laterals = new ArrayDeque<>();

    /** The lateral subqueries in scope of the select being written, as JSON rows: alias, then its columns. */
    private final Deque<Map<String, List<com.legend.sql.OutputCol>>> rowLaterals = new ArrayDeque<>();

    /** The labels of the statement being rendered, as planned (its root's outputs before this dialect's passes). */
    private final Deque<List<com.legend.sql.OutputCol>> roots = new ArrayDeque<>();

    /** The statement's root selects once its root is written (the root, a union's branches, a WITH's body), or the
     *  empty map while the root is still to come. */
    private final Deque<Map<SqlSelect, Boolean>> rootSelects = new ArrayDeque<>();

    private boolean pendingRoot() {
        Map<SqlSelect, Boolean> top = rootSelects.peek();
        return top != null && top.isEmpty();
    }

    private static Map<SqlSelect, Boolean> selectsOf(SqlQuery q) {
        Map<SqlSelect, Boolean> out = new IdentityHashMap<>();
        collectSelects(q, out);
        if (out.isEmpty()) {
            out.put(new SqlSelect(List.of(), false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null,
                    null, List.of()), Boolean.FALSE);
        }
        return out;
    }

    private static void collectSelects(SqlQuery q, Map<SqlSelect, Boolean> into) {
        switch (q) {
            case SqlSelect s -> into.put(s, Boolean.TRUE);
            case SqlUnion u -> u.branches().forEach(b -> collectSelects(b, into));
            case com.legend.sql.SqlWith w -> collectSelects(w.body(), into);
        }
    }

    @Override
    public String render(SqlQuery query) {
        roots.push(query.outputs());
        rootSelects.push(new IdentityHashMap<>());
        try {
            return super.render(query);
        } finally {
            roots.pop();
            rootSelects.pop();
        }
    }

    @Override
    public RenderedStatement renderStatement(SqlQuery query) {
        roots.push(query.outputs());
        rootSelects.push(new IdentityHashMap<>());
        try {
            return super.renderStatement(query);
        } finally {
            roots.pop();
            rootSelects.pop();
        }
    }

    /**
     * A select that only explodes lists (an explosion marker) is its reads over the marker's positions, in order; the
     * markers of a FROM are in scope of its column reads, and a star over one is spelled out. A root select's values
     * are delivered as their labels say ({@link #delivered}).
     */
    @Override
    protected SqlWriter select(SqlWriter writer, SqlSelect s, int depth) {
        if (isMarker(s)) {
            String alias = "__u" + depth;
            boolean aligned = s.outputs().size() == s.projections().size();
            List<SqlSelect.Projection> ps = new ArrayList<>();
            for (int i = 0; i < s.projections().size(); i++) {
                SqlSelect.Projection p = s.projections().get(i);
                SqlType slot = aligned ? s.outputs().get(i).type() : p.out() != null ? p.out().type() : null;
                ps.add(new SqlSelect.Projection(explodedRead(alias, p, slot), markerColumn(p), p.out()));
            }
            SqlSelect.SortKey position = new SqlSelect.SortKey(SqlExpr.Column.derived(alias, "key"), true, null, null);
            SqlSelect over = new SqlSelect(ps, false, new SqlSource.Subselect(s, alias, null), null, List.of(), null,
                    null, List.of(position), null, null, List.of());
            Map<SqlSelect, Boolean> top = rootSelects.peek();
            if (top != null && top.containsKey(s)) {
                top.put(over, Boolean.TRUE);
            }
            return select(writer, over, depth);
        }
        Map<String, Map<String, SqlExpr>> scope = new LinkedHashMap<>();
        lateralsOf(s.from(), scope);
        Map<String, List<com.legend.sql.OutputCol>> rows = new LinkedHashMap<>();
        rowLateralsOf(s.from(), rows);
        SqlSelect spelled = delivered(scope.isEmpty() && rows.isEmpty() ? s : starsSpelled(s));
        laterals.push(scope);
        rowLaterals.push(rows);
        try {
            return super.select(writer, spelled.offset() != null && spelled.limit() == null
                    ? spelled.withLimit(-1L) : spelled, depth);
        } finally {
            laterals.pop();
            rowLaterals.pop();
        }
    }

    /**
     * A root select's values as their labels deliver them: a decimal as its text at its scale (SQLite has no exact
     * decimal; the text is read back exact), a Float as a REAL (an integral value is an INTEGER here, measured). A
     * sort key naming a rewritten column is its original expression (ORDER BY reads an alias first).
     */
    private SqlSelect delivered(SqlSelect s) {
        Map<SqlSelect, Boolean> top = rootSelects.peek();
        List<com.legend.sql.OutputCol> labels = roots.peek();
        if (top == null || !Boolean.TRUE.equals(top.get(s)) || labels == null
                || labels.size() != s.projections().size()) {
            return s;
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        Map<String, SqlExpr> moved = new LinkedHashMap<>();
        for (int i = 0; i < s.projections().size(); i++) {
            SqlSelect.Projection p = s.projections().get(i);
            SqlExpr value = deliveredValue(p.expr(), labels.get(i).type());
            if (value != p.expr() && p.outputName() != null) {
                moved.put(p.outputName(), p.expr());
            }
            ps.add(value == p.expr() ? p : new SqlSelect.Projection(value, p.outputName(), p.out()));
        }
        if (moved.isEmpty()) {
            return s;
        }
        List<SqlSelect.SortKey> order = new ArrayList<>();
        for (SqlSelect.SortKey k : s.orderBy()) {
            order.add(k.expr() instanceof SqlExpr.Column c && c.table() == null && moved.containsKey(c.name())
                    ? new SqlSelect.SortKey(java.util.Objects.requireNonNull(moved.get(c.name()), "moved"),
                            k.ascending(), k.nullOrder(), k.outputName())
                    : k);
        }
        return new SqlSelect(ps, s.distinct(), s.from(), s.where(), s.groupBy(), s.having(), s.qualify(), order,
                s.limit(), s.offset(), s.outputs());
    }

    private static SqlExpr deliveredValue(SqlExpr e, SqlType label) {
        if (e instanceof SqlExpr.NullLit || e instanceof SqlExpr.Star || e instanceof SqlExpr.StarExcept) {
            return e;
        }
        if (label instanceof SqlType.Decimal d) {
            return SqlExpr.Call.of(SqlFn.FORMAT, new SqlExpr.StringLit("%." + d.scale() + "f"), e);
        }
        if (label == SqlType.Scalar.DOUBLE) {
            return new SqlExpr.Cast(e, SqlType.Scalar.DOUBLE);
        }
        // an integer past 64 bits has no SQLite value: its arithmetic overflows to a REAL silently (measured:
        // 9223372036854775807 + 3 is 9.22337203685478e+18), so a big integer's column raises instead
        if (label == SqlType.Scalar.HUGEINT) {
            return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.OR,
                    SqlExpr.Call.of(SqlFn.GREATER, e, new SqlExpr.IntLit(Long.MAX_VALUE)),
                    SqlExpr.Call.of(SqlFn.LESS, e, new SqlExpr.IntLit(Long.MIN_VALUE))),
                    SqlExpr.Call.of(SqlFn.ERROR,
                            new SqlExpr.StringLit("an integer past 64 bits has no SQLite value")))),
                    e);
        }
        return e;
    }

    /** {@code s} with each star over a FROM holding a marker spelled out: each other source's columns, then each
     *  exploded column (the marker's positions are a column of their own, which no star may carry). */
    private static SqlSelect starsSpelled(SqlSelect s) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        if (s.projections().isEmpty()) {
            spell(s.from(), null, ps);
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr() instanceof SqlExpr.Star st) {
                spell(s.from(), st.table(), ps);
            } else {
                ps.add(p);
            }
        }
        return new SqlSelect(ps, s.distinct(), s.from(), s.where(), s.groupBy(), s.having(), s.qualify(), s.orderBy(),
                s.limit(), s.offset(), s.outputs());
    }

    private static void spell(SqlSource src, @com.legend.base.Nullable String table, List<SqlSelect.Projection> into) {
        if (src instanceof SqlSource.Join j && rowLateral(j) != null) {
            spell(j.left(), table, into);
            SqlSource.Subselect sub = java.util.Objects.requireNonNull(rowLateral(j), "lateral");
            if (table == null || table.equals(sub.alias())) {
                for (com.legend.sql.OutputCol oc : sub.inner().outputs()) {
                    into.add(new SqlSelect.Projection(SqlExpr.Column.of(sub.alias(), oc), oc.name(), oc));
                }
            }
        } else if (src instanceof SqlSource.Join j) {
            spell(j.left(), table, into);
            spell(j.right(), table, into);
        } else if (src instanceof SqlSource.Subselect sub && isMarker(sub.inner())
                && (table == null || table.equals(sub.alias()))) {
            for (SqlSelect.Projection p : ((SqlSelect) sub.inner()).projections()) {
                into.add(new SqlSelect.Projection(explodedRead(sub.alias(), p, p.out() != null ? p.out().type() : null),
                        markerColumn(p), p.out()));
            }
        } else if (!(src instanceof SqlSource.Dual) && (table == null || table.equals(src.alias()))) {
            into.add(new SqlSelect.Projection(new SqlExpr.Star(src.alias()), null, null));
        }
    }

    /** The lateral subquery a join reads as JSON rows, or null for any other join. */
    private static SqlSource.@com.legend.base.Nullable Subselect rowLateral(SqlSource.Join j) {
        return (j.kind() == SqlSource.Join.Kind.CROSS_LATERAL || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)
                && j.right() instanceof SqlSource.Subselect sub && !isMarker(sub.inner()) ? sub : null;
    }

    /** The lateral subqueries read as JSON rows, by alias, anywhere in a FROM. */
    private static void rowLateralsOf(SqlSource src, Map<String, List<com.legend.sql.OutputCol>> into) {
        if (src instanceof SqlSource.Join j) {
            rowLateralsOf(j.left(), into);
            rowLateralsOf(j.right(), into);
            SqlSource.Subselect sub = rowLateral(j);
            if (sub != null) {
                into.put(sub.alias(), sub.inner().outputs());
            }
        }
    }

    /** A column of a lateral subquery read as JSON rows: its field of the row ({@code ->>}, a Variant's {@code ->}), or
     *  null for any other column. */
    private SqlWriter.@com.legend.base.Nullable Piece rowLateralField(SqlExpr.Column c) {
        String table = c.table();
        if (table == null) {
            return null;
        }
        for (Map<String, List<com.legend.sql.OutputCol>> scope : rowLaterals) {
            List<com.legend.sql.OutputCol> outs = scope.get(table);
            if (outs != null) {
                SqlType t = outs.stream().filter(oc -> oc.name().equals(c.name())).map(com.legend.sql.OutputCol::type)
                        .findFirst().orElse(typeOf(c));
                return w -> w.append("(").append(aliasIdent(table)).append(".value ")
                        .append(t == SqlType.Scalar.JSON ? "->" : "->>").append(" ").append(stringLit(c.name()))
                        .append(")");
            }
        }
        return null;
    }

    /**
     * A lateral subquery's rows, for a database with no LATERAL: the subquery, correlated, inside a scalar subquery
     * that aggregates its rows as a JSON array of objects, and {@code json_each} over it (a table-valued function's
     * arguments correlate with the FROM before it, measured; LEFT JOIN keeps a row with none). Each column is a field.
     */
    private SqlWriter.Piece lateralRows(SqlSource.Subselect sub, int depth) {
        List<com.legend.sql.OutputCol> outs = sub.inner().outputs();
        if (outs.isEmpty()) {
            throw new DialectCapability("a lateral subquery of undeclared columns reached SQLite");
        }
        return w -> {
            w.append("json_each((SELECT json_group_array(json_object(");
            for (int i = 0; i < outs.size(); i++) {
                com.legend.sql.OutputCol oc = outs.get(i);
                w.append(i > 0 ? ", " : "").append(stringLit(oc.name())).append(", ")
                        .piece(jsonOf(x -> x.append(columnRef(SqlExpr.Column.of("__l", oc))), oc.type()));
            }
            w.append(")) FROM (");
            nl(w, depth + 1);
            query(w, sub.inner(), depth + 1);
            nl(w, depth).append(") AS __l))");
        };
    }

    /** The markers' exploded columns, by alias, anywhere in a FROM. */
    private static void lateralsOf(SqlSource src, Map<String, Map<String, SqlExpr>> into) {
        if (src instanceof SqlSource.Join j) {
            lateralsOf(j.left(), into);
            lateralsOf(j.right(), into);
        } else if (src instanceof SqlSource.Subselect sub && sub.inner() instanceof SqlSelect m && isMarker(m)) {
            Map<String, SqlExpr> cols = new LinkedHashMap<>();
            for (SqlSelect.Projection p : m.projections()) {
                cols.put(markerColumn(p), ((SqlExpr.Call) p.expr()).args().get(0));
            }
            into.put(sub.alias(), cols);
        }
    }

    /** An exploded column's read, its list's element at the marker's position, or null for any other column. */
    private SqlWriter.@com.legend.base.Nullable Piece explodedColumn(SqlExpr.Column c) {
        for (Map<String, Map<String, SqlExpr>> scope : laterals) {
            if (c.table() != null) {
                Map<String, SqlExpr> cols = scope.get(c.table());
                if (cols != null && cols.containsKey(c.name())) {
                    return element(java.util.Objects.requireNonNull(cols.get(c.name()), "list"), c.table());
                }
            } else {
                List<Map.Entry<String, Map<String, SqlExpr>>> owners = scope.entrySet().stream()
                        .filter(e -> e.getValue().containsKey(c.name())).toList();
                if (owners.size() == 1) {
                    return element(java.util.Objects.requireNonNull(owners.get(0).getValue().get(c.name()), "list"),
                            owners.get(0).getKey());
                }
            }
        }
        return null;
    }

    /** {@code xs[position]}: the list's element at the marker's 0-based position (NULL past its end). */
    private SqlWriter.Piece element(SqlExpr xs, String position) {
        if (nullList(xs)) {
            return w -> w.append("NULL");
        }
        return elementAt(xs, w -> w.append(aliasIdent(position) + ".key"));
    }

    /** A marker's positions: {@code json_each} over its longest list (DuckDB's parallel UNNEST pads the shorter with
     *  NULL; a NULL list is none), its {@code key} the position; a lateral one joins its left side correlated (a
     *  table-valued function correlates, measured; LEFT JOIN keeps an empty list's row). Any other lateral derived
     *  table is refused: SQLite has no LATERAL. */
    @Override
    protected SqlWriter source(SqlWriter writer, SqlSource src, int depth) {
        if (src instanceof SqlSource.Join j && j.right() instanceof SqlSource.Subselect sub && isMarker(sub.inner())
                && (j.kind() == SqlSource.Join.Kind.CROSS_LATERAL || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)) {
            if (j.on() != null && !(j.on() instanceof SqlExpr.BoolLit b && b.value())) {
                throw new DialectCapability("a lateral explosion with a join condition reached SQLite");
            }
            source(writer, j.left(), depth);
            nl(writer, depth).append(j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? "LEFT JOIN " : "CROSS JOIN ")
                    .piece(positions((SqlSelect) sub.inner())).append(" AS ").append(aliasIdent(sub.alias()));
            return j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? writer.append(" ON TRUE") : writer;
        }
        if (src instanceof SqlSource.Subselect sub && isMarker(sub.inner())) {
            return writer.piece(positions((SqlSelect) sub.inner())).append(" AS ").append(aliasIdent(sub.alias()));
        }
        if (src instanceof SqlSource.Join j && rowLateral(j) != null) {
            SqlSource.Subselect sub = java.util.Objects.requireNonNull(rowLateral(j), "lateral");
            boolean left = j.kind() == SqlSource.Join.Kind.LEFT_LATERAL;
            boolean always = j.on() == null || j.on() instanceof SqlExpr.BoolLit b && b.value();
            source(writer, j.left(), depth);
            nl(writer, depth).append(left ? "LEFT JOIN " : always ? "CROSS JOIN " : "JOIN ")
                    .piece(lateralRows(sub, depth)).append(" AS ").append(aliasIdent(sub.alias()));
            if (left || !always) {
                writer.append(" ON ");
                if (always) {
                    writer.append("1");
                } else {
                    writer.expr(java.util.Objects.requireNonNull(j.on(), "on"), 0);
                }
            }
            return writer;
        }
        return super.source(writer, src, depth);
    }

    private static SqlWriter.Piece positions(SqlSelect m) {
        List<SqlExpr> lists = m.projections().stream().map(p -> ((SqlExpr.Call) p.expr()).args().get(0))
                .filter(xs -> !nullList(xs)).toList();
        return w -> {
            w.append("json_each(");
            if (lists.isEmpty()) {
                w.append("NULL");
            } else if (lists.size() == 1) {
                w.expr(lists.get(0), 0);
            } else {
                // the longest list (its positions are every list's)
                w.append("CASE");
                for (int i = 0; i < lists.size() - 1; i++) {
                    SqlExpr xs = lists.get(i);
                    w.append(" WHEN ");
                    for (int j = 0; j < lists.size(); j++) {
                        if (j != i) {
                            w.append(j > (i == 0 ? 1 : 0) ? " AND " : "").append("coalesce(json_array_length(")
                                    .expr(xs, 0).append("), 0) >= coalesce(json_array_length(").expr(lists.get(j), 0)
                                    .append("), 0)");
                        }
                    }
                    w.append(" THEN ").expr(xs, 0);
                }
                w.append(" ELSE ").expr(lists.get(lists.size() - 1), 0).append(" END");
            }
            w.append(")");
        };
    }

    /** No VALUES alias list ({@code AS t(a, b)} is a syntax error, measured): the rows are named by a select over
     *  them ({@code column1}, {@code column2} ... are VALUES's own names). No rows is one row of NULLs, filtered. */
    @Override
    protected SqlWriter valuesSource(SqlWriter writer, SqlSource.Values v) {
        writer.append("(SELECT ");
        for (int i = 0; i < v.columns().size(); i++) {
            writer.append(i > 0 ? ", " : "").append(v.rows().isEmpty() ? "NULL" : "column" + (i + 1)).append(" AS ")
                    .append(aliasIdent(v.columns().get(i)));
        }
        if (v.rows().isEmpty()) {
            return writer.append(" WHERE 0) AS ").append(aliasIdent(v.alias()));
        }
        writer.append(" FROM (VALUES ");
        for (int r = 0; r < v.rows().size(); r++) {
            writer.append(r > 0 ? ", " : "").append("(").join(v.rows().get(r), ", ",
                    (w, x) -> w.piece(cell(x))).append(")");
        }
        return writer.append(")) AS ").append(aliasIdent(v.alias()));
    }

    /** A value as a row holds it: a nested one as JSON text (a JSON function's result keeps its subtype only in its own
     *  expression). */
    private SqlWriter.Piece cell(SqlExpr x) {
        return w -> w.expr(x, 0);
    }

    // ==================================================================
    // Lexical: identifiers, literals, types
    // ==================================================================

    /** A NUL cannot stand in a literal (SQLite's text stops there): it is {@code char(0)} between segments. */
    @Override
    protected String stringLit(String value) {
        if (value.indexOf('\u0000') >= 0) {
            String[] parts = value.split("\u0000", -1);
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < parts.length; i++) {
                sb.append(i > 0 ? " || char(0) || " : "").append('\'').append(parts[i].replace("'", "''")).append('\'');
            }
            return sb.append(')').toString();
        }
        return "'" + value.replace("'", "''") + "'";
    }

    /** A date is its ISO text. */
    @Override
    protected String dateLit(String iso) {
        return "'" + iso + "'";
    }

    /** A date-time is its ISO text with a space (the form SQLite's date functions write, so texts compare as
     *  instants), its fraction as written. */
    @Override
    protected String timestampLit(String iso) {
        return "'" + iso.replace('T', ' ') + "'";
    }

    /** A time of day is its text. */
    @Override
    protected String timeLit(String iso) {
        return "'" + iso + "'";
    }

    /** A Float literal is a REAL; the infinities are REALs past the range ({@code 9e999}, measured: Infinity); NaN has
     *  no SQLite value (0.0 / 0 is NULL). */
    @Override
    protected String floatLiteral(double v) {
        if (Double.isNaN(v)) {
            throw new DialectCapability("a NaN literal reached SQLite, which has no NaN (0.0 / 0 is NULL)");
        }
        if (Double.isInfinite(v)) {
            return v > 0 ? "9e999" : "-9e999";
        }
        String s = plainFloat(v);
        return s.contains(".") || s.contains("E") || s.contains("e") ? s : s + ".0";
    }

    @Override
    protected String arrayTypeName(String element) {
        return "TEXT";
    }

    @Override
    protected String mapTypeName(String key, String value) {
        return "TEXT";
    }

    /** Alias-less projections label EXPLICITLY from the declared output: SQLite labels an expression by its text. */
    @Override
    protected @com.legend.base.Nullable String implicitLabel(SqlSelect.Projection p) {
        com.legend.sql.OutputCol out = p.out();
        return out != null ? aliasIdent(out.name()) : super.implicitLabel(p);
    }

    /** A NULL is NULL whatever its slot (a typed NULL has no SQLite type to carry). */
    @Override
    protected SqlWriter projection(SqlWriter writer, SqlSelect.Projection p) {
        if (p.expr() instanceof SqlExpr.NullLit) {
            writer.append("NULL");
            String alias = p.alias();
            String label = alias != null ? aliasIdent(alias) : implicitLabel(p);
            return label == null ? writer : writer.append(" AS ").append(label);
        }
        return super.projection(writer, p);
    }

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        return switch (e) {
            case SqlExpr.DecimalLit d -> writer.append(d.value().toPlainString());
            case SqlExpr.ArrayLit a -> writer.append("json_array(").join(a.elements(), ", ", (w, x) -> w.piece(json(x)))
                    .append(")");
            case SqlExpr.BoolLit b -> writer.append(b.value() ? "1" : "0");
            // DuckDB's list(v ORDER BY k): ascending, NULLs last
            case SqlExpr.OrderedListAgg ola -> writer.append("json_group_array(").piece(json(ola.value()))
                    .append(" ORDER BY ").expr(ola.orderBy(), 0).append(" NULLS LAST)");
            case SqlExpr.Membership m when m.collection() instanceof SqlExpr.PlanParam p -> throw listParameter(p);
            case SqlExpr.Quantified q -> writer.piece(quantified(q));
            case SqlExpr.Column c when explodedColumn(c) != null ->
                    writer.piece(java.util.Objects.requireNonNull(explodedColumn(c), "exploded"));
            case SqlExpr.Column c when rowLateralField(c) != null ->
                    writer.piece(java.util.Objects.requireNonNull(rowLateralField(c), "lateral"));
            default -> super.expr(writer, e, parentPrec);
        };
    }

    /** A list plan parameter: SQLite binds no array. */
    private static DialectCapability listParameter(SqlExpr.PlanParam p) {
        return new DialectCapability("list plan parameter '" + p.name() + "' reached SQLite, which binds no array");
    }

    /** {@code v cmp ANY|ALL (subquery)}: SQLite has no quantified comparison ("near ALL: syntax error", measured), so
     *  the subquery's values are counted in SQL's three-valued logic -- ANY is TRUE when one comparison is, FALSE over
     *  no rows or when none is TRUE and none unknown, else NULL; ALL is its dual. The values are the subquery's one
     *  column, named by a CTE's column list. */
    private SqlWriter.Piece quantified(SqlExpr.Quantified q) {
        SqlExpr cmp = SqlExpr.Call.of(q.comparison(), q.value(), SqlExpr.Column.derived(null, "__qc"));
        boolean any = q.quantifier() == SqlExpr.Quantifier.ANY;
        return w -> {
            w.append("(WITH __q(__qc) AS (");
            inline(w, q.subquery());
            w.append(") SELECT CASE WHEN ");
            if (any) {
                w.append("count(CASE WHEN ").expr(cmp, 0).append(" THEN 1 END) > 0 THEN 1 WHEN count(*) = 0 THEN 0");
            } else {
                w.append("count(*) = 0 THEN 1 WHEN count(CASE WHEN NOT (").expr(cmp, 0)
                        .append(") THEN 1 END) > 0 THEN 0");
            }
            w.append(" WHEN count(CASE WHEN (").expr(cmp, 0).append(") IS NULL THEN 1 END) > 0 THEN NULL ELSE ")
                    .append(any ? "0" : "1").append(" END FROM __q)");
        };
    }

    // ==================================================================
    // JSON: how a list, a struct, a map and a Variant hold a value
    // ==================================================================

    private static @com.legend.base.Nullable SqlType typeOf(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
    }

    /** The element type of a list, or null for a value that is no list. */
    private static @com.legend.base.Nullable SqlType listElement(SqlExpr e) {
        return typeOf(e) instanceof SqlType.Array a ? a.element() : null;
    }

    private static boolean nested(@com.legend.base.Nullable SqlType t) {
        return t instanceof SqlType.Array || t instanceof SqlType.Struct || t instanceof SqlType.Map
                || t == SqlType.Scalar.JSON;
    }

    /** A value as a JSON container holds it ({@link #jsonOf}). */
    private SqlWriter.Piece json(SqlExpr x) {
        SqlType t = typeOf(x);
        if (t == null && (x instanceof SqlExpr.ArrayLit || x instanceof SqlExpr.StructLit)) {
            return w -> w.append("json(").expr(x, 0).append(")");
        }
        if (t == null && x instanceof SqlExpr.BoolLit) {
            t = SqlType.Scalar.BOOLEAN;
        }
        return jsonOf(w -> w.expr(x, 0), t);
    }

    /** A value of type {@code t} as a JSON container holds it: a boolean as JSON true or false (it is 1 or 0 here), a
     *  list, struct, map or Variant as its JSON (its text, read by {@code json()}: a column or a subquery's result has
     *  lost its JSON subtype, measured: {@code json_array(j)} quotes it), anything else as it is. */
    private static SqlWriter.Piece jsonOf(SqlWriter.Piece v, @com.legend.base.Nullable SqlType t) {
        if (t == SqlType.Scalar.BOOLEAN) {
            return w -> w.append("json(CASE WHEN ").piece(v).append(" THEN 'true' WHEN NOT (").piece(v)
                    .append(") THEN 'false' END)");
        }
        if (nested(t)) {
            return w -> w.append("json(").piece(v).append(")");
        }
        return v;
    }

    /** The element read by {@code json_each}'s columns as a value of type {@code t}: a Variant element is its JSON text
     *  (a string quoted, a boolean {@code true}), anything else the {@code value} column (a boolean 1 or 0, a nested
     *  value its JSON text). */
    private static String elementRead(@com.legend.base.Nullable SqlType t) {
        return elementRead(t, "");
    }

    /** {@link #elementRead} of the json_each aliased {@code alias}. */
    private static String elementRead(@com.legend.base.Nullable SqlType t, String alias) {
        String q = alias.isEmpty() ? "" : alias + ".";
        return t == SqlType.Scalar.JSON
                ? "CASE " + q + "type WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' WHEN 'null' THEN 'null'"
                        + " WHEN 'array' THEN " + q + "value WHEN 'object' THEN " + q + "value ELSE json_quote(" + q
                        + "value) END"
                : q + "value";
    }

    /** The element of {@code xs} at a 0-based {@code index} (NULL past either end): a Variant's as JSON
     *  ({@code ->}), anything else's as its value. */
    private SqlWriter.Piece elementAt(SqlExpr xs, SqlWriter.Piece index) {
        boolean variant = listElement(xs) == SqlType.Scalar.JSON;
        return w -> w.append(variant ? "(" : "json_extract(").expr(xs, variant ? 9 : 0)
                .append(variant ? " -> ('$[' || (" : ", '$[' || (").piece(index)
                .append(variant ? ") || ']'))" : ") || ']')");
    }

    // ==================================================================
    // Lists: JSON arrays, lambda-less -- each operation a correlated subquery over json_each(xs)
    // ==================================================================

    /** A lambda parameter (or an element's or index's name) as its references spell it. */
    private String param(String name) {
        return columnRef(SqlExpr.Column.derived(null, name));
    }

    /** {@code (SELECT <element> AS e, key AS o [, key + 1 AS i] FROM json_each(xs)) AS e__t}: each element bound to
     *  {@code e} as its type reads, its 0-based position to {@code o}, its 1-based one to {@code i} (DuckDB's index
     *  parameter) when named. A correlated derived table inside a scalar subquery, measured; json_each of NULL is
     *  no rows. */
    private SqlWriter.Piece elements(SqlExpr xs, String e, String o, @com.legend.base.Nullable String i) {
        String read = elementRead(listElement(xs));
        return w -> {
            w.append("(SELECT " + read + " AS ").append(param(e)).append(", key AS ").append(param(o));
            if (i != null) {
                w.append(", key + 1 AS ").append(param(i));
            }
            w.append(" FROM json_each(").expr(xs, 0).append(")) AS ").append(param(e + "__t"));
        };
    }

    /** {@code body}, or NULL for a NULL list (DuckDB's list functions answer NULL there; an aggregate over its no rows
     *  would be the empty array). */
    private static SqlWriter.Piece unlessNull(SqlExpr xs, SqlWriter.Piece body) {
        return w -> w.append("(CASE WHEN ").expr(xs, 4).append(" IS NULL THEN NULL ELSE ").piece(body).append(" END)");
    }

    private static SqlExpr.Lambda lambdaOf(SqlExpr e, SqlFn fn) {
        if (e instanceof SqlExpr.Lambda l && !l.params().isEmpty()) {
            return l;
        }
        throw new DialectCapability(fn + " without a lambda reached SQLite");
    }

    /** The position alias of a lambda's element: unique per lambda, so an inner list operation never shadows it. */
    private static String offsetOf(SqlExpr.Lambda l) {
        return l.params().get(0) + "__o";
    }

    private static @com.legend.base.Nullable String indexOf(SqlExpr.Lambda l) {
        return l.params().size() > 1 ? l.params().get(1) : null;
    }

    /** A lambda's body over elements of {@code element} type: a field read off a struct parameter ({@code p.first},
     *  as DuckDB spells it -- a lambda parameter is no table, so a column qualified by one is a field) becomes a field
     *  read of the JSON object, for every parameter. */
    private static SqlExpr lambdaBody(SqlExpr.Lambda l, @com.legend.base.Nullable SqlType element) {
        SqlExpr body = l.body();
        for (int i = 0; i < l.params().size(); i++) {
            body = structFields(body, l.params().get(i), i == 0 ? element : null);
        }
        return body;
    }

    private static SqlExpr structFields(SqlExpr body, String param, @com.legend.base.Nullable SqlType struct) {
        if (body instanceof SqlExpr.Column c && param.equals(c.table())) {
            SqlExpr source = struct instanceof SqlType.Struct ? SqlExpr.Column.of(null, param, struct, true,
                    com.legend.sql.OutputCol.Origin.DERIVED) : SqlExpr.Column.derived(null, param);
            return new SqlExpr.StructGet(source, c.name(), c.type());
        }
        List<SqlExpr> kids = body.children();
        if (kids.isEmpty()) {
            return body;
        }
        List<SqlExpr> mapped = new ArrayList<>(kids.size());
        boolean changed = false;
        for (SqlExpr k : kids) {
            SqlExpr m = structFields(k, param, struct);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? body.withChildren(mapped) : body;
    }

    /** {@code (SELECT json_group_array(select ORDER BY o) FROM elements [WHERE where])}, NULL for a NULL list: the
     *  elements kept in list order. */
    private SqlWriter.Piece overElements(SqlExpr xs, String e, String o, @com.legend.base.Nullable String i,
            SqlWriter.Piece select, SqlWriter.@com.legend.base.Nullable Piece where) {
        return unlessNull(xs, w -> {
            w.append("(SELECT json_group_array(").piece(select).append(" ORDER BY ").append(param(o)).append(") FROM ")
                    .piece(elements(xs, e, o, i));
            if (where != null) {
                w.append(" WHERE ").piece(where);
            }
            w.append(")");
        });
    }

    /** {@code (SELECT agg(e) FROM elements)}: a list reduced as rows (an empty or NULL list reduces over no rows).
     *  MODE is the most frequent non-NULL element, the first to occur among ties (DuckDB's list_mode). */
    private SqlWriter.Piece overRows(SqlExpr xs, SqlAgg.Fn fn, List<SqlExpr> extras) {
        String e = param("__e");
        String o = param("__o");
        if (fn == SqlAgg.Fn.MODE) {
            if (!extras.isEmpty()) {
                throw new DialectCapability("a mode with arguments reached SQLite");
            }
            return w -> w.append("(SELECT " + e + " FROM ").piece(elements(xs, "__e", "__o", null))
                    .append(" WHERE " + e + " IS NOT NULL GROUP BY " + e + " ORDER BY count(*) DESC, min(" + o
                            + ") LIMIT 1)");
        }
        SqlExpr el = listElement(xs) == null ? SqlExpr.Column.derived(null, "__e") : SqlExpr.Column.param("__e", xs);
        List<SqlExpr> args = new ArrayList<>();
        args.add(el);
        args.addAll(extras);
        List<SqlSelect.SortKey> order = fn == SqlAgg.Fn.STRING_AGG || fn == SqlAgg.Fn.LIST
                ? List.of(new SqlSelect.SortKey(SqlExpr.Column.derived(null, "__o"), true, null, null)) : List.of();
        SqlAgg.Reducer r = new SqlAgg.Reducer(fn, args, false, order);
        return w -> {
            w.append("(SELECT ");
            reducer(w, r);
            w.append(" FROM ").piece(elements(xs, "__e", "__o", null)).append(")");
        };
    }

    @Override
    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        return writer.append("json_array(").join(elements, ", ", (w, x) -> w.piece(json(x))).append(")");
    }

    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit st) {
        return writer.append("json_object(").join(st.fields(), ", ",
                (w, f) -> w.append(stringLit(f.name())).append(", ").piece(json(f.value()))).append(")");
    }

    /** A field of a JSON object: a Variant field as its JSON ({@code ->}), anything else as its value ({@code ->>},
     *  a nested value its JSON text, a boolean 1 or 0). */
    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        SqlType type = typeOf(g);
        if (type == null && typeOf(g.source()) instanceof SqlType.Struct struct) {
            for (SqlType.Struct.Field f : struct.fields()) {
                if (f.name().equals(g.field())) {
                    type = f.type();
                }
            }
        }
        return writer.append("(").expr(g.source(), 9).append(type == SqlType.Scalar.JSON ? " -> " : " ->> ")
                .append(stringLit(g.field())).append(")");
    }

    /** DuckDB's struct_insert: the struct with the field set (a new one last). */
    @Override
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> args) {
        if (!(args.get(1) instanceof SqlExpr.StringLit name)) {
            throw new DialectCapability("struct_insert of a field that is not a literal reached SQLite");
        }
        return writer.append("json_set(").expr(args.get(0), 0).append(", ")
                .append(stringLit("$.\"" + name.value().replace("\"", "\\\"") + "\"")).append(", ")
                .piece(json(args.get(2))).append(")");
    }

    /** exists([]) is false: Pure's empty-collection semantics. */
    @Override
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "max", "0", SqlFn.LIST_EXISTS);
    }

    /** forAll([]) is true. */
    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "min", "1", SqlFn.LIST_FOR_ALL);
    }

    private SqlWriter listPredicate(SqlWriter writer, List<SqlExpr> args, String agg, String empty, SqlFn fn) {
        SqlExpr.Lambda l = lambdaOf(args.get(1), fn);
        SqlExpr body = lambdaBody(l, listElement(args.get(0)));
        return writer.append("coalesce((SELECT " + agg + "(CASE WHEN ").expr(body, 0).append(" THEN 1 WHEN NOT (")
                .expr(body, 0).append(") THEN 0 END) FROM ")
                .piece(elements(args.get(0), l.params().get(0), offsetOf(l), indexOf(l))).append("), " + empty + ")");
    }

    /** No duplicates iff the distinct count is the count (a NULL element is no distinct value, as DuckDB's
     *  len(list_distinct(x)) = len(x)); an empty or NULL list is distinct. */
    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("coalesce((SELECT count(*) = count(DISTINCT value) FROM json_each(").expr(args.get(0), 0)
                .append(")), 1)");
    }

    /** DuckDB's list_contains: NULL for a NULL list or needle; else found or not -- a NULL element never makes an
     *  absent needle NULL. A nested value compares as its JSON text. */
    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        if (bindable(m.collection()) || bindable(m.needle())) {
            return writer.piece(over(List.of(m.needle(), m.collection()),
                    r -> w -> membership(w, new SqlExpr.Membership(r.get(0), r.get(1)))));
        }
        SqlType el = listElement(m.collection());
        String e = param("__e");
        SqlWriter.Piece needle = nested(el) ? w -> w.append("json(").expr(m.needle(), 0).append(")")
                : w -> w.expr(m.needle(), 5);
        return writer.append("(CASE WHEN ").expr(m.collection(), 4).append(" IS NULL OR ").expr(m.needle(), 4)
                .append(" IS NULL THEN NULL ELSE EXISTS(SELECT 1 FROM ").piece(elements(m.collection(), "__e", "__eo",
                        null)).append(" WHERE " + (nested(el) ? "json(" + e + ")" : e) + " = ").piece(needle)
                .append(") END)");
    }

    @Override
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        return writer.piece(overRows(rc.collection(), rc.reducer(), rc.extras()));
    }

    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("an unnest in a select list reached SQLite, which has no set-returning"
                + " projection (an explode is placed in FROM as a json_each)");
    }

    /**
     * Pure's fold, {@code (element, acc)}, as a correlated {@code WITH RECURSIVE} over the elements in order (measured
     * to correlate inside a scalar subquery): the accumulator starts at the init, each step applies the body to the
     * next element; the last row's accumulator is the fold. An empty or NULL list folds to the init.
     */
    @Override
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        SqlExpr.Lambda l = f.lambda();
        String x = l.params().get(0);
        String acc = l.params().get(1);
        SqlExpr body = lambdaBody(l, listElement(f.source()));
        return writer.append("(WITH RECURSIVE __f(__i, ").append(param(acc)).append(") AS (SELECT 0, ")
                .expr(f.init(), 0).append(" UNION ALL SELECT __f.__i + 1, ").expr(body, 0)
                .append(" FROM __f JOIN (SELECT key AS __k, " + elementRead(listElement(f.source())) + " AS ")
                .append(param(x)).append(" FROM json_each(").expr(f.source(), 0)
                .append(")) AS __x ON __x.__k = __f.__i) SELECT ").append(param(acc))
                .append(" FROM __f ORDER BY __i DESC LIMIT 1)");
    }

    /** DuckDB's list_reduce: the first element starts the accumulator, the lambda (acc, x) runs over the rest; an empty
     *  list reduces to NULL here (DuckDB raises). */
    private SqlWriter listReduce(SqlWriter writer, List<SqlExpr> a) {
        if (!(a.get(1) instanceof SqlExpr.Lambda l) || l.params().size() != 2) {
            throw wall(SqlFn.LIST_REDUCE, "a reduction lambda of another shape");
        }
        String acc = l.params().get(0);
        String x = l.params().get(1);
        SqlExpr body = lambdaBody(new SqlExpr.Lambda(List.of(x, acc), l.body()), listElement(a.get(0)));
        return writer.append("(WITH RECURSIVE __r(__i, ").append(param(acc)).append(") AS (SELECT 1, ")
                .piece(elementAt(a.get(0), w -> w.append("0"))).append(" UNION ALL SELECT __r.__i + 1, ")
                .expr(body, 0).append(" FROM __r JOIN (SELECT key AS __k, " + elementRead(listElement(a.get(0)))
                        + " AS ").append(param(x)).append(" FROM json_each(").expr(a.get(0), 0)
                .append(")) AS __x ON __x.__k = __r.__i) SELECT ").append(param(acc))
                .append(" FROM __r ORDER BY __i DESC LIMIT 1)");
    }

    /** Pure's splitPart: the non-empty tokens, 1-based, NULL past the end. */
    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("(CASE WHEN ").expr(a.get(2), 4).append(" > 0 THEN (SELECT value FROM json_each(")
                .piece(split(a.get(0), a.get(1))).append(") WHERE value <> '' ORDER BY key LIMIT 1 OFFSET ")
                .expr(a.get(2), 6).append(" - 1) END)");
    }

    /**
     * DuckDB's string_split as a JSON array of the parts: a correlated recursive walk over the text (the empty
     * delimiter splits into characters; the empty text is no parts, as DuckDB's, measured on BigQuery's lane).
     */
    private SqlWriter.Piece split(SqlExpr s, SqlExpr d) {
        return w -> w.append("(CASE WHEN ").expr(s, 4).append(" IS NULL OR ").expr(d, 4)
                .append(" IS NULL THEN NULL WHEN ").expr(s, 4).append(" = '' THEN json_array() WHEN ").expr(d, 4)
                .append(" = '' THEN (WITH RECURSIVE __s(__n, __p, __r) AS (SELECT 0, NULL, ").expr(s, 0)
                .append(" UNION ALL SELECT __n + 1, substr(__r, 1, 1), substr(__r, 2) FROM __s WHERE __r <> '')")
                .append(" SELECT json_group_array(__p ORDER BY __n) FROM __s WHERE __n > 0)")
                .append(" ELSE (WITH RECURSIVE __s(__n, __p, __r) AS (SELECT 0, NULL, ").expr(s, 6).append(" || ")
                .expr(d, 6).append(" UNION ALL SELECT __n + 1, substr(__r, 1, instr(__r, ").expr(d, 0)
                .append(") - 1), substr(__r, instr(__r, ").expr(d, 0).append(") + length(").expr(d, 0)
                .append(")) FROM __s WHERE __r <> '') SELECT json_group_array(__p ORDER BY __n) FROM __s")
                .append(" WHERE __n > 0) END)");
    }

    // ==================================================================
    // Variant: JSON text
    // ==================================================================

    private static boolean isJson(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.JSON;
    }

    private static boolean isText(SqlExpr e) {
        return e instanceof SqlExpr.StringLit || typeOf(e) == SqlType.Scalar.VARCHAR;
    }

    private static boolean isDouble(SqlExpr e) {
        return e instanceof SqlExpr.FloatLit || typeOf(e) == SqlType.Scalar.DOUBLE;
    }

    private static boolean isInteger(SqlExpr e) {
        SqlType t = typeOf(e);
        return e instanceof SqlExpr.IntLit || t == SqlType.Scalar.INTEGER || t == SqlType.Scalar.BIGINT
                || t == SqlType.Scalar.HUGEINT;
    }

    /** A value as a Variant's JSON text: a string quoted, a boolean true or false, a nested value its JSON. */
    private SqlWriter.Piece variantOf(SqlExpr x) {
        SqlType t = typeOf(x);
        if (t == SqlType.Scalar.JSON) {
            return w -> w.expr(x, 0);
        }
        if (nested(t) || x instanceof SqlExpr.ArrayLit || x instanceof SqlExpr.StructLit) {
            return w -> w.append("json(").expr(x, 0).append(")");
        }
        if (t == SqlType.Scalar.BOOLEAN || x instanceof SqlExpr.BoolLit) {
            return w -> w.append("(CASE WHEN ").expr(x, 0).append(" THEN 'true' WHEN NOT (").expr(x, 0)
                    .append(") THEN 'false' END)");
        }
        return w -> w.append("json_quote(").expr(x, 0).append(")");
    }

    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() != 1) {
            throw new DialectCapability("toVariant of " + a.size() + " arguments reached SQLite");
        }
        return writer.piece(variantOf(a.get(0)));
    }

    /** A member by key, or a 0-based index (negative from the end; past either end NULL): {@code ->}, JSON text. */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        SqlExpr key = args.get(1);
        if (key instanceof SqlExpr.StringLit root && "$".equals(root.value())) {
            return writer.expr(args.get(0), 0);
        }
        if (isText(key)) {
            return writer.append("(").expr(args.get(0), 9).append(" -> ('$.\"' || replace(").expr(key, 0)
                    .append(", '\"', '\\\"') || '\"'))");
        }
        return writer.append("(").expr(args.get(0), 9).append(" -> (CASE WHEN ").expr(key, 4)
                .append(" < 0 THEN '$[#' || ").expr(key, 6).append(" || ']' ELSE '$[' || ").expr(key, 6)
                .append(" || ']' END))");
    }

    /** A Variant array's elements, each a Variant: the array's JSON is the list (NULL for a non-array). */
    @Override
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("(CASE WHEN json_type(").expr(args.get(0), 0).append(") = 'array' THEN ")
                .expr(args.get(0), 0).append(" END)");
    }

    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        SqlType target = c.target();
        SqlExpr v = c.value();
        if (target == SqlType.Scalar.TEMPORAL_TEXT || target == SqlType.Scalar.DECIMAL_TEXT) {
            return writer.expr(v, 0);
        }
        if (v instanceof SqlExpr.NullLit) {
            return writer.append("NULL");
        }
        if (target == SqlType.Scalar.JSON) {
            return isJson(v) ? writer.expr(v, 0)
                    : isText(v) ? writer.append("json(").expr(v, 0).append(")") : writer.piece(variantOf(v));
        }
        if (isJson(v)) {
            return writer.piece(fromVariant(v, target));
        }
        if (target instanceof SqlType.Array arr) {
            SqlType from = listElement(v);
            // a text element as LITERAL is the text itself, as the scalar cast below writes it (LITERAL is VARCHAR
            // physically): the mixed sort's ordered ids (2026-10-13, PR #34's review: element-wise, it wrote the
            // aggregate twice)
            if (from == null || from.equals(arr.element())
                    || from == SqlType.Scalar.VARCHAR && arr.element() == SqlType.Scalar.LITERAL) {
                return writer.expr(v, 0);
            }
            // element-wise
            SqlExpr el = SqlExpr.Column.param("__c", v);
            return writer.piece(overElements(v, "__c", "__co", null,
                    jsonOf(castTo(w -> w.append(param("__c")), from, arr.element()), arr.element()), null));
        }
        if (target instanceof SqlType.Struct || target instanceof SqlType.Map) {
            return writer.expr(v, 0);
        }
        if (target == SqlType.Scalar.VARCHAR || target == SqlType.Scalar.LITERAL) {
            return writer.piece(asText(v));
        }
        return writer.piece(castTo(w -> w.expr(v, 0), typeOf(v), target));
    }

    /** A Variant's value as {@code to}: a scalar by its SQL value ({@code ->> '$'}: a string unquoted, a boolean 1 or
     *  0), as text a string's text and anything else's JSON; a list re-held element by element. */
    private SqlWriter.Piece fromVariant(SqlExpr v, SqlType to) {
        // a member read as text is its scalar text (a string unquoted); a whole value as text is its JSON (DuckDB's
        // CAST(json AS VARCHAR) keeps a string's quotes)
        boolean member = v instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET;
        if ((to == SqlType.Scalar.VARCHAR || to == SqlType.Scalar.LITERAL) && !member) {
            return w -> w.expr(v, 0);
        }
        if (to == SqlType.Scalar.VARCHAR || to == SqlType.Scalar.LITERAL) {
            return w -> w.append("(CASE WHEN json_type(").expr(v, 0).append(") = 'text' THEN (").expr(v, 9)
                    .append(" ->> '$') ELSE ").expr(v, 0).append(" END)");
        }
        if (to == SqlType.Scalar.BOOLEAN) {
            // JSON true and false read as 1 and 0 by ->> (measured); a string by its text
            return w -> w.append("(CASE json_type(").expr(v, 0).append(") WHEN 'true' THEN 1 WHEN 'false' THEN 0")
                    .append(" WHEN 'text' THEN CASE lower(").expr(v, 9).append(" ->> '$') WHEN 'true' THEN 1 WHEN")
                    .append(" 'false' THEN 0 END WHEN 'integer' THEN (").expr(v, 9).append(" ->> '$') <> 0 END)");
        }
        if (to instanceof SqlType.Array arr) {
            SqlWriter.Piece each = arr.element() == SqlType.Scalar.JSON ? w -> w.append(param("__j"))
                    : castTo(w -> w.append("(" + param("__j") + " ->> '$')"), SqlType.Scalar.JSON, arr.element());
            return w -> w.append("(CASE WHEN json_type(").expr(v, 0).append(") = 'array' THEN (SELECT ")
                    .append("json_group_array(").piece(jsonOf(each, arr.element())).append(" ORDER BY ")
                    .append(param("__jo")).append(") FROM (SELECT " + elementRead(SqlType.Scalar.JSON) + " AS ")
                    .append(param("__j")).append(", key AS ").append(param("__jo")).append(" FROM json_each(")
                    .expr(v, 0).append("))) END)");
        }
        if (to instanceof SqlType.Map || to instanceof SqlType.Struct) {
            return w -> w.expr(v, 0);
        }
        return castTo(w -> w.append("(").expr(v, 9).append(" ->> '$')"), SqlType.Scalar.JSON, to);
    }

    /** {@code value} (of type {@code from}, when known) cast to {@code to}, each cast decided here: a type name casts
     *  by its affinity only (CAST('2024-01-01' AS DATE) is 2024, measured). */
    private SqlWriter.Piece castTo(SqlWriter.Piece value, @com.legend.base.Nullable SqlType from, SqlType to) {
        if (to instanceof SqlType.Decimal d) {
            // DuckDB's decimal cast rounds to the scale, half away from zero (as round does)
            return w -> w.append("round(CAST(").piece(value).append(" AS REAL), " + d.scale() + ")");
        }
        if (!(to instanceof SqlType.Scalar s)) {
            return value;
        }
        return switch (s) {
            case VARCHAR, LITERAL -> textOf(value, from);
            case BOOLEAN -> from == SqlType.Scalar.VARCHAR || from == SqlType.Scalar.JSON
                    ? w -> w.append("(CASE lower(").piece(value).append(") WHEN 'true' THEN 1 WHEN 'false' THEN 0 END)")
                    : w -> w.append("(").piece(value).append(" <> 0)");
            // DuckDB rounds a double to an integer, half away from zero (SQLite's CAST truncates)
            case INTEGER, BIGINT, HUGEINT -> from == SqlType.Scalar.DOUBLE || from instanceof SqlType.Decimal
                    ? w -> w.append("CAST(round(").piece(value).append(") AS INTEGER)")
                    : w -> w.append("CAST(").piece(value).append(" AS INTEGER)");
            case DOUBLE -> w -> w.append("CAST(").piece(value).append(" AS REAL)");
            case DATE -> w -> w.append("date(").piece(value).append(")");
            case TIMESTAMP -> from == SqlType.Scalar.DATE
                    ? w -> w.append("(").piece(value).append(" || ' 00:00:00')")
                    : from == SqlType.Scalar.VARCHAR ? padded(dateTimeText(value)) : value;
            // a zoned date-time is held as its naive UTC text: its offset applied at the cast
            case TIMESTAMPTZ -> from == SqlType.Scalar.DATE
                    ? w -> w.append("(").piece(value).append(" || ' 00:00:00')")
                    : from == SqlType.Scalar.VARCHAR ? w -> w.append("(SELECT ").piece(utcOf(w2 -> w2.append("__u.t")))
                            .append(" FROM (SELECT ").piece(padded(dateTimeText(value))).append(" AS t) AS __u)")
                    : value;
            case JSON -> w -> w.append("json_quote(").piece(value).append(")");
            // a time of day is its text: a date-time text's from its twelfth character, a date's midnight
            case TIME -> from == SqlType.Scalar.TIMESTAMP || from == SqlType.Scalar.TIMESTAMPTZ
                    ? w -> w.append("substr(").piece(value).append(", 12)")
                    : from == SqlType.Scalar.DATE ? w -> w.append("'00:00:00'") : value;
            case TEMPORAL_TEXT, DECIMAL_TEXT -> value;
        };
    }

    /** A value as text, as DuckDB's CAST to VARCHAR prints it. */
    private SqlWriter.Piece asText(SqlExpr e) {
        if (e instanceof SqlExpr.StringLit) {
            return w -> w.expr(e, 0);
        }
        // a decimal literal's text is its digits as written (DuckDB's DECIMAL keeps its scale: 2.50D is '2.50')
        if (e instanceof SqlExpr.DecimalLit d) {
            return w -> w.append(stringLit(d.value().toPlainString()));
        }
        SqlType t = e instanceof SqlExpr.FloatLit ? SqlType.Scalar.DOUBLE
                : e instanceof SqlExpr.BoolLit ? SqlType.Scalar.BOOLEAN : typeOf(e);
        return textOf(w -> w.expr(e, 0), t);
    }

    /** DuckDB's text of a value of {@code type}: a Float (and a decimal, a REAL here) its shortest round-trip digits
     *  ({@code CAST AS TEXT} keeps 15, measured: 0.1 + 0.2 is '0.3'; the {@code !} flag gives 16 and 17, measured), a
     *  whole one with {@code .0}; a boolean true or false; a date-time and a nested value their text as held; anything
     *  else its CAST. */
    private SqlWriter.Piece textOf(SqlWriter.Piece value, @com.legend.base.Nullable SqlType type) {
        if (type == SqlType.Scalar.DOUBLE) {
            // the value bound once (its text is tried at three precisions)
            return w -> w.append("(SELECT " + SHORTEST + " FROM (SELECT ").piece(value).append(" AS v) AS __t)");
        }
        if (type == SqlType.Scalar.BOOLEAN) {
            return w -> w.append("(CASE WHEN ").piece(value).append(" THEN 'true' WHEN NOT (").piece(value)
                    .append(") THEN 'false' END)");
        }
        if (type instanceof SqlType.Decimal d && d.scale() > 0) {
            // DuckDB prints a DECIMAL with its scale's digits (the REAL here rounded to them)
            return w -> w.append("printf('%." + d.scale() + "f', ").piece(value).append(")");
        }
        if (type instanceof SqlType.Decimal) {
            // a scale-0 decimal is an INTEGER or a REAL here: its text the value's own digits -- an integer's as one,
            // a REAL's shortest (the plan's scale-0 fact is not the value's: a Number column's is its integers')
            return w -> w.append("(SELECT CASE typeof(__t.v) WHEN 'integer' THEN CAST(__t.v AS TEXT) ELSE " + SHORTEST
                    + " END FROM (SELECT ").piece(value).append(" AS v) AS __t)");
        }
        return w -> w.append("CAST(").piece(value).append(" AS TEXT)");
    }

    /** A double's shortest round-trip text, of {@code __t.v}: 15 digits, else 16, else 17 ({@code !} keeps a whole
     *  value's {@code .0}, measured); NULL's is NULL (printf reads a NULL as 0 and wrote '0.0': measured 2026-10-11 by
     *  channel B on SQLite, an empty max's text). */
    private static final String SHORTEST = "CASE WHEN __t.v IS NULL THEN NULL"
            + " WHEN CAST(printf('%!.15g', __t.v) AS REAL) = __t.v THEN"
            + " printf('%!.15g', __t.v) WHEN CAST(printf('%!.16g', __t.v) AS REAL) = __t.v THEN printf('%!.16g', __t.v)"
            + " ELSE printf('%!.17g', __t.v) END";

    // ==================================================================
    // Scalar functions -- every SqlFn decided here
    // ==================================================================

    /** The calls whose spelling writes an operand more than once (a CASE testing it, a bound read twice). */
    private static final Set<SqlFn> REPEATING = Set.of(SqlFn.LPAD, SqlFn.RPAD, SqlFn.LEFT, SqlFn.RIGHT,
            SqlFn.STARTS_WITH, SqlFn.ENDS_WITH, SqlFn.SPLIT, SqlFn.SPLIT_PART, SqlFn.PURE_SPLIT_PART, SqlFn.PARSE_INT,
            SqlFn.ROUND, SqlFn.ROUND_HALF_UP, SqlFn.GREATEST, SqlFn.LEAST, SqlFn.MOD, SqlFn.REM, SqlFn.CBRT,
            SqlFn.ASCII_CODE, SqlFn.ACOS, SqlFn.ASIN, SqlFn.BOOL_TO_TEXT, SqlFn.LIST_GET, SqlFn.LIST_SLICE,
            SqlFn.LIST_INIT, SqlFn.LIST_TAIL, SqlFn.LIST_DISTINCT, SqlFn.LIST_SORT, SqlFn.LIST_SORT_DESC,
            SqlFn.LIST_REVERSE, SqlFn.LIST_FLATTEN, SqlFn.LIST_FILTER, SqlFn.LIST_TRANSFORM, SqlFn.LIST_REDUCE,
            SqlFn.MAP_KEYS, SqlFn.MAP_VALUES, SqlFn.MAP_EXTRACT, SqlFn.EPOCH_SECONDS, SqlFn.EPOCH_MS,
            SqlFn.ADD_INTERVAL, SqlFn.ADD_INTERVAL_TEMPORAL, SqlFn.DATE_DIFF, SqlFn.TIME_BUCKET, SqlFn.VARIANT_GET,
            SqlFn.MAKE_TIMESTAMP);

    /** Fresh names for bound operands, unique within a statement. */
    private final int[] binds = {0};

    /**
     * {@code body} over {@code operands}: each large one (a dozen nodes or more, with no lambda, aggregate or window --
     * an aggregate is its select's own) bound ONCE as a column of a one-row derived table under a scalar subquery, so a
     * spelling that writes it several times writes its name: a statement is capped at 1,000,000 bytes here (measured
     * "statement too long"), and nested spellings that each repeat their operand grow as the product of the repeats.
     */
    private SqlWriter.Piece over(List<SqlExpr> operands,
            java.util.function.Function<List<SqlExpr>, SqlWriter.Piece> body) {
        String alias = "__b" + binds[0]++;
        List<SqlExpr> refs = new ArrayList<>(operands.size());
        List<Integer> bound = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            SqlExpr x = operands.get(i);
            if (bindable(x)) {
                SqlType t = typeOf(x);
                refs.add(t == null ? SqlExpr.Column.derived(alias, "__a" + i)
                        : SqlExpr.Column.of(alias, "__a" + i, t, true, com.legend.sql.OutputCol.Origin.DERIVED));
                bound.add(i);
            } else {
                refs.add(x);
            }
        }
        SqlWriter.Piece b = body.apply(refs);
        if (bound.isEmpty()) {
            return b;
        }
        return w -> {
            w.append("(SELECT ").piece(b).append(" FROM (SELECT ");
            for (int k = 0; k < bound.size(); k++) {
                int i = bound.get(k);
                w.append(k > 0 ? ", " : "").expr(operands.get(i), 0).append(" AS __a" + i);
            }
            w.append(") AS " + alias + ")");
        };
    }

    private static boolean bindable(SqlExpr x) {
        return !(x instanceof SqlExpr.Lambda) && !(x instanceof SqlExpr.Column) && expandedSize(x, 12) >= 12
                && !grouped(x);
    }

    /** {@code e}'s size as written, counted up to {@code cap}. */
    private static int expandedSize(SqlExpr e, int cap) {
        int n = 1;
        for (SqlExpr k : e.children()) {
            if (n >= cap) {
                return n;
            }
            n += expandedSize(k, cap - n);
        }
        return n;
    }

    private static boolean grouped(SqlExpr e) {
        if (e instanceof SqlAgg || e instanceof SqlExpr.WindowCall || e instanceof SqlExpr.Lambda) {
            return true;
        }
        for (SqlExpr k : e.children()) {
            if (grouped(k)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        if (REPEATING.contains(c.fn()) && c.args().stream().anyMatch(SQLite::bindable)) {
            return writer.piece(over(c.args(), refs -> w -> calls(w, (SqlExpr.Call) c.withChildren(refs), 0)));
        }
        return calls(writer, c, parentPrec);
    }

    private SqlWriter calls(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            // ---- each function as this dialect spells it, by name (the dialect program's Phase 1, D1, 2026-10-13:
            // an exhaustive switch; a spelling several databases share is a named helper of the base)
            // the ANSI operators
            case AND, OR, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES -> infix(writer, c, parentPrec);
            case EQUAL, NOT_EQUAL -> equalArm(writer, c, parentPrec);
            // the shared spellings (the base's named helpers)
            case NOT -> notPrefix(writer, c.args(), parentPrec);
            case IS_NULL -> isNullTest(writer, c.args());
            case IS_NOT_NULL -> isNotNullTest(writer, c.args());
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> distinctFrom(writer, c.args());
            case NULL_SAFE_EQUAL -> notDistinctFrom(writer, c.args());
            case XOR -> xorAsAndOr(writer, c.args(), parentPrec);
            case UC_FIRST -> firstCharCased(writer, "upper", c.args(), parentPrec);
            case LC_FIRST -> firstCharCased(writer, "lower", c.args(), parentPrec);
            case CEILING -> bigintOf(writer, "ceil", c.args());
            case FLOOR -> bigintOf(writer, "floor", c.args());
            case SIGN -> bigintOf(writer, "sign", c.args());
            case PI -> writer.append("pi()");
            case FLOAT_CANON -> floatCanonByText(writer, c.args(), parentPrec);
            // the hooks: this dialect's own (or the base's refusal)
            case HASH -> hashSigned(writer, c.args());
            case ROUND -> roundHalfEven(writer, c.args());
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), c.args());
            case STRUCT_INSERT -> structInsert(writer, c.args());
            case UNNEST -> unnestProjection(writer, c.args());
            case PURE_SPLIT_PART -> splitPartCall(writer, c.args());
            case LIST_EXISTS -> listExists(writer, c.args());
            case LIST_FOR_ALL -> listForAll(writer, c.args());
            case ALL_DISTINCT -> allDistinct(writer, c.args());
            case TO_VARIANT -> variantConstruct(writer, c.args());
            case VARIANT_GET -> variantGet(writer, c.args());
            case VARIANT_ELEMENTS -> variantElements(writer, c.args());
            // this database's plain spellings: name(args)
            case ABS -> writer.function("abs", c.args());
            case ATAN -> writer.function("atan", c.args());
            case ATAN2 -> writer.function("atan2", c.args());
            case COS -> writer.function("cos", c.args());
            case COSH -> writer.function("cosh", c.args());
            case COT -> writer.function("cot", c.args());
            case SIN -> writer.function("sin", c.args());
            case SINH -> writer.function("sinh", c.args());
            case TAN -> writer.function("tan", c.args());
            case TANH -> writer.function("tanh", c.args());
            case DEGREES -> writer.function("degrees", c.args());
            case RADIANS -> writer.function("radians", c.args());
            case EXP -> writer.function("exp", c.args());
            case LN -> writer.function("ln", c.args());
            case LOG10 -> writer.function("log10", c.args());
            case SQRT -> writer.function("sqrt", c.args());
            case POW -> writer.function("pow", c.args());
            case LOWER -> writer.function("lower", c.args());
            case UPPER -> writer.function("upper", c.args());
            case LTRIM -> writer.function("ltrim", c.args());
            case RTRIM -> writer.function("rtrim", c.args());
            case TRIM -> writer.function("trim", c.args());
            case REPLACE -> writer.function("replace", c.args());
            case TRANSLATE -> throw wall(c.fn(), "SQLite has no translate function");
            case REVERSE_STRING -> writer.function("reverse", c.args());
            case CHR -> writer.function("char", c.args());
            case FLOOR_RAW -> writer.function("floor", c.args());
            case SUBSTRING -> writer.function("substr", c.args());
            case STRPOS -> writer.function("instr", c.args());
            case LIST_LENGTH, JSON_ARRAY_LENGTH -> writer.function("json_array_length", c.args());
            case IN -> in(writer, c, parentPrec);
            // DuckDB's ascii('') is 0 (unicode('') is NULL here, measured)
            case ASCII_CODE -> writer.append("(CASE WHEN ").expr(a.get(0), 4)
                    .append(" IS NULL THEN NULL ELSE coalesce(")
                    .append("unicode(").expr(a.get(0), 0).append("), 0) END)");
            // outside [-1, 1] SQLite answers NULL (measured), DuckDB raises
            case ACOS -> domainGuarded(writer, c, "acos");
            case ASIN -> domainGuarded(writer, c, "asin");
            case COALESCE -> a.size() == 1 ? writer.expr(a.get(0), parentPrec) : writer.function("coalesce", a);
            // never "--x": that is a comment
            case NEGATE -> writer.append("-(").expr(a.get(0), 0).append(")");
            case ERROR -> writer.piece(raise(a));

            // ---- arithmetic
            // Pure's divide is a DOUBLE division; x / 0 is NULL here (DuckDB's answers +-Infinity, measured)
            case DIVIDE -> writer.append("(CAST(").expr(a.get(0), 0).append(" AS REAL) / CAST(").expr(a.get(1), 0)
                    .append(" AS REAL))");
            case MOD, REM -> writer.piece(remainder(c.fn(), a));
            // DuckDB's // truncates toward zero, NULL for a zero divisor: SQLite's integer division
            case INT_DIVIDE -> intDivide(writer, c);
            case ROUND_HALF_UP -> roundHalfUp(writer, a, c);
            case CBRT -> writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" < 0 THEN -pow(-(").expr(a.get(0), 0)
                    .append("), 1.0 / 3) ELSE pow(").expr(a.get(0), 0).append(", 1.0 / 3) END)");
            case BIT_NOT -> writer.append("(~ ").expr(a.get(0), 7).append(")");
            case PARSE_INT -> writer.piece(parseInt(a.get(0)));
            case BOOL_TO_TEXT -> writer.piece(textOf(w -> w.expr(a.get(0), 0), SqlType.Scalar.BOOLEAN));
            // NULL-skipping, as DuckDB's (SQLite's max/min of several are NULL for any NULL argument)
            case GREATEST -> writer.piece(nullSkipping("max", a));
            case LEAST -> writer.piece(nullSkipping("min", a));

            // ---- strings
            // NULL-skipping, as DuckDB's concat (SQLite's concat, 3.44)
            case CONCAT -> writer.append("concat(").join(flattenConcat(a), ", ", (w, x) -> w.piece(asText(x)))
                    .append(")");
            case CONCAT_JOIN -> writer.append("concat(").join(a, ", ", (w, x) -> w.piece(asText(x))).append(")");
            case LENGTH -> writer.append("length(").piece(asText(a.get(0))).append(")");
            case LEFT, RIGHT -> writer.piece(leftRight(c.fn(), a));
            case LPAD, RPAD -> writer.piece(pad(c.fn(), a));
            case REPEAT_STR -> writer.append("replace(hex(zeroblob(max(").expr(a.get(1), 0).append(", 0))), '00', ")
                    .expr(a.get(0), 0).append(")");
            case STARTS_WITH -> writer.append("(substr(").expr(a.get(0), 0).append(", 1, length(").expr(a.get(1), 0)
                    .append(")) = ").expr(a.get(1), 5).append(")");
            case ENDS_WITH -> writer.append("(substr(").expr(a.get(0), 0).append(", length(").expr(a.get(0), 0)
                    .append(") - length(").expr(a.get(1), 0).append(") + 1) = ").expr(a.get(1), 5).append(")");
            case SPLIT -> writer.piece(split(a.get(0), a.get(1)));
            // DuckDB's split_part: 1-based, negative from the end, out of range ''
            case SPLIT_PART -> writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL OR ").expr(a.get(1), 4)
                    .append(" IS NULL THEN NULL ELSE coalesce(json_extract(").piece(split(a.get(0), a.get(1)))
                    .append(", CASE WHEN ").expr(a.get(2), 4).append(" < 0 THEN '$[#' || ").expr(a.get(2), 6)
                    .append(" || ']' ELSE '$[' || (").expr(a.get(2), 6).append(" - 1) || ']' END), '') END)");
            // SQLite has no regular-expression function: a literal pattern's NFA is walked by the database
            // (SqliteRegex); a full match anchors both ends, MATCHES searches
            case MATCHES, REGEXP_FULL_MATCH -> matchesArm(writer, c);
            // where a literal pattern matches: its leftmost match found by the same walk (SqliteRegex.find)
            case REGEXP_EXTRACT -> writer.piece(regexpExtract(a));
            case REGEXP_EXTRACT_ALL -> writer.piece(regexpExtractAll(a));
            case REGEXP_REPLACE -> writer.piece(regexpReplace(a));
            case ENCODE_BASE64 -> writer.piece(base64Encoded(a.get(0)));
            case DECODE_BASE64 -> writer.piece(base64Decoded(a.get(0)));
            case MD5, SHA1, SHA256 -> throw wall(c.fn(), "SQLite has no digest function");
            case LEVENSHTEIN -> writer.piece(levenshtein(a.get(0), a.get(1)));
            case JARO_WINKLER -> throw wall(c.fn(), "SQLite has no string-distance function");
            // random() is a signed 64-bit integer: its low 53 bits over 2^53, every value a double exactly, in
            // [0, 1) (shifted by 2^63 as a REAL and scaled, a draw within 1024 of the top rounded to 1.0: measured,
            // the independent review, 2026-10-18)
            case RANDOM -> writer.append("((random() & 9007199254740991) / 9007199254740992.0)");
            case GUID -> writer.append("lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' ||"
                    + " substr(hex(randomblob(2)), 2) || '-' || substr('89ab', 1 + (abs(random()) % 4), 1) ||"
                    + " substr(hex(randomblob(2)), 2) || '-' || hex(randomblob(6)))");
            case FORMAT -> format(writer, a);
            case CURRENT_USER_FN -> throw wall(c.fn(), "SQLite has no users");

            // ---- temporal
            case TODAY -> writer.append("date('now')");
            case NOW -> writer.append("strftime('%Y-%m-%d %H:%M:%f', 'now')");
            case DATE_TRUNC_DAY -> writer.append("date(").expr(a.get(0), 0).append(")");
            case PARSE_DATE -> writer.piece(parsedDate(a.get(0)));
            case EXTRACT -> writer.piece(extract(a));
            case DATE_TRUNC -> writer.piece(dateTrunc(a));
            case MAKE_DATE -> writer.append("printf('%04d-%02d-%02d', ").list(a).append(")");
            case MAKE_TIMESTAMP -> makeTimestamp(writer, a, c);
            // past SQLite's years 0000-9999 a date function answers NULL (measured): raised, never a NULL value
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> writer.append("(CASE WHEN ").expr(a.get(1), 4)
                    .append(" IS NULL OR ").expr(a.get(2), 4).append(" IS NULL THEN NULL ELSE coalesce(")
                    .piece(addInterval(a, c)).append(", ").piece(raise(List.of(new SqlExpr.StringLit(
                            "a date past the years 0000-9999 has no SQLite value")))).append(") END)");
            case DATE_DIFF -> writer.piece(dateDiff(a));
            case TIME_BUCKET -> writer.piece(timeBucket(a));
            // a zoned date-time is held as its naive UTC text: UTC's wall time is the instant's, either way
            case TIMEZONE -> timezone(writer, c, parentPrec);
            // a date's text, a space, a time's text: the date-time text SQLite's date functions read
            case DATE_AT_TIME -> op(writer, parentPrec, () -> writer.expr(a.get(0), 3).append(" || ' ' || ")
                    .expr(a.get(1), 3));
            case EPOCH_SECONDS -> writer.append("(").piece(micros(a.get(0))).append(" / 1000000.0)");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability("a nanosecond epoch: SQLite keeps"
                    + " microseconds (the Cypher island's nanosecond leg is DuckDB's)");
            case EPOCH_MS -> writer.append("(").piece(micros(a.get(0))).append(" / 1000)");
            case FROM_EPOCH_SECONDS -> writer.append("datetime(").expr(a.get(0), 0).append(", 'unixepoch')");
            case FROM_EPOCH_MS -> writer.append("strftime('%Y-%m-%d %H:%M:%f', ").expr(a.get(0), 6)
                    .append(" / 1000.0, 'unixepoch')");
            case DAYNAME -> writer.piece(dayName(a.get(0)));
            case MONTHNAME -> writer.piece(monthName(a.get(0), false));
            case STRFTIME -> strftimeArm(writer, c);
            case STRPTIME -> throw wall(c.fn(), "SQLite parses ISO text only");

            // ---- lists (JSON arrays; lambdas as correlated subqueries)
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION, LIST_DISTINCT, LIST_APPEND,
                 LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_BOOL_AND, LIST_BOOL_OR,
                 LIST_SORT, LIST_SORT_DESC, LIST_TAIL, LIST_INIT, LIST_SLICE, LIST_REVERSE, LIST_FLATTEN, LIST_ZIP,
                 LIST_PRODUCT, LIST_REDUCE, RANGE_FN, REPEAT_VALUE, TYPEOF -> lists(writer, c);

            // ---- maps: JSON objects (text keys)
            case MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT ->
                    maps(writer, c);

            // ---- variant
            // in DuckDB's json_type vocabulary, which the lowering compares against
            case JSON_TYPE -> writer.append("(CASE json_type(").expr(a.get(0), 0)
                    .append(") WHEN 'null' THEN 'NULL' WHEN 'text' THEN 'VARCHAR' WHEN 'true' THEN 'BOOLEAN'")
                    .append(" WHEN 'false' THEN 'BOOLEAN' WHEN 'integer' THEN 'BIGINT' WHEN 'real' THEN 'DOUBLE'")
                    .append(" WHEN 'array' THEN 'ARRAY' WHEN 'object' THEN 'OBJECT' END)");
            case JSON_PRETTY -> writer.append("json_pretty(").expr(a.get(0), 0).append(")");
            case JSON_MERGE_PATCH -> writer.append("json_patch(").list(a).append(")");
        };
    }

    /** The {@code ACOS} and {@code ASIN} arms of {@link #calls}: undefined outside [-1, 1], raised. */
    private SqlWriter domainGuarded(SqlWriter writer, SqlExpr.Call c, String fn) {
        List<SqlExpr> a = c.args();
        return writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL ELSE coalesce(").append(fn)
                .append("(").expr(a.get(0), 0).append("), ")
                .piece(raise(List.of(new SqlExpr.StringLit(c.fn() + " is undefined outside [-1,1]"))))
                .append(") END)");
    }

    /** The {@code EQUAL}, {@code NOT_EQUAL} arm of {@link #calls}. */
    private SqlWriter equalArm(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && (composite(a.get(0)) || composite(a.get(1)))) {
            return writer.append("(json(").expr(a.get(0), 0).append(c.fn() == SqlFn.EQUAL ? ") = json(" : ") <> json(")
                    .expr(a.get(1), 0).append("))");
        }
        return infix(writer, c, parentPrec);
    }

    /** The {@code IN} arm of {@link #calls}. */
    private SqlWriter in(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && a.get(1) instanceof SqlExpr.PlanParam p) {
            throw listParameter(p);
        }
        return inList(writer, a, parentPrec);
    }

    /** The {@code INT_DIVIDE} arm of {@link #calls}. */
    private SqlWriter intDivide(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.stream().anyMatch(SQLite::isDouble)) {
            throw wall(c.fn(), "an integer division of a Float");
        }
        return writer.append("(").expr(a.get(0), 6).append(" / ").expr(a.get(1), 7).append(")");
    }

    /** The {@code MATCHES}, {@code REGEXP_FULL_MATCH} arm of {@link #calls}. */
    private SqlWriter matchesArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.StringLit pattern) || a.size() > 2) {
            throw wall(c.fn(), "a pattern that is not a literal, or options beside it (SQLite matches a literal"
                    + " pattern only, as a table its query walks)");
        }
        return writer.piece(SqliteRegex.matches(w -> w.expr(a.get(0), 0), pattern.value(),
                c.fn() == SqlFn.REGEXP_FULL_MATCH));
    }

    /** The {@code TIMEZONE} arm of {@link #calls}. */
    private SqlWriter timezone(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (!(a.get(0) instanceof SqlExpr.StringLit zone) || !UTC_ZONES.contains(zone.value())) {
            throw wall(c.fn(), "SQLite has no time zones but UTC");
        }
        return writer.expr(a.get(1), parentPrec);
    }

    /** The {@code STRFTIME} arm of {@link #calls}. */
    private SqlWriter strftimeArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit fl)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        return writer.piece(strftime(a.get(0), fl));
    }

    private static boolean composite(SqlExpr e) {
        SqlType t = typeOf(e);
        return t instanceof SqlType.Struct || t instanceof SqlType.Array || t instanceof SqlType.Map;
    }

    /** A raised error: SQLite has no RAISE outside a trigger, and a bad JSON path is an error carrying its text
     *  ("bad JSON path: '...'", measured), raised only when evaluated (a CASE branch not taken raises nothing). The
     *  message travels between the U+001F sentinels, as every dialect's, a quote as U+001D
     *  (RaisedErrors.QUOTE_MARK). */
    private SqlWriter.Piece raise(List<SqlExpr> a) {
        return w -> {
            w.append("json_extract('{}', char(31) || ");
            if (a.size() > 1) {
                w.expr(a.get(1), 6).append(" || char(30) || ");
            }
            // the error text quotes the path SQL-style ('' for '): each quote travels as the envelope's quote mark
            w.append("replace(").expr(a.get(0), 0).append(", '''', char(29)) || char(31))");
        };
    }

    /** Pure's mod (the divisor's sign) and rem (the dividend's): {@code %} over integers, {@code mod()} over Floats (a
     *  REAL {@code %} truncates to integers, measured); a zero divisor is NULL, as DuckDB's. */
    private SqlWriter.Piece remainder(SqlFn fn, List<SqlExpr> a) {
        boolean dbl = !isInteger(a.get(0)) || !isInteger(a.get(1));
        SqlWriter.Piece rem = dbl ? w -> w.append("mod(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")")
                : w -> w.append("(").expr(a.get(0), 7).append(" % ").expr(a.get(1), 7).append(")");
        if (fn == SqlFn.REM) {
            return rem;
        }
        return dbl ? w -> w.append("mod(").piece(rem).append(" + ").expr(a.get(1), 6).append(", ").expr(a.get(1), 0)
                .append(")")
                : w -> w.append("((").piece(rem).append(" + ").expr(a.get(1), 6).append(") % ").expr(a.get(1), 7)
                        .append(")");
    }

    /** DuckDB's round: half away from zero; a double by DuckDB's own algorithm, {@code round(x * 10^n) / 10^n} (SQLite
     *  rounds 2.675's double, 2.67499..., to 2.67; DuckDB to 2.68), {@code round(x / 10^-n) * 10^-n} for a negative n.
     *  An integer result is an INTEGER (round answers a REAL). */
    private SqlWriter roundHalfUp(SqlWriter writer, List<SqlExpr> a, SqlExpr.Call c) {
        if (a.size() == 1) {
            boolean integral = typeOf(c) == SqlType.Scalar.BIGINT || typeOf(c) == SqlType.Scalar.INTEGER;
            return writer.append(integral ? "CAST(round(" : "round(").expr(a.get(0), 0)
                    .append(integral ? ") AS INTEGER)" : ")");
        }
        SqlWriter.Piece places = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS INTEGER)");
        SqlWriter.Piece up = w -> w.append("pow(10, ").piece(places).append(")");
        SqlWriter.Piece down = w -> w.append("pow(10, -").piece(places).append(")");
        return writer.append("(CASE WHEN ").piece(places).append(" >= 0 THEN round(").expr(a.get(0), 6).append(" * ")
                .piece(up).append(") / ").piece(up).append(" ELSE round(").expr(a.get(0), 6).append(" / ").piece(down)
                .append(") * ").piece(down).append(" END)");
    }

    /** DuckDB's 64-bit parse: an optional sign and digits, else an error (SQLite's CAST reads 'abc' as 0, measured). */
    private SqlWriter.Piece parseInt(SqlExpr s) {
        SqlWriter.Piece digits = w -> w.append("ltrim(trim(").expr(s, 0).append("), '+-')");
        return w -> w.append("(CASE WHEN ").expr(s, 4).append(" IS NULL THEN NULL WHEN ").piece(digits)
                .append(" <> '' AND ").piece(digits).append(" NOT GLOB '*[^0-9]*' AND length(trim(").expr(s, 0)
                .append(")) - length(").piece(digits).append(") <= 1 THEN CAST(trim(").expr(s, 0)
                .append(") AS INTEGER) ELSE ").piece(raise(List.of(SqlExpr.Call.of(SqlFn.CONCAT,
                        new SqlExpr.StringLit("Could not convert string '"), s,
                        new SqlExpr.StringLit("' to INT64"))))).append(" END)");
    }

    /** GREATEST/LEAST over the arguments, each replaced by the next non-NULL one when it is NULL: NULL only when every
     *  argument is. */
    private static SqlWriter.Piece nullSkipping(String fn, List<SqlExpr> a) {
        if (a.size() == 1) {
            return w -> w.expr(a.get(0), 0);
        }
        return w -> {
            w.append(fn).append("(");
            for (int i = 0; i < a.size(); i++) {
                w.append(i > 0 ? ", coalesce(" : "coalesce(").expr(a.get(i), 0);
                for (int j = 0; j < a.size(); j++) {
                    if (j != i) {
                        w.append(", ").expr(a.get(j), 0);
                    }
                }
                w.append(")");
            }
            w.append(")");
        };
    }

    /** DuckDB's left/right: a negative count is all but that many. */
    private static SqlWriter.Piece leftRight(SqlFn fn, List<SqlExpr> a) {
        if (fn == SqlFn.LEFT) {
            return w -> w.append("(CASE WHEN ").expr(a.get(1), 4).append(" < 0 THEN substr(").expr(a.get(0), 0)
                    .append(", 1, max(length(").expr(a.get(0), 0).append(") + ").expr(a.get(1), 6)
                    .append(", 0)) ELSE substr(").expr(a.get(0), 0).append(", 1, ").expr(a.get(1), 0).append(") END)");
        }
        return w -> w.append("(CASE WHEN ").expr(a.get(1), 4).append(" < 0 THEN substr(").expr(a.get(0), 0)
                .append(", 1 - ").expr(a.get(1), 7).append(") WHEN ").expr(a.get(1), 4)
                .append(" = 0 THEN '' ELSE substr(").expr(a.get(0), 0).append(", -").expr(a.get(1), 7)
                .append(") END)");
    }

    /** DuckDB's lpad/rpad: to the length, truncating a longer text; the pad (a space by default) repeated
     *  ({@code replace(hex(zeroblob(n)), '00', p)} is n copies of p). */
    private SqlWriter.Piece pad(SqlFn fn, List<SqlExpr> a) {
        SqlWriter.Piece p = a.size() > 2 ? w -> w.expr(a.get(2), 0) : w -> w.append("' '");
        SqlWriter.Piece fill = w -> w.append("substr(replace(hex(zeroblob(max(").expr(a.get(1), 0)
                .append(", 0))), '00', ").piece(p).append("), 1, ").expr(a.get(1), 6).append(" - length(")
                .expr(a.get(0), 0).append("))");
        return w -> {
            w.append("(CASE WHEN length(").expr(a.get(0), 0).append(") >= ").expr(a.get(1), 5)
                    .append(" THEN substr(").expr(a.get(0), 0).append(", 1, ").expr(a.get(1), 0).append(") ELSE ");
            if (fn == SqlFn.LPAD) {
                w.piece(fill).append(" || ").expr(a.get(0), 6);
            } else {
                w.expr(a.get(0), 6).append(" || ").piece(fill);
            }
            w.append(" END)");
        };
    }

    /** printf: a {@code %s} argument as its text (a boolean true or false, a Float its shortest digits). */
    private SqlWriter format(SqlWriter writer, List<SqlExpr> a) {
        if (!(a.get(0) instanceof SqlExpr.StringLit f)) {
            return writer.append("printf(").list(a).append(")");
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("%[-+ #0]*[0-9]*(?:\\.[0-9]+)?([a-zA-Z%])")
                .matcher(f.value());
        List<SqlWriter.Piece> args = new ArrayList<>();
        int arg = 1;
        while (m.find()) {
            char conversion = m.group(1).charAt(0);
            if (conversion == '%') {
                continue;
            }
            if (arg < a.size()) {
                SqlExpr x = a.get(arg);
                args.add(conversion == 's' ? asText(x) : w -> w.expr(x, 0));
            }
            arg++;
        }
        for (; arg < a.size(); arg++) {
            SqlExpr x = a.get(arg);
            args.add(w -> w.expr(x, 0));
        }
        writer.append("printf(").expr(a.get(0), 0);
        for (SqlWriter.Piece p : args) {
            writer.append(", ").piece(p);
        }
        return writer.append(")");
    }

    // ---- regular expressions: a literal pattern's matches found by the database (SqliteRegex)

    private static int group(List<SqlExpr> a) {
        if (a.size() < 3) {
            return 0;
        }
        if (a.get(2) instanceof SqlExpr.IntLit g && g.value() >= 0 && g.value() <= 99) {
            return (int) g.value();
        }
        throw new DialectCapability("a regexp group that is not a literal reached SQLite");
    }

    /** The JSON fields of a find's span of group {@code g}: its start's and end's. */
    private static String[] span(int g) {
        return g == 0 ? new String[] {"0", "1"} : new String[] {"2", "3"};
    }

    /** {@code text}'s part from the find {@code f}'s span ({@code from} an offset into it), '' for a group that took
     *  no part. */
    private static String part(String text, String f, String[] at, String from) {
        return "coalesce(substr(" + text + ", " + from + " + (" + f + " ->> " + at[0] + ") + 1, (" + f + " ->> "
                + at[1] + ") - (" + f + " ->> " + at[0] + ")), '')";
    }

    /** DuckDB's regexp_extract: the leftmost match's group ('' when none matches; NULL for NULL). */
    private SqlWriter.Piece regexpExtract(List<SqlExpr> a) {
        String pattern = literal(a.get(1), "a regexp_extract pattern");
        int g = group(a);
        List<Integer> groups = g == 0 ? List.of() : List.of(g);
        return w -> w.append("(SELECT CASE WHEN __rq.s IS NULL THEN NULL WHEN __rq.f IS NULL THEN '' ELSE "
                + part("__rq.s", "__rq.f", span(g), "0") + " END FROM (SELECT __rp.s AS s, ")
                .piece(SqliteRegex.find(v -> v.append("__rp.s"), pattern, groups))
                .append(" AS f FROM (SELECT ").expr(a.get(0), 0).append(" AS s) AS __rp) AS __rq)");
    }

    /** DuckDB's regexp_extract_all: each non-overlapping match's group, left to right (an empty match moves one past
     *  itself); NULL for NULL. A walk over the matches, each found from the last one's end. */
    private SqlWriter.Piece regexpExtractAll(List<SqlExpr> a) {
        String pattern = literal(a.get(1), "a regexp_extract_all pattern");
        int g = group(a);
        List<Integer> groups = g == 0 ? List.of() : List.of(g);
        String next = "__ms.off + (__ms.f ->> 1) + CASE WHEN (__ms.f ->> 1) = (__ms.f ->> 0) THEN 1 ELSE 0 END";
        return w -> w.append("(SELECT CASE WHEN __rp.s IS NULL THEN NULL ELSE (WITH RECURSIVE __ms(n, off, f) AS"
                + " (SELECT 0, 0, ").piece(SqliteRegex.find(v -> v.append("__rp.s"), pattern, groups))
                .append(" UNION ALL SELECT __ms.n + 1, " + next + ", ")
                .piece(SqliteRegex.find(v -> v.append("substr(__rp.s, " + next + " + 1)"), pattern, groups))
                .append(" FROM __ms WHERE __ms.f IS NOT NULL AND " + next + " <= length(__rp.s)) SELECT"
                        + " json_group_array(" + part("__rp.s", "__ms.f", span(g), "__ms.off") + " ORDER BY __ms.n)"
                        + " FROM __ms WHERE __ms.f IS NOT NULL) END FROM (SELECT ").expr(a.get(0), 0)
                .append(" AS s) AS __rp)");
    }

    /**
     * DuckDB's regexp_replace(s, p, r[, options]): the first match unless the options say 'g' (each, left to right; an
     * empty match moves one past itself). The replacement's {@code \0}..{@code \9} are the match's groups,
     * {@code \\} a backslash. NULL for NULL.
     */
    private SqlWriter.Piece regexpReplace(List<SqlExpr> a) {
        String pattern = literal(a.get(1), "a regexp_replace pattern");
        String replacement = literal(a.get(2), "a regexp_replace replacement");
        String options = a.size() > 3 ? literal(a.get(3), "regexp_replace options") : "";
        boolean global = false;
        String flags = "";
        for (char o : options.toCharArray()) {
            switch (o) {
                case 'g' -> global = true;
                case 'i' -> flags = "(?i)";
                case 'c' -> flags = "";
                default -> throw new DialectCapability("regexp_replace option '" + o + "' reached SQLite");
            }
        }
        String p = flags + pattern;
        // the replacement's parts: text, or a group reference
        List<Object> parts = new ArrayList<>();
        List<Integer> groups = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < replacement.length(); i++) {
            char ch = replacement.charAt(i);
            if (ch == '\\' && i + 1 < replacement.length() && Character.isDigit(replacement.charAt(i + 1))) {
                if (text.length() > 0) {
                    parts.add(text.toString());
                    text.setLength(0);
                }
                int k = replacement.charAt(++i) - '0';
                parts.add(k);
                if (k > 0 && !groups.contains(k)) {
                    groups.add(k);
                }
            } else if (ch == '\\' && i + 1 < replacement.length()) {
                text.append(replacement.charAt(++i));
            } else {
                text.append(ch);
            }
        }
        if (text.length() > 0) {
            parts.add(text.toString());
        }
        java.util.function.BiFunction<String, String, String> repl = (f, off) -> {
            List<String> pieces = new ArrayList<>();
            for (Object part : parts) {
                pieces.add(part instanceof Integer k ? part("__rp.s", f, k == 0 ? span(0)
                        : new String[] {String.valueOf(2 + 2 * groups.indexOf(k)), String.valueOf(3 + 2
                                * groups.indexOf(k))}, off) : stringLit((String) part));
            }
            return pieces.isEmpty() ? "''" : String.join(" || ", pieces);
        };
        if (!global) {
            return w -> w.append("(SELECT CASE WHEN __rp.s IS NULL THEN NULL WHEN __rp.f IS NULL THEN __rp.s ELSE"
                    + " substr(__rp.s, 1, __rp.f ->> 0) || " + repl.apply("__rp.f", "0")
                    + " || substr(__rp.s, (__rp.f ->> 1) + 1) END FROM (SELECT __rr.s AS s, ")
                    .piece(SqliteRegex.find(v -> v.append("__rr.s"), p, groups)).append(" AS f FROM (SELECT ")
                    .expr(a.get(0), 0).append(" AS s) AS __rr) AS __rp)");
        }
        String empty = "(__ms.f ->> 1) = (__ms.f ->> 0)";
        String next = "__ms.off + (__ms.f ->> 1) + CASE WHEN " + empty + " THEN 1 ELSE 0 END";
        return w -> w.append("(SELECT CASE WHEN __rp.s IS NULL THEN NULL ELSE (WITH RECURSIVE __ms(off, o, f) AS"
                + " (SELECT 0, '', ").piece(SqliteRegex.find(v -> v.append("__rp.s"), p, groups))
                .append(" UNION ALL SELECT " + next + ", __ms.o || substr(__rp.s, __ms.off + 1, __ms.f ->> 0) || "
                        + repl.apply("__ms.f", "__ms.off") + " || CASE WHEN " + empty + " THEN substr(__rp.s, __ms.off"
                        + " + (__ms.f ->> 1) + 1, 1) ELSE '' END, ")
                .piece(SqliteRegex.find(v -> v.append("substr(__rp.s, " + next + " + 1)"), p, groups))
                .append(" FROM __ms WHERE __ms.f IS NOT NULL AND __ms.off <= length(__rp.s)) SELECT __ms.o ||"
                        + " substr(__rp.s, __ms.off + 1) FROM __ms ORDER BY __ms.off DESC LIMIT 1) END FROM (SELECT ")
                .expr(a.get(0), 0).append(" AS s) AS __rp)");
    }

    private static DialectCapability wall(SqlFn fn, String why) {
        return new DialectCapability(fn + " reached SQLite: " + why);
    }

    private static String literal(SqlExpr e, String what) {
        if (e instanceof SqlExpr.StringLit s) {
            return s.value();
        }
        throw new DialectCapability(what + " that is not a literal reached SQLite");
    }

    // ---- list calls

    private SqlWriter lists(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String e = param("__e");
        String o = param("__o");
        return switch (c.fn()) {
            case LIST_FILTER -> {
                SqlExpr.Lambda l = lambdaOf(a.get(1), c.fn());
                String x = l.params().get(0);
                SqlType el = listElement(a.get(0));
                yield writer.piece(overElements(a.get(0), x, offsetOf(l), indexOf(l), jsonOf(w -> w.append(param(x)),
                        el), w -> w.expr(lambdaBody(l, el), 0)));
            }
            case LIST_TRANSFORM -> {
                SqlExpr.Lambda l = lambdaOf(a.get(1), c.fn());
                SqlExpr body = lambdaBody(l, listElement(a.get(0)));
                yield writer.piece(overElements(a.get(0), l.params().get(0), offsetOf(l), indexOf(l), json(body),
                        null));
            }
            // NULL ++ xs is xs, as DuckDB's list_concat
            case LIST_CONCAT -> writer.piece(concatLists(a, c));
            // 1-based, negative from the end, 0 and out of range NULL (DuckDB's list_extract)
            case LIST_GET -> writer.append("(CASE WHEN ").expr(a.get(1), 4).append(" = 0 THEN NULL ELSE ")
                    .piece(elementAt(a.get(0), w -> w.append("CASE WHEN ").expr(a.get(1), 4).append(" < 0 THEN '#' || ")
                            .expr(a.get(1), 6).append(" ELSE ").expr(a.get(1), 6).append(" - 1 END")))
                    .append(" END)");
            // DuckDB's list_position: 1-based, NULL when absent, a NULL needle found by IS
            case LIST_POSITION -> writer.append("(SELECT min(" + o + ") + 1 FROM ")
                    .piece(elements(a.get(0), "__e", "__o", null)).append(" WHERE " + e + " IS ").expr(a.get(1), 5)
                    .append(")");
            // DuckDB's list_distinct drops NULLs; the first occurrence's order
            case LIST_DISTINCT -> writer.piece(unlessNull(a.get(0), w -> w.append("(SELECT json_group_array(")
                    .piece(jsonOf(v -> v.append(e), listElement(a.get(0)))).append(" ORDER BY " + o + ") FROM (SELECT "
                            + e + ", min(" + o + ") AS " + o + " FROM ")
                    .piece(elements(a.get(0), "__e", "__o", null)).append(" WHERE " + e + " IS NOT NULL GROUP BY " + e
                            + "))")));
            // list_append(NULL, v) is [v]
            case LIST_APPEND -> writer.append("json_insert(coalesce(").expr(a.get(0), 0).append(", json_array()),"
                    + " '$[#]', ").piece(json(a.get(1))).append(")");
            case LIST_SUM -> writer.piece(overRows(a.get(0), SqlAgg.Fn.SUM, List.of()));
            case LIST_MIN -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MIN, List.of()));
            case LIST_MAX -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MAX, List.of()));
            case LIST_AVG -> writer.piece(overRows(a.get(0), SqlAgg.Fn.AVG, List.of()));
            case LIST_MEDIAN -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MEDIAN, List.of()));
            case LIST_MODE -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MODE, List.of()));
            case LIST_BOOL_AND -> writer.piece(overRows(a.get(0), SqlAgg.Fn.BOOL_AND, List.of()));
            case LIST_BOOL_OR -> writer.piece(overRows(a.get(0), SqlAgg.Fn.BOOL_OR, List.of()));
            // ascending / descending, NULLs last both ways (DuckDB's list_sort / list_reverse_sort); a struct by its
            // fields in declared order
            case LIST_SORT, LIST_SORT_DESC -> {
                String dir = c.fn() == SqlFn.LIST_SORT ? " ASC NULLS LAST" : " DESC NULLS LAST";
                SqlType el = listElement(a.get(0));
                yield writer.piece(unlessNull(a.get(0), w -> w.append("(SELECT json_group_array(")
                        .piece(jsonOf(v -> v.append(e), el)).append(" ORDER BY ").append(elementSort(e, el, dir))
                        .append(") FROM ").piece(elements(a.get(0), "__e", "__o", null)).append(")")));
            }
            case LIST_REVERSE -> writer.piece(unlessNull(a.get(0), w -> w.append("(SELECT json_group_array(")
                    .piece(jsonOf(v -> v.append(e), listElement(a.get(0)))).append(" ORDER BY " + o + " DESC) FROM ")
                    .piece(elements(a.get(0), "__e", "__o", null)).append(")")));
            case LIST_TAIL -> writer.piece(overElements(a.get(0), "__e", "__o", null,
                    jsonOf(w -> w.append(e), listElement(a.get(0))), w -> w.append(o + " > 0")));
            case LIST_INIT -> writer.piece(overElements(a.get(0), "__e", "__o", null,
                    jsonOf(w -> w.append(e), listElement(a.get(0))), w -> w.append(o + " < json_array_length(")
                            .expr(a.get(0), 0).append(") - 1")));
            // DuckDB's list_slice: 1-based, both ends inclusive, a negative end from the end
            case LIST_SLICE -> writer.piece(overElements(a.get(0), "__e", "__o", null,
                    jsonOf(w -> w.append(e), listElement(a.get(0))), w -> w.append(o + " + 1 >= ")
                            .piece(sliceEnd(a.get(0), a.get(1))).append(" AND " + o + " + 1 <= ")
                            .piece(sliceEnd(a.get(0), a.get(2)))));
            case LIST_FLATTEN -> {
                SqlType inner = listElement(a.get(0)) instanceof SqlType.Array arr ? arr.element() : null;
                yield writer.piece(unlessNull(a.get(0), w -> w.append("(SELECT json_group_array(")
                        .piece(jsonOf(v -> v.append(elementRead(inner, "__fi")), inner))
                        .append(" ORDER BY __fo.key, __fi.key) FROM json_each(").expr(a.get(0), 0)
                        .append(") AS __fo, json_each(__fo.value) AS __fi)")));
            }
            case LIST_ZIP -> writer.piece(zip(a, c));
            case LIST_PRODUCT -> writer.append("(WITH RECURSIVE __p(__i, __v) AS (SELECT 0, 1 UNION ALL SELECT __p.__i"
                    + " + 1, __p.__v * __x.value FROM __p JOIN json_each(").expr(a.get(0), 0)
                    .append(") AS __x ON __x.key = __p.__i) SELECT CASE WHEN ").expr(a.get(0), 4)
                    .append(" IS NULL THEN NULL ELSE __v END FROM __p ORDER BY __i DESC LIMIT 1)");
            case LIST_REDUCE -> listReduce(writer, a);
            // DuckDB's range: [start, stop) by step (start 0, step 1 by default)
            case RANGE_FN -> range(writer, a);
            case REPEAT_VALUE -> writer.append("(WITH RECURSIVE __g(__n) AS (SELECT 1 UNION ALL SELECT __n + 1 FROM __g"
                    + " WHERE __n < ").expr(a.get(1), 4).append(") SELECT CASE WHEN ").expr(a.get(1), 4)
                    .append(" > 0 THEN json_group_array(").piece(json(a.get(0))).append(") ELSE json_array() END FROM"
                            + " __g)");
            case TYPEOF -> throw wall(c.fn(), "its type names are SQLite's storage classes");
            default -> throw new IllegalStateException("not a list call: " + c.fn());
        };
    }

    /** The lists concatenated, NULL ones as empty, each element re-held as its type. */
    private SqlWriter.Piece concatLists(List<SqlExpr> a, SqlExpr.Call c) {
        SqlType el = listElement(c);
        return w -> {
            w.append("(SELECT json_group_array(").piece(jsonOf(v -> v.append("__c.__v"), el))
                    .append(" ORDER BY __c.__s, __c.__k) FROM (");
            for (int i = 0; i < a.size(); i++) {
                w.append(i > 0 ? " UNION ALL " : "").append("SELECT " + i + " AS __s, key AS __k, " + elementRead(el)
                        + " AS __v FROM json_each(").expr(a.get(i), 0).append(")");
            }
            w.append(") AS __c)");
        };
    }

    /** DuckDB's list_zip: a struct per position of the longest list, a shorter one's field NULL there; the fields
     *  named as the result's struct type names them. */
    private SqlWriter.Piece zip(List<SqlExpr> a, SqlExpr.Call c) {
        if (!(listElement(c) instanceof SqlType.Struct st) || st.fields().size() != a.size()) {
            throw wall(c.fn(), "a zip of unknown struct type");
        }
        return w -> {
            w.append("(SELECT json_group_array(json_object(");
            for (int i = 0; i < a.size(); i++) {
                int at = i;
                w.append(i > 0 ? ", " : "").append(stringLit(st.fields().get(i).name())).append(", ")
                        .piece(jsonOf(elementAt(a.get(i), v -> v.append("__z.key")), st.fields().get(at).type()));
            }
            w.append(") ORDER BY __z.key) FROM json_each(");
            // the longest list's positions
            w.append("(SELECT __l FROM (");
            for (int i = 0; i < a.size(); i++) {
                w.append(i > 0 ? " UNION ALL " : "").append("SELECT ").expr(a.get(i), 0).append(" AS __l");
            }
            w.append(") ORDER BY coalesce(json_array_length(__l), 0) DESC LIMIT 1)) AS __z)");
        };
    }

    private SqlWriter range(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece start = a.size() == 1 ? w -> w.append("0") : w -> w.expr(a.get(0), 0);
        SqlExpr last = a.get(a.size() == 1 ? 0 : 1);
        SqlWriter.Piece stop = w -> w.expr(last, 0);
        SqlWriter.Piece step = a.size() > 2 ? w -> w.expr(a.get(2), 0) : w -> w.append("1");
        return writer.append("(CASE WHEN ((").piece(stop).append(") - (").piece(start).append(")) * (").piece(step)
                .append(") > 0 THEN (WITH RECURSIVE __g(__v) AS (SELECT ").piece(start)
                .append(" UNION ALL SELECT __v + (").piece(step).append(") FROM __g WHERE (").piece(step)
                .append(" > 0 AND __v + (").piece(step).append(") < ").piece(stop).append(") OR (").piece(step)
                .append(" < 0 AND __v + (").piece(step).append(") > ").piece(stop)
                .append(")) SELECT json_group_array(__v) FROM __g) ELSE json_array() END)");
    }

    /** A slice bound, 1-based: a negative one counts from the end. */
    private static SqlWriter.Piece sliceEnd(SqlExpr xs, SqlExpr i) {
        return w -> w.append("(CASE WHEN ").expr(i, 4).append(" < 0 THEN json_array_length(").expr(xs, 0)
                .append(") + ").expr(i, 6).append(" + 1 ELSE ").expr(i, 0).append(" END)");
    }

    /** The ORDER BY of a list's elements: a struct by its fields in declared order. */
    private String elementSort(String x, @com.legend.base.Nullable SqlType el, String dir) {
        if (el instanceof SqlType.Struct st) {
            List<String> keys = new ArrayList<>();
            for (SqlType.Struct.Field f : st.fields()) {
                keys.add(elementSort("(" + x + " ->> " + stringLit(f.name()) + ")", f.type(), dir));
            }
            return String.join(", ", keys);
        }
        return x + dir;
    }

    // ---- map calls: JSON objects (text keys)

    private SqlWriter maps(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlType value = typeOf(c) instanceof SqlType.Map m ? m.value()
                : typeOf(a.isEmpty() ? c : a.get(0)) instanceof SqlType.Map m2 ? m2.value() : null;
        return switch (c.fn()) {
            case MAP_EMPTY -> writer.append("json_object()");
            case MAP_FROM_LISTS -> {
                SqlType v = listElement(a.get(1));
                yield writer.append("(SELECT json_group_object(__k.value, ").piece(jsonOf(w -> w.append(
                        elementRead(v, "__v")), v))
                        .append(" ORDER BY __k.key) FROM json_each(").expr(a.get(0), 0)
                        .append(") AS __k LEFT JOIN json_each(").expr(a.get(1), 0)
                        .append(") AS __v ON __v.key = __k.key)");
            }
            // DuckDB's map_from_entries: each entry a struct of (key, value), by position whatever their names
            case MAP_FROM_ENTRIES -> {
                if (!(listElement(a.get(0)) instanceof SqlType.Struct entry) || entry.fields().size() != 2) {
                    throw wall(c.fn(), "entries of " + listElement(a.get(0)));
                }
                String k = stringLit(entry.fields().get(0).name());
                String v = stringLit(entry.fields().get(1).name());
                SqlType vt = entry.fields().get(1).type();
                yield writer.append("(SELECT json_group_object(__e.value ->> " + k + ", ")
                        .piece(jsonOf(w -> w.append("(__e.value " + (vt == SqlType.Scalar.JSON ? "->" : "->>") + " "
                                + v + ")"), vt)).append(" ORDER BY __e.key) FROM json_each(").expr(a.get(0), 0)
                        .append(") AS __e)");
            }
            // DuckDB's map_extract: a list of the key's value, empty when absent
            case MAP_EXTRACT -> writer.append("(CASE WHEN ").expr(a.get(0), 4)
                    .append(" IS NULL THEN NULL ELSE coalesce((")
                    .append("SELECT json_array(").piece(jsonOf(w -> w.append(elementRead(value)), value))
                    .append(") FROM json_each(").expr(a.get(0), 0).append(") WHERE key = ").expr(a.get(1), 5)
                    .append("), json_array()) END)");
            case MAP_KEYS -> writer.piece(unlessNull(a.get(0), w -> w.append("(SELECT json_group_array(key ORDER BY id)"
                    + " FROM json_each(").expr(a.get(0), 0).append("))")));
            case MAP_VALUES -> writer.piece(unlessNull(a.get(0), w -> w.append("(SELECT json_group_array(")
                    .piece(jsonOf(v -> v.append(elementRead(value)), value)).append(" ORDER BY id) FROM json_each(")
                    .expr(a.get(0), 0).append("))")));
            // the later map's keys win, as DuckDB's map_concat
            case MAP_CONCAT -> writer.append("(SELECT json_group_object(__m.__k, ")
                    .piece(jsonOf(w -> w.append("__m.__v"), value))
                    .append(" ORDER BY __m.__s, __m.__i) FROM (SELECT key AS __k, " + elementRead(value)
                            + " AS __v, __s, id AS __i FROM (").piece(w -> {
                        for (int i = 0; i < a.size(); i++) {
                            w.append(i > 0 ? " UNION ALL " : "").append("SELECT " + i + " AS __s, j.* FROM json_each(")
                                    .expr(a.get(i), 0).append(") AS j");
                        }
                    }).append(") AS __u WHERE __s = (SELECT max(__s2) FROM (").piece(w -> {
                        for (int i = 0; i < a.size(); i++) {
                            w.append(i > 0 ? " UNION ALL " : "").append("SELECT " + i + " AS __s2, key AS __k2 FROM"
                                    + " json_each(").expr(a.get(i), 0).append(")");
                        }
                    }).append(") WHERE __k2 = __u.key)) AS __m)");
            default -> throw new IllegalStateException("not a map call: " + c.fn());
        };
    }

    // ---- temporal helpers: dates and date-times are ISO text

    private static final Set<String> UTC_ZONES = Set.of("UTC", "GMT", "Etc/UTC", "Z");

    /** A date-time text as held: its 'T' a space. */
    private static SqlWriter.Piece dateTimeText(SqlWriter.Piece text) {
        return w -> w.append("replace(trim(").piece(text).append("), 'T', ' ')");
    }

    /** A date-time text with an offset ('Z', '+HH:MM', '+HHMM') as its naive UTC text (SQLite's modifiers shift by
     *  '+HH:MM'; the fraction's digits kept as written); one with none as it is. */
    private static SqlWriter.Piece utcOf(SqlWriter.Piece t) {
        SqlWriter.Piece shifted6 = shift(w -> w.append("substr(").piece(t).append(", 1, length(").piece(t)
                .append(") - 6)"), w -> w.append("substr(").piece(t).append(", -6)"));
        SqlWriter.Piece shifted5 = shift(w -> w.append("substr(").piece(t).append(", 1, length(").piece(t)
                .append(") - 5)"), w -> w.append("(substr(").piece(t).append(", -5, 3) || ':' || substr(").piece(t)
                .append(", -2))"));
        return w -> w.append("(CASE WHEN ").piece(t).append(" GLOB '*Z' THEN substr(").piece(t).append(", 1, length(")
                .piece(t).append(") - 1) WHEN ").piece(t).append(" GLOB '*[+-][0-9][0-9]:[0-9][0-9]' THEN ")
                .piece(shifted6).append(" WHEN length(").piece(t).append(") > 10 AND ").piece(t)
                .append(" GLOB '*[0-9][+-][0-9][0-9][0-9][0-9]' THEN ").piece(shifted5).append(" ELSE ").piece(t)
                .append(" END)");
    }

    /** {@code body} (a naive date-time text) shifted back by {@code offset} ('+HH:MM'): its seconds by datetime, its
     *  fraction's digits as written. */
    private static SqlWriter.Piece shift(SqlWriter.Piece body, SqlWriter.Piece offset) {
        return w -> w.append("(datetime(substr(").piece(body).append(", 1, 19), CASE substr(").piece(offset)
                .append(", 1, 1) WHEN '+' THEN '-' ELSE '+' END || substr(").piece(offset).append(", 2)) || substr(")
                .piece(body).append(", 20))");
    }

    /** A date-time text with its date's fields padded ('2014-2-27' is 2014-02-27, as DuckDB's cast reads it), its time
     *  and fraction as written; a text with no year-month-day date as it is. */
    private static SqlWriter.Piece padded(SqlWriter.Piece text) {
        String t = "__pd.t";
        String d = "CASE WHEN instr(" + t + ", ' ') > 0 THEN substr(" + t + ", 1, instr(" + t + ", ' ') - 1) ELSE " + t
                + " END";
        String rest = "CASE WHEN instr(" + t + ", ' ') > 0 THEN substr(" + t + ", instr(" + t + ", ' ')) ELSE '' END";
        String y = "substr(__pd2.d, 1, instr(__pd2.d, '-') - 1)";
        String md = "substr(__pd2.d, instr(__pd2.d, '-') + 1)";
        return w -> w.append("(SELECT CASE WHEN __pd2.d GLOB '[0-9]*-[0-9]*-[0-9]*' THEN printf('%04d-%02d-%02d', " + y
                + ", substr(" + md + ", 1, instr(" + md + ", '-') - 1), substr(" + md + ", instr(" + md
                + ", '-') + 1)) || __pd2.rest ELSE __pd2.t END FROM (SELECT " + t + " AS t, " + d + " AS d, " + rest
                + " AS rest FROM (SELECT ").piece(text).append(" AS t) AS __pd) AS __pd2)");
    }

    /** Pure's parseDate of a text: its date's fields padded ({@link #padded}), its time and fraction as written; a text
     *  that is no date raises. */
    private SqlWriter.Piece parsedDate(SqlExpr x) {
        return w -> w.append("(SELECT CASE WHEN date(substr(__pp.t, 1, 10)) = substr(__pp.t, 1, 10) AND (length(__pp.t)"
                + " = 10 OR substr(__pp.t, 11, 1) = ' ') THEN __pp.t ELSE ").piece(raise(List.of(SqlExpr.Call.of(
                        SqlFn.CONCAT, new SqlExpr.StringLit("Could not parse '"), x,
                        new SqlExpr.StringLit("' as a date"))))).append(" END FROM (SELECT ")
                .piece(padded(dateTimeText(w2 -> w2.expr(x, 0)))).append(" AS t) AS __pp)");
    }

    private static final String BASE64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    /** A hex digit's value, of the digit at {@code k} (1-based) of {@code hex}. */
    private static String nibble(String hex, int k) {
        return "(instr('0123456789ABCDEF', substr(" + hex + ", " + k + ", 1)) - 1)";
    }

    /** A text's base64 (of its UTF-8 bytes, padded with '='): its bytes' hex three bytes at a time, each group's 24
     *  bits four characters, in a recursive walk. */
    private static SqlWriter.Piece base64Encoded(SqlExpr x) {
        StringBuilder n = new StringBuilder();
        for (int k = 1; k <= 6; k++) {
            n.append(k == 1 ? "" : " | ").append("(").append(nibble("__g.h6", k)).append(" << ").append(4 * (6 - k))
                    .append(")");
        }
        String a = "'" + BASE64 + "'";
        return w -> w.append("(WITH RECURSIVE __b64(h, o) AS (SELECT hex(__s.v), CASE WHEN __s.v IS NULL THEN NULL"
                + " ELSE '' END FROM (SELECT ").expr(x, 0).append(" AS v) AS __s UNION ALL SELECT substr(__b64.h, 7),"
                + " __b64.o || (SELECT substr(" + a + ", ((__q.n >> 18) & 63) + 1, 1) || substr(" + a
                + ", ((__q.n >> 12) & 63) + 1, 1) || CASE WHEN __q.l >= 4 THEN substr(" + a
                + ", ((__q.n >> 6) & 63) + 1, 1) ELSE '=' END || CASE WHEN __q.l = 6 THEN substr(" + a
                + ", (__q.n & 63) + 1, 1) ELSE '=' END FROM (SELECT "
                + n + " AS n, __g.l AS l FROM (SELECT substr(substr(__b64.h, 1, 6) || '00000', 1, 6) AS h6,"
                + " length(substr(__b64.h, 1, 6)) AS l) AS __g) AS __q) FROM __b64 WHERE __b64.h <> '')"
                + " SELECT o FROM __b64 WHERE h = '')");
    }

    /** A base64 text's bytes as UTF-8 text ('=' padding optional): four characters at a time, each group's 24 bits
     *  three bytes' hex (fewer for a short last group), the whole {@code unhex}ed. */
    private static SqlWriter.Piece base64Decoded(SqlExpr x) {
        StringBuilder n = new StringBuilder();
        for (int k = 1; k <= 4; k++) {
            n.append(k == 1 ? "" : " | ").append("((instr('").append(BASE64).append("', substr(__g.q4, ").append(k)
                    .append(", 1)) - 1) << ").append(6 * (4 - k)).append(")");
        }
        return w -> w.append("(WITH RECURSIVE __d64(t, h) AS (SELECT rtrim(__s.v, '='), CASE WHEN __s.v IS NULL THEN"
                + " NULL ELSE '' END FROM (SELECT ").expr(x, 0).append(" AS v) AS __s UNION ALL SELECT substr(__d64.t,"
                + " 5), __d64.h || (SELECT substr(printf('%06X', __q.n), 1, 2 * (__q.l - 1)) FROM (SELECT " + n
                + " AS n, __g.l AS l FROM (SELECT substr(substr(__d64.t, 1, 4) || 'AAA', 1, 4) AS q4,"
                + " length(substr(__d64.t, 1, 4)) AS l) AS __g) AS __q) FROM __d64 WHERE __d64.t <> '')"
                + " SELECT CAST(unhex(h) AS TEXT) FROM __d64 WHERE t = '')");
    }

    /** The Levenshtein distance of two texts, by characters: the edit-distance table a row at a time, each row a JSON
     *  array its cells are appended to, in one recursive walk over the cells. */
    private static SqlWriter.Piece levenshtein(SqlExpr x, SqlExpr y) {
        String cell = "min((__lv.p ->> (__lv.j + 1)) + 1, (__lv.c ->> __lv.j) + 1, (__lv.p ->> __lv.j)"
                + " + (substr(__lv.a, __lv.i, 1) <> substr(__lv.b, __lv.j + 1, 1)))";
        String more = "__lv.j < length(__lv.b)";
        return w -> w.append("(WITH RECURSIVE __lv(a, b, i, j, p, c) AS (SELECT __s.a, __s.b, 0, 0, '[]', '[0]' FROM"
                + " (SELECT ").expr(x, 0).append(" AS a, ").expr(y, 0).append(" AS b) AS __s UNION ALL SELECT __lv.a,"
                + " __lv.b, CASE WHEN " + more + " THEN __lv.i ELSE __lv.i + 1 END, CASE WHEN " + more + " THEN __lv.j"
                + " + 1 ELSE 0 END, CASE WHEN " + more + " THEN __lv.p ELSE __lv.c END, CASE WHEN " + more + " THEN"
                + " json_insert(__lv.c, '$[#]', CASE WHEN __lv.i = 0 THEN __lv.j + 1 ELSE " + cell + " END) ELSE"
                + " json_array(__lv.i + 1) END FROM __lv WHERE NOT (__lv.j = length(__lv.b) AND __lv.i ="
                + " length(__lv.a))) SELECT c ->> j FROM __lv WHERE i = length(a) AND j = length(b))");
    }

    /** A date-time's microseconds since the epoch: its whole seconds ({@code unixepoch} of its first 19 characters,
     *  exact) and its fraction's digits, as written (SQLite's own functions keep milliseconds). */
    private static SqlWriter.Piece micros(SqlExpr t) {
        return w -> w.append("(unixepoch(substr(").expr(t, 0).append(", 1, 19)) * 1000000 + CASE WHEN instr(")
                .expr(t, 0).append(", '.') > 0 THEN CAST(substr(substr(").expr(t, 0).append(", instr(").expr(t, 0)
                .append(", '.') + 1) || '000000', 1, 6) AS INTEGER) ELSE 0 END)");
    }

    /** A date-time of the given microseconds since the epoch: its seconds by {@code datetime}, its fraction's digits
     *  (none when whole; else six, trailing zeros trimmed). */
    private static SqlWriter.Piece fromMicros(SqlWriter.Piece us) {
        SqlWriter.Piece secs = w -> w.append("((").piece(us).append(" - ((").piece(us)
                .append(" % 1000000) + 1000000) % 1000000) / 1000000)");
        SqlWriter.Piece frac = w -> w.append("(((").piece(us).append(" % 1000000) + 1000000) % 1000000)");
        return w -> w.append("(datetime(").piece(secs).append(", 'unixepoch') || CASE WHEN ").piece(frac)
                .append(" = 0 THEN '' ELSE '.' || rtrim(printf('%06d', ").piece(frac).append("), '0') END)");
    }

    /** A date part of a date, a Pure integer: the ISO day of the week Monday 1 to Sunday 7, the week the ISO week. */
    private static SqlWriter.Piece extract(List<SqlExpr> a) {
        com.legend.sql.DatePart part = part(a.get(0));
        String code = switch (part) {
            case YEAR -> "%Y";
            case MONTH -> "%m";
            case DAY -> "%d";
            case HOUR -> "%H";
            case MINUTE -> "%M";
            case SECOND -> "%S";
            case WEEK -> "%V";
            case DAY_OF_YEAR -> "%j";
            case ISO_DAY_OF_WEEK -> "%u";
            case QUARTER -> null;
            case MILLISECOND, MICROSECOND, NANOSECOND -> throw new DialectCapability("date part '"
                    + part.name().toLowerCase(Locale.ROOT) + "' has no measured SQLite spelling");
        };
        if (code == null) {
            return w -> w.append("((CAST(strftime('%m', ").expr(a.get(1), 0).append(") AS INTEGER) + 2) / 3)");
        }
        return w -> w.append("CAST(strftime('" + code + "', ").expr(a.get(1), 0).append(") AS INTEGER)");
    }

    /** The base's contract: year/quarter/month/week deliver a DATE, finer parts a date-time; DuckDB's week starts on
     *  Monday ({@code date(x, '-6 days', 'weekday 1')}: the Monday on or before x). */
    private static SqlWriter.Piece dateTrunc(List<SqlExpr> a) {
        com.legend.sql.DatePart part = part(a.get(0));
        SqlExpr t = a.get(1);
        return switch (part) {
            case YEAR -> w -> w.append("strftime('%Y-01-01', ").expr(t, 0).append(")");
            case QUARTER -> w -> w.append("printf('%s-%02d-01', strftime('%Y', ").expr(t, 0)
                    .append("), ((CAST(strftime('%m', ").expr(t, 0).append(") AS INTEGER) - 1) / 3) * 3 + 1)");
            case MONTH -> w -> w.append("strftime('%Y-%m-01', ").expr(t, 0).append(")");
            case WEEK -> w -> w.append("date(").expr(t, 0).append(", '-6 days', 'weekday 1')");
            case DAY -> w -> w.append("(date(").expr(t, 0).append(") || ' 00:00:00')");
            case HOUR -> w -> w.append("strftime('%Y-%m-%d %H:00:00', ").expr(t, 0).append(")");
            case MINUTE -> w -> w.append("strftime('%Y-%m-%d %H:%M:00', ").expr(t, 0).append(")");
            case SECOND -> w -> w.append("strftime('%Y-%m-%d %H:%M:%S', ").expr(t, 0).append(")");
            case MILLISECOND -> w -> w.append("strftime('%Y-%m-%d %H:%M:%f', ").expr(t, 0).append(")");
            case MICROSECOND, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_trunc part '"
                    + part.name().toLowerCase(Locale.ROOT) + "' has no measured SQLite spelling");
        };
    }

    /** A date plus an interval. Whole years and months by the date functions' modifiers, a month past the month's end
     *  clamped ({@code 'floor'}, SQLite 3.46: 2020-01-31 + 1 month is 2020-02-29, as DuckDB's); a date plus days is a
     *  date; anything finer in microseconds. A date-time keeps its fraction's digits through whole units. */
    private SqlWriter.Piece addInterval(List<SqlExpr> a, SqlExpr.Call c) {
        com.legend.sql.DatePart unit = part(a.get(0));
        SqlExpr n = a.get(1);
        SqlExpr t = a.get(2);
        boolean date = typeOf(c) == SqlType.Scalar.DATE || typeOf(t) == SqlType.Scalar.DATE
                && typeOf(c) != SqlType.Scalar.TIMESTAMP;
        String months = switch (unit) {
            case YEAR -> "years";
            case MONTH -> "months";
            case QUARTER, WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND, NANOSECOND, DAY_OF_YEAR,
                    ISO_DAY_OF_WEEK -> null;
        };
        if (months != null) {
            SqlWriter.Piece modifier = w -> w.append("printf('%+d " + months + "', ").expr(n, 0).append(")");
            if (date) {
                return w -> w.append("date(").expr(t, 0).append(", ").piece(modifier).append(", 'floor')");
            }
            return w -> w.append("(datetime(substr(").expr(t, 0).append(", 1, 19), ").piece(modifier)
                    .append(", 'floor') || substr(").expr(t, 0).append(", 20))");
        }
        Long micros = intervalMicros(unit);
        if (micros == null) {
            throw new DialectCapability("interval unit " + unit + " has no SQLite spelling");
        }
        if (date && micros % 86400000000L == 0) {
            long days = micros / 86400000000L;
            return w -> w.append("date(").expr(t, 0).append(", printf('%+d days', ").expr(n, 6)
                    .append(" * " + days + "))");
        }
        return fromMicros(w -> w.append("(").piece(micros(t)).append(" + ").expr(n, 6).append(" * " + micros + ")"));
    }

    /** A fixed-length unit's length in microseconds; null for a calendar one. */
    private static @com.legend.base.Nullable Long intervalMicros(com.legend.sql.DatePart unit) {
        return switch (unit) {
            case WEEK -> 7L * 86400000000L;
            case DAY -> 86400000000L;
            case HOUR -> 3600000000L;
            case MINUTE -> 60000000L;
            case SECOND -> 1000000L;
            case MILLISECOND -> 1000L;
            case MICROSECOND -> 1L;
            case YEAR, QUARTER, MONTH, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> null;
        };
    }

    /** DuckDB's date_diff counts BOUNDARIES crossed (23:59:59 to 00:00 is a day, 01-31 to 02-01 a month); its week is
     *  whole days over 7, truncated. */
    private static SqlWriter.Piece dateDiff(List<SqlExpr> a) {
        com.legend.sql.DatePart part = part(a.get(0));
        SqlExpr from = a.get(1);
        SqlExpr to = a.get(2);
        return switch (part) {
            case YEAR -> w -> w.append("(").piece(component(to, "%Y")).append(" - ").piece(component(from, "%Y"))
                    .append(")");
            case QUARTER -> w -> w.append("((").piece(component(to, "%Y")).append(" * 4 + (")
                    .piece(component(to, "%m")).append(" - 1) / 3) - (").piece(component(from, "%Y")).append(" * 4 + (")
                    .piece(component(from, "%m")).append(" - 1) / 3))");
            case MONTH -> w -> w.append("((").piece(component(to, "%Y")).append(" * 12 + ")
                    .piece(component(to, "%m")).append(") - (").piece(component(from, "%Y")).append(" * 12 + ")
                    .piece(component(from, "%m")).append("))");
            case WEEK -> w -> w.append("CAST((julianday(date(").expr(to, 0).append(")) - julianday(date(")
                    .expr(from, 0).append("))) / 7 AS INTEGER)");
            case DAY -> w -> w.append("CAST(julianday(date(").expr(to, 0).append(")) - julianday(date(")
                    .expr(from, 0).append(")) AS INTEGER)");
            case HOUR -> boundaries(from, to, 3600000000L);
            case MINUTE -> boundaries(from, to, 60000000L);
            case SECOND -> boundaries(from, to, 1000000L);
            case MILLISECOND -> boundaries(from, to, 1000L);
            case MICROSECOND -> boundaries(from, to, 1L);
            case NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_diff part '"
                    + part.name().toLowerCase(Locale.ROOT) + "' has no measured SQLite spelling");
        };
    }

    private static SqlWriter.Piece component(SqlExpr t, String code) {
        return w -> w.append("CAST(strftime('" + code + "', ").expr(t, 0).append(") AS INTEGER)");
    }

    /** The unit boundaries between two date-times: each floored to the unit, in microseconds. */
    private static SqlWriter.Piece boundaries(SqlExpr from, SqlExpr to, long unit) {
        return w -> w.append("(").piece(floorDiv(micros(to), unit)).append(" - ").piece(floorDiv(micros(from), unit))
                .append(")");
    }

    /** {@code a} floor-divided by a positive {@code b} (SQLite's / truncates toward zero). */
    private static SqlWriter.Piece floorDiv(SqlWriter.Piece a, long b) {
        return w -> w.append("((").piece(a).append(" - (((").piece(a).append(") % " + b + ") + " + b + ") % " + b
                + ") / " + b + ")");
    }

    /** DuckDB's time_bucket with the base's origins (1970-01-01; weeks the Monday 1969-12-29): fixed units binned in
     *  microseconds, floored; months and years by the month index since 1970-01, floored to the bucket's size. */
    private static SqlWriter.Piece timeBucket(List<SqlExpr> a) {
        com.legend.sql.DatePart unit = part(a.get(0));
        SqlExpr n = a.get(1);
        SqlExpr t = a.get(2);
        Long micros = intervalMicros(unit);
        if (micros != null) {
            long origin = unit == com.legend.sql.DatePart.WEEK ? -3L * 86400000000L : 0L;
            SqlWriter.Piece size = w -> w.append("(").expr(n, 6).append(" * " + micros + ")");
            SqlWriter.Piece m = w -> w.append("(").piece(micros(t)).append(" - (" + origin + "))");
            SqlWriter.Piece floored = w -> w.append("((").piece(m).append(" - (((").piece(m).append(") % ").piece(size)
                    .append(" + ").piece(size).append(") % ").piece(size).append(")) + (" + origin + "))");
            return fromMicros(floored);
        }
        int months = unit == com.legend.sql.DatePart.MONTH ? 1 : unit == com.legend.sql.DatePart.YEAR ? 12 : 0;
        if (months == 0) {
            throw new DialectCapability("a " + unit + " time bucket has no SQLite spelling");
        }
        SqlWriter.Piece k = w -> w.append("((").piece(component(t, "%Y")).append(" - 1970) * 12 + ")
                .piece(component(t, "%m")).append(" - 1)");
        SqlWriter.Piece size = w -> w.append("(").expr(n, 6).append(" * " + months + ")");
        return w -> w.append("(date('1970-01-01', printf('%+d months', ((").piece(k).append(") - (((").piece(k)
                .append(") % ").piece(size).append(" + ").piece(size).append(") % ").piece(size)
                .append(")))) || ' 00:00:00')");
    }

    private SqlWriter makeTimestamp(SqlWriter writer, List<SqlExpr> a, SqlExpr.Call c) {
        if (a.size() != 6) {
            throw wall(c.fn(), "make_timestamp of " + a.size() + " arguments");
        }
        SqlExpr s = a.get(5);
        return writer.append("(printf('%04d-%02d-%02d %02d:%02d:', ").list(a.subList(0, 5)).append(") || CASE WHEN ")
                .expr(s, 4).append(" = CAST(").expr(s, 0).append(" AS INTEGER) THEN printf('%02d', ").expr(s, 0)
                .append(") ELSE rtrim(printf('%09.6f', ").expr(s, 0).append("), '0') END)");
    }

    private static final String[] MONTHS = {"January", "February", "March", "April", "May", "June", "July", "August",
        "September", "October", "November", "December"};

    private static final String[] DAYS = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
        "Saturday"};

    private static SqlWriter.Piece monthName(SqlExpr t, boolean abbreviated) {
        return w -> {
            w.append("(CASE CAST(strftime('%m', ").expr(t, 0).append(") AS INTEGER)");
            for (int i = 0; i < 12; i++) {
                w.append(" WHEN " + (i + 1) + " THEN '" + (abbreviated ? MONTHS[i].substring(0, 3) : MONTHS[i]) + "'");
            }
            w.append(" END)");
        };
    }

    private static SqlWriter.Piece dayName(SqlExpr t) {
        return w -> {
            w.append("(CASE CAST(strftime('%w', ").expr(t, 0).append(") AS INTEGER)");
            for (int i = 0; i < 7; i++) {
                w.append(" WHEN " + i + " THEN '" + DAYS[i] + "'");
            }
            w.append(" END)");
        };
    }

    /** A date-time's fraction as {@code digits} digits: its text's, zero-padded. */
    private static SqlWriter.Piece fraction(SqlExpr t, int digits) {
        String zeros = "0".repeat(digits);
        return w -> w.append("substr(CASE WHEN instr(").expr(t, 0).append(", '.') > 0 THEN substr(").expr(t, 0)
                .append(", instr(").expr(t, 0).append(", '.') + 1) ELSE '' END || '" + zeros + "', 1, " + digits + ")");
    }

    /** A date-time formatted by DuckDB's strftime parts: runs of codes SQLite's strftime has, concatenated with the
     *  parts it has none for (names, the 12-hour clock, a fraction's digits). */
    private SqlWriter.Piece strftime(SqlExpr value, SqlExpr.FormatLit fl) {
        List<SqlWriter.Piece> pieces = new ArrayList<>();
        StringBuilder run = new StringBuilder();
        for (DateFmt p : fl.parts()) {
            String code = switch (p) {
                case DateFmt.Text t -> t.s().replace("%", "%%");
                case DateFmt.Part part -> switch (part) {
                    case YEAR4 -> "%Y";
                    case MONTH2 -> "%m";
                    case DAY2 -> "%d";
                    case HOUR2 -> "%H";
                    case MIN2 -> "%M";
                    case SEC2 -> "%S";
                    case SUBSEC_MICRO, SUBSEC_NANO, SUBSEC_MIN, MONTH_ABBREV, MONTH_NAME, WEEKDAY_NAME, HOUR12,
                         HOUR12_NOPAD, AMPM -> null;
                };
            };
            if (code != null) {
                run.append(code);
                continue;
            }
            if (run.length() > 0) {
                String f = stringLit(run.toString());
                pieces.add(w -> w.append("strftime(" + f + ", ").expr(value, 0).append(")"));
                run.setLength(0);
            }
            DateFmt.Part part = (DateFmt.Part) p;
            SqlWriter.Piece hour = w -> w.append("CAST(strftime('%H', ").expr(value, 0).append(") AS INTEGER)");
            pieces.add(switch (part) {
                case SUBSEC_MICRO -> fraction(value, 6);
                case SUBSEC_NANO -> fraction(value, 9);
                // DuckDB's %g: milliseconds, three digits
                case SUBSEC_MIN -> fraction(value, 3);
                case MONTH_ABBREV -> monthName(value, true);
                case MONTH_NAME -> monthName(value, false);
                case WEEKDAY_NAME -> dayName(value);
                case HOUR12 -> w -> w.append("printf('%02d', (").piece(hour).append(" + 11) % 12 + 1)");
                case HOUR12_NOPAD -> w -> w.append("CAST((").piece(hour).append(" + 11) % 12 + 1 AS TEXT)");
                case AMPM -> w -> w.append("(CASE WHEN ").piece(hour).append(" < 12 THEN 'AM' ELSE 'PM' END)");
                case YEAR4, MONTH2, DAY2, HOUR2, MIN2, SEC2 -> throw new IllegalStateException("coded: " + part);
            });
        }
        if (run.length() > 0 || pieces.isEmpty()) {
            String f = stringLit(run.toString());
            pieces.add(w -> w.append("strftime(" + f + ", ").expr(value, 0).append(")"));
        }
        if (pieces.size() == 1) {
            return pieces.get(0);
        }
        return w -> w.append("(").join(pieces, " || ", (x, piece) -> x.piece(piece)).append(")");
    }

    // ==================================================================
    // Hooks of the base
    // ==================================================================

    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        throw new DialectCapability("hashCode reached SQLite, which has no hash function");
    }

    /** Pure round is half-EVEN: the value scaled, rounded half away from zero by {@code round}, and a tie that rounded
     *  to an odd number taken back one toward zero. */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece scale = a.size() > 1 ? w -> w.append("pow(10, ").expr(a.get(1), 0).append(")")
                : w -> w.append("1");
        SqlWriter.Piece scaled = w -> w.append("(").expr(a.get(0), 6).append(" * ").piece(scale).append(")");
        SqlWriter.Piece r = w -> w.append("round(").piece(scaled).append(")");
        writer.append("((CASE WHEN abs(").piece(scaled).append(" - trunc(").piece(scaled)
                .append(")) = 0.5 AND CAST(").piece(r).append(" AS INTEGER) % 2 <> 0 THEN ").piece(r)
                .append(" - sign(").piece(scaled).append(") ELSE ").piece(r).append(" END) / ").piece(scale)
                .append(")");
        return writer;
    }

    /** 64-bit integer bit operators; there is no XOR operator ({@code (a | b) - (a & b)}); {@code >>} is arithmetic
     *  (-16 >> 2 is -4, as DuckDB's). */
    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        SqlWriter.Piece x = w -> w.expr(a.get(0), 7);
        SqlWriter.Piece y = w -> w.expr(a.get(1), 7);
        return switch (fnName) {
            case BIT_AND -> writer.append("(").piece(x).append(" & ").piece(y).append(")");
            case BIT_OR -> writer.append("(").piece(x).append(" | ").piece(y).append(")");
            case BIT_XOR -> writer.append("((").piece(x).append(" | ").piece(y).append(") - (").piece(x).append(" & ")
                    .piece(y).append("))");
            case BIT_SHIFT_LEFT -> writer.append("(").piece(x).append(" << ").piece(y).append(")");
            case BIT_SHIFT_RIGHT -> writer.append("(").piece(x).append(" >> ").piece(y).append(")");
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
    }

    // ==================================================================
    // Aggregates and windows
    // ==================================================================

    private static SqlWriter.Piece aggregateArgs(SqlAgg.Reducer r) {
        return w -> w.append(r.distinct() ? "DISTINCT " : "").list(r.args());
    }

    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM -> aggregate(writer, "SUM", r);
            case COUNT -> aggregate(writer, "COUNT", r);
            case AVG -> aggregate(writer, "AVG", r);
            case MIN -> aggregate(writer, "MIN", r);
            case MAX -> aggregate(writer, "MAX", r);
            // any non-NULL value: the least (SQLite has no any_value)
            case ANY_VALUE -> writer.append("min(").piece(aggregateArgs(r)).append(")");
            // the driver's statistics extension: stdev and variance are the sample ones
            case STDDEV_SAMP, STDDEV -> writer.append("stdev(").piece(aggregateArgs(r)).append(")");
            case VAR_SAMP, VARIANCE -> writer.append("variance(").piece(aggregateArgs(r)).append(")");
            case VAR_POP -> writer.piece(populationVariance(r));
            case STDDEV_POP -> writer.append("sqrt(").piece(populationVariance(r)).append(")");
            case COVAR_POP, COVAR_SAMP, CORR -> writer.piece(moments(r));
            case BOOL_AND -> writer.append("min(").piece(aggregateArgs(r)).append(")");
            case BOOL_OR -> writer.append("max(").piece(aggregateArgs(r)).append(")");
            case STRING_AGG -> groupConcat(writer, r);
            case LIST -> listOfValues(writer, r);
            case MEDIAN -> median(writer, r);
            case QUANTILE_CONT, QUANTILE_DISC -> percentile(writer, r);
            // the most frequent non-NULL value, the least among ties
            case MODE -> writer.append("(SELECT __m.value FROM json_each(json_group_array(")
                    .piece(json(r.args().get(0)))
                    .append(")) AS __m WHERE __m.value IS NOT NULL GROUP BY __m.value ORDER BY count(*) DESC,"
                            + " min(__m.key) LIMIT 1)");
            // the value at the key's greatest (least) row, rows of a NULL key ignored
            case ARG_MAX -> argExtreme(writer, r, " DESC");
            case ARG_MIN -> argExtreme(writer, r, " ASC");
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // a window function is a ranking or value function, never an aggregate (windowCall writes it)
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

    /** The population variance from the sample one: NULL over no value, 0 over one. */
    private static SqlWriter.Piece populationVariance(SqlAgg.Reducer r) {
        SqlExpr x = r.args().get(0);
        return w -> w.append("(CASE count(").expr(x, 0)
                .append(") WHEN 0 THEN NULL WHEN 1 THEN 0.0"
                + " ELSE variance(").expr(x, 0).append(") * (count(").expr(x, 0).append(") - 1) / count(")
                .expr(x, 0).append(") END)");
    }

    /** The {@code STRING_AGG} arm of {@link #reducer}. */
    private SqlWriter groupConcat(SqlWriter writer, SqlAgg.Reducer r) {
        writer.append("group_concat(").piece(aggregateArgs(r));
        if (!r.orderBy().isEmpty()) {
            writer.append(" ORDER BY ").join(r.orderBy(), ", ", this::aggregateOrderKey);
        }
        return writer.append(")");
    }

    /** The {@code LIST} arm of {@link #reducer}. */
    private SqlWriter listOfValues(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1) {
            throw new DialectCapability("a LIST aggregate of " + r.args().size() + " arguments reached SQLite");
        }
        writer.append("json_group_array(").append(r.distinct() ? "DISTINCT " : "").piece(json(r.args().get(0)));
        if (!r.orderBy().isEmpty()) {
            writer.append(" ORDER BY ").join(r.orderBy(), ", ", this::aggregateOrderKey);
        }
        return writer.append(")");
    }

    /** The {@code MEDIAN} arm of {@link #reducer}. */
    private SqlWriter median(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1 || r.distinct()) {
            throw new DialectCapability("a median of this shape reached SQLite");
        }
        return writer.piece(quantile(SqlAgg.Fn.QUANTILE_CONT, r.args().get(0), w -> w.append("0.5"), r));
    }

    /** The {@code QUANTILE_CONT} and {@code QUANTILE_DISC} arms of {@link #reducer}. */
    private SqlWriter percentile(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct() || r.orderBy().size() > 1) {
            throw new DialectCapability(r.fn() + " of this shape reached SQLite");
        }
        return writer.piece(quantile(r.fn(), r.args().get(0), w -> w.expr(r.args().get(1), 0), r));
    }

    /** The value at the first row in the key's {@code direction}, rows of a NULL key ignored: an aggregate's own ORDER
     *  BY (a json_each over the group's array is a subquery, where SQLite refuses the aggregate: "misuse of aggregate
     *  function", measured). */
    private SqlWriter argExtreme(SqlWriter writer, SqlAgg.Reducer r, String direction) {
        if (r.args().size() != 2 || r.distinct() || !r.orderBy().isEmpty()) {
            throw new DialectCapability(r.fn() + " of this shape reached SQLite");
        }
        SqlExpr v = r.args().get(0);
        SqlExpr k = r.args().get(1);
        return writer.append("((json_group_array(").piece(json(v)).append(" ORDER BY ").expr(k, 0)
                .append(direction).append(") FILTER (WHERE ").expr(k, 4)
                .append(" IS NOT NULL)) ->> 0)");
    }

    /** Covariance and correlation over the pairs with both values: their sums, as the moments' definitions. */
    private SqlWriter.Piece moments(SqlAgg.Reducer r) {
        SqlExpr y = r.args().get(0);
        SqlExpr x = r.args().get(1);
        SqlWriter.Piece n = w -> w.append("count(").expr(x, 6).append(" * ").expr(y, 6).append(")");
        SqlWriter.Piece sxy = w -> w.append("sum(").expr(x, 6).append(" * ").expr(y, 6).append(")");
        SqlWriter.Piece sx = w -> w.append("sum(").expr(x, 0).append(") FILTER (WHERE ").expr(y, 4)
                .append(" IS NOT NULL)");
        SqlWriter.Piece sy = w -> w.append("sum(").expr(y, 0).append(") FILTER (WHERE ").expr(x, 4)
                .append(" IS NOT NULL)");
        SqlWriter.Piece sxx = w -> w.append("sum(").expr(x, 6).append(" * ").expr(x, 6).append(") FILTER (WHERE ")
                .expr(y, 4).append(" IS NOT NULL)");
        SqlWriter.Piece syy = w -> w.append("sum(").expr(y, 6).append(" * ").expr(y, 6).append(") FILTER (WHERE ")
                .expr(x, 4).append(" IS NOT NULL)");
        SqlWriter.Piece cxy = w -> w.append("(").piece(sxy).append(" - CAST(").piece(sx).append(" AS REAL) * ")
                .piece(sy).append(" / ").piece(n).append(")");
        return switch (r.fn()) {
            case COVAR_POP -> w -> w.append("(CASE WHEN ").piece(n).append(" > 0 THEN ").piece(cxy).append(" / ")
                    .piece(n).append(" END)");
            case COVAR_SAMP -> w -> w.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxy).append(" / (")
                    .piece(n).append(" - 1) END)");
            default -> w -> w.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxy).append(" / sqrt((")
                    .piece(sxx).append(" - CAST(").piece(sx).append(" AS REAL) * ").piece(sx).append(" / ").piece(n)
                    .append(") * (").piece(syy).append(" - CAST(").piece(sy).append(" AS REAL) * ").piece(sy)
                    .append(" / ").piece(n).append(")) END)");
        };
    }

    /**
     * A grouped quantile: the group's non-NULL values sorted ({@code json_group_array(x ORDER BY x)}, its own order
     * key's direction, each at 17 digits: JSON keeps 15, measured) and indexed -- continuous: the value at
     * {@code q * (n - 1)} interpolated linearly; discrete: the first value whose cumulative share reaches {@code q},
     * {@code ceil(n * q) - 1} (both DuckDB's).
     */
    private static SqlWriter.Piece quantile(SqlAgg.Fn fn, SqlExpr x, SqlWriter.Piece q, SqlAgg.Reducer r) {
        boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
        boolean text = typeOf(x) == SqlType.Scalar.VARCHAR || typeOf(x) == SqlType.Scalar.DATE
                || typeOf(x) == SqlType.Scalar.TIMESTAMP;
        SqlWriter.Piece held = text ? w -> w.expr(x, 0)
                : w -> w.append("json(printf('%!.17g', ").expr(x, 0).append("))");
        SqlWriter.Piece sorted = w -> w.append("json_group_array(").piece(held).append(" ORDER BY ").expr(x, 0)
                .append(desc ? " DESC)" : " ASC)").append(" FILTER (WHERE ").expr(x, 4).append(" IS NOT NULL)");
        SqlWriter.Piece n = w -> w.append("count(").expr(x, 0).append(")");
        SqlWriter.Piece at = w -> w.append("json_extract(").piece(sorted).append(", '$[' || ");
        if (fn == SqlAgg.Fn.QUANTILE_DISC) {
            return w -> w.piece(at).append("max(CAST(ceil(").piece(n).append(" * (").piece(q)
                    .append(")) AS INTEGER) - 1, 0) || ']')");
        }
        SqlWriter.Piece pos = w -> w.append("((").piece(q).append(") * (").piece(n).append(" - 1))");
        SqlWriter.Piece lo = w -> w.piece(at).append("CAST(floor(").piece(pos).append(") AS INTEGER) || ']')");
        SqlWriter.Piece hi = w -> w.piece(at).append("CAST(ceil(").piece(pos).append(") AS INTEGER) || ']')");
        return w -> w.append("(").piece(lo).append(" + (").piece(hi).append(" - ").piece(lo).append(") * (")
                .piece(pos).append(" - floor(").piece(pos).append(")))");
    }

    private static final Set<SqlAgg.Fn> ORDERED_SETS = Set.of(SqlAgg.Fn.MEDIAN, SqlAgg.Fn.MODE,
            SqlAgg.Fn.QUANTILE_CONT, SqlAgg.Fn.QUANTILE_DISC);

    private static final Set<SqlAgg.Fn> MOMENTS = Set.of(SqlAgg.Fn.VAR_SAMP, SqlAgg.Fn.VARIANCE, SqlAgg.Fn.VAR_POP,
            SqlAgg.Fn.STDDEV_SAMP, SqlAgg.Fn.STDDEV, SqlAgg.Fn.STDDEV_POP, SqlAgg.Fn.COVAR_POP, SqlAgg.Fn.COVAR_SAMP,
            SqlAgg.Fn.CORR);

    /** A window function: SQLite's are the standard ones. The statistics extension's aggregates are no window
     *  functions ("stdev() may not be used as a window function", measured): a windowed moment is its sums over the
     *  window. An aggregate's own ORDER BY over a whole partition is the window's order over every row of it; an
     *  ordered set over a window is refused (a whole partition's is a correlated subquery by
     *  WholePartitionOrderedSets). */
    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        switch (w.fn()) {
            case SqlAgg.Reducer r when ORDERED_SETS.contains(r.fn()) ->
                    throw new DialectCapability(r.fn() + " over an ordered or framed window reached SQLite");
            case SqlAgg.Reducer r when MOMENTS.contains(r.fn()) -> {
                return writer.piece(windowMoments(r, w));
            }
            case SqlAgg.Reducer r when r.fn() == SqlAgg.Fn.ARG_MAX || r.fn() == SqlAgg.Fn.ARG_MIN
                    || r.fn() == SqlAgg.Fn.MODE ->
                    throw new DialectCapability("a windowed " + r.fn() + " reached SQLite");
            case SqlAgg.Reducer r when !r.orderBy().isEmpty() -> {
                if (!w.orderBy().isEmpty() || w.frame() != null) {
                    throw new DialectCapability("an ordered " + r.fn() + " over an ordered or framed window reached"
                            + " SQLite");
                }
                reducer(writer, new SqlAgg.Reducer(r.fn(), r.args(), r.distinct(), List.of()));
                return windowOver(writer, new SqlExpr.WindowCall(w.fn(), w.partitionBy(), r.orderBy(),
                        new SqlExpr.WindowCall.Frame(SqlExpr.WindowCall.Frame.Kind.ROWS,
                                new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding(),
                                new SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing())));
            }
            case SqlAgg.Reducer r -> reducer(writer, r);
            case SqlAgg.RankingFn r -> writer.append(aggregateName(r.fn())).append("(").list(r.args()).append(")");
            case SqlAgg.ValueFn v -> writer.append(aggregateName(v.fn())).append("(").list(v.args()).append(")");
        }
        return windowOver(writer, w);
    }

    /** A windowed variance, deviation, covariance or correlation from its sums over the window: the pairs with both
     *  values (one value: itself), {@code (sum(xy) - sum(x) sum(y) / n) / n} (population) or {@code / (n - 1)}
     *  (sample, NULL under two). */
    private SqlWriter.Piece windowMoments(SqlAgg.Reducer r, SqlExpr.WindowCall w) {
        boolean pair = r.args().size() == 2;
        SqlExpr y = r.args().get(0);
        SqlExpr x = pair ? r.args().get(1) : y;
        java.util.function.Function<SqlWriter.Piece, SqlWriter.Piece> over = arg -> v -> {
            v.append("sum(").piece(arg).append(")");
            windowOver(v, w);
        };
        SqlWriter.Piece both = v -> v.append("(CASE WHEN ").expr(x, 4).append(" IS NOT NULL AND ").expr(y, 4)
                .append(" IS NOT NULL THEN 1 END)");
        SqlWriter.Piece n = over.apply(both);
        SqlWriter.Piece sx = over.apply(v -> v.append("CAST(").expr(x, 0).append(" AS REAL) * ").piece(both));
        SqlWriter.Piece sy = over.apply(v -> v.append("CAST(").expr(y, 0).append(" AS REAL) * ").piece(both));
        SqlWriter.Piece sxy = over.apply(v -> v.append("CAST(").expr(x, 0).append(" AS REAL) * ").expr(y, 6)
                .append(" * ").piece(both));
        SqlWriter.Piece sxx = over.apply(v -> v.append("CAST(").expr(x, 0).append(" AS REAL) * ").expr(x, 6)
                .append(" * ").piece(both));
        SqlWriter.Piece syy = over.apply(v -> v.append("CAST(").expr(y, 0).append(" AS REAL) * ").expr(y, 6)
                .append(" * ").piece(both));
        SqlWriter.Piece cxy = v -> v.append("(").piece(sxy).append(" - ").piece(sx).append(" * ").piece(sy)
                .append(" / ").piece(n).append(")");
        SqlWriter.Piece cxx = v -> v.append("max(").piece(sxx).append(" - ").piece(sx).append(" * ").piece(sx)
                .append(" / ").piece(n).append(", 0.0)");
        SqlWriter.Piece cyy = v -> v.append("max(").piece(syy).append(" - ").piece(sy).append(" * ").piece(sy)
                .append(" / ").piece(n).append(", 0.0)");
        return switch (r.fn()) {
            case VAR_POP -> v -> v.append("(CASE WHEN ").piece(n).append(" > 0 THEN ").piece(cxx).append(" / ")
                    .piece(n).append(" END)");
            case STDDEV_POP -> v -> v.append("(CASE WHEN ").piece(n).append(" > 0 THEN sqrt(").piece(cxx)
                    .append(" / ").piece(n).append(") END)");
            case VAR_SAMP, VARIANCE -> v -> v.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxx)
                    .append(" / (").piece(n).append(" - 1) END)");
            case STDDEV_SAMP, STDDEV -> v -> v.append("(CASE WHEN ").piece(n).append(" > 1 THEN sqrt(").piece(cxx)
                    .append(" / (").piece(n).append(" - 1)) END)");
            case COVAR_POP -> v -> v.append("(CASE WHEN ").piece(n).append(" > 0 THEN ").piece(cxy).append(" / ")
                    .piece(n).append(" END)");
            case COVAR_SAMP -> v -> v.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxy).append(" / (")
                    .piece(n).append(" - 1) END)");
            default -> v -> v.append("(CASE WHEN ").piece(n).append(" > 1 THEN ").piece(cxy).append(" / sqrt(")
                    .piece(cxx).append(" * ").piece(cyy).append(") END)");
        };
    }

    /** A window's {@code OVER (...)}. A RANGE frame with an offset takes ONE numeric key: a date or date-time key is
     *  its Julian day number (an interval offset in days). */
    private SqlWriter windowOver(SqlWriter writer, SqlExpr.WindowCall w) {
        writer.append(" OVER (");
        boolean any = false;
        if (!w.partitionBy().isEmpty()) {
            writer.append("PARTITION BY ").list(w.partitionBy());
            any = true;
        }
        SqlExpr.WindowCall.Frame f = w.frame();
        boolean days = false;
        if (!w.orderBy().isEmpty()) {
            writer.append(any ? " " : "").append("ORDER BY ");
            boolean interval = f != null && (intervalBound(f.from()) || intervalBound(f.to()));
            if (interval) {
                if (w.orderBy().size() != 1) {
                    throw new DialectCapability("a RANGE frame over " + w.orderBy().size() + " keys reached SQLite");
                }
                SqlSelect.SortKey k = w.orderBy().get(0);
                SqlType kt = typeOf(k.expr());
                if (kt != SqlType.Scalar.DATE && kt != SqlType.Scalar.TIMESTAMP) {
                    throw new DialectCapability("an interval RANGE frame over a " + kt + " key reached SQLite");
                }
                days = true;
                boolean nullsFirst = k.nullOrder() != null ? k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                        : !k.ascending();
                writer.append("julianday(").expr(k.expr(), 0).append(")").append(k.ascending() ? "" : " DESC")
                        .append(nullsFirst ? " NULLS FIRST" : " NULLS LAST");
            } else {
                writer.join(w.orderBy(), ", ", this::sortKey);
            }
            any = true;
        }
        if (f != null) {
            boolean inDays = days;
            writer.append(any ? " " : "").append(f.kind().toString()).append(" BETWEEN ")
                    .append(frameBound(f.from(), inDays)).append(" AND ").append(frameBound(f.to(), inDays));
        }
        return writer.append(")");
    }

    private static boolean intervalBound(SqlExpr.WindowCall.Frame.Bound b) {
        return b instanceof SqlExpr.WindowCall.Frame.Bound.IntervalPreceding
                || b instanceof SqlExpr.WindowCall.Frame.Bound.IntervalFollowing;
    }

    private String frameBound(SqlExpr.WindowCall.Frame.Bound b, boolean days) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> intervalDays(p.n(), p.unit(), days)
                    + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing fo -> intervalDays(fo.n(), fo.unit(), days)
                    + " FOLLOWING";
            default -> bound(b);
        };
    }

    private static String intervalDays(long n, String unit, boolean days) {
        Long micros = FRAME_MICROS.get(unit.toUpperCase(Locale.ROOT));
        if (!days || micros == null) {
            throw new DialectCapability("a " + unit + " window frame offset over this key reached SQLite");
        }
        return String.valueOf(n * (double) micros / 86400000000.0);
    }

    /** DurationUnit name to microseconds (months and years have no fixed length). */
    private static final Map<String, Long> FRAME_MICROS = Map.of("WEEKS", 7L * 86400000000L, "DAYS", 86400000000L,
            "HOURS", 3600000000L, "MINUTES", 60000000L, "SECONDS", 1000000L, "MILLISECONDS", 1000L);

    // ==================================================================
    // JSON constructors
    // ==================================================================

    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        writer.append("json_object(");
        List<SqlExpr> kv = j.kv();
        for (int i = 0; i < kv.size(); i++) {
            SqlExpr x = kv.get(i);
            writer.append(i > 0 ? ", " : "").piece(i % 2 == 0 ? w -> w.expr(x, 0) : json(x));
        }
        return writer.append(")");
    }

    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("json_array(").join(j.elements(), ", ", (w, x) -> w.piece(json(x))).append(")");
    }

    /** A JSON array of the values, NULLs kept as JSON nulls; over no rows the empty array. */
    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        writer.append("json_group_array(").piece(json(j.value()));
        if (!j.orderKeys().isEmpty()) {
            writer.append(" ORDER BY ").join(j.orderKeys(), ", ",
                    (w, k) -> w.expr(k.expr(), 0).append(k.desc() ? " DESC" : " ASC").append(" NULLS LAST"));
        }
        return writer.append(")");
    }

    // ==================================================================
    // Plan parameters: a date or date-time is bound as its text
    // ==================================================================

    @Override
    protected RenderedStatement.TypeSpelling holeType(com.legend.sql.ValueKind kind) {
        return switch (kind) {
            case DATE -> new RenderedStatement.TypeSpelling("TEXT", RenderedStatement.Digits.NONE);
            case DATE_TIME, DATE_TIME_NANOS ->
                    new RenderedStatement.TypeSpelling("TEXT", RenderedStatement.Digits.NONE);
            case INTEGER, DECIMAL, FLOATING -> super.holeType(kind);
        };
    }

    // ==================================================================
    // Values: what the driver hands back
    // ==================================================================

    /** A boolean arrives as 1 or 0; a struct or a map as its JSON text, decoded to the platform's map of its fields; a
     *  root decimal as its text at its scale ({@link #delivered}), read exact. */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            @com.legend.base.Nullable SqlType type) {
        if (type == SqlType.Scalar.BOOLEAN && jdbcValue instanceof Number n) {
            return n.longValue() != 0;
        }
        if (jdbcValue instanceof String s && (type instanceof SqlType.Struct || type instanceof SqlType.Map)
                && s.startsWith("{")) {
            return com.legend.sql.Json.parse(s);
        }
        if (jdbcValue instanceof String s && type instanceof SqlType.Decimal) {
            return new java.math.BigDecimal(s);
        }
        return jdbcValue;
    }

    // ==================================================================
    // DDL (test fixtures)
    // ==================================================================

    /** A table of a schema is named {@code "schema.table"} in the session's one database. SQLite has no CREATE SCHEMA;
     *  an attached database is a namespace, but ATTACH has no IF NOT EXISTS and the metamodel's seed creates its schema
     *  once per table (measured: "database metamodel is already in use"). */
    @Override
    protected String tableName(String name, com.legend.sql.NameQuoting quoting) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            return super.tableName(name, quoting);
        }
        // one delimited name, its case kept, whichever parts the store declared quoted
        return delimited(name);
    }

    private String qualified(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting) {
        return schema == null || schema.isEmpty() || "default".equals(schema) ? storedName(table, quoting.table())
                : delimited(schema + "." + table);
    }

    /** A schema names no object of its own ({@link #tableName}): its creation and its drop are no statement. */
    @Override
    public String render(com.legend.sql.SqlDdl ddl) {
        return switch (ddl) {
            case com.legend.sql.SqlDdl.CreateSchema cs -> "SELECT 1;";
            case com.legend.sql.SqlDdl.DropSchema ds -> "SELECT 1;";
            case com.legend.sql.SqlDdl.CreateTable ct -> {
                StringBuilder sb = new StringBuilder(ct.temporary() ? "Create Temporary Table " : "Create Table ")
                        .append(qualified(ct.schema(), ct.table(), ct.quoting())).append("(");
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < ct.columns().size(); i++) {
                    com.legend.sql.SqlDdl.Column col = ct.columns().get(i);
                    sb.append(i > 0 ? ", " : "").append(ddlIdentifier(col.name(), col.declaredQuoted())).append(' ')
                            .append(ddlType(col.type()))
                            .append(col.primaryKey() || col.notNull() ? " NOT NULL" : " NULL");
                    if (col.primaryKey()) {
                        keys.add(ddlKeyIdentifier(col.name(), col.declaredQuoted()));
                    }
                }
                if (!keys.isEmpty()) {
                    sb.append(", PRIMARY KEY(").append(String.join(",", keys)).append(')');
                }
                yield sb.append(");").toString();
            }
            case com.legend.sql.SqlDdl.DropTable dt ->
                    "Drop table if exists " + qualified(dt.schema(), dt.table(), dt.quoting()) + ";";
        };
    }

    /** Rows in and out of a table, named as {@link #tableName} names it. */
    @Override
    public String render(com.legend.sql.SqlDml dml) {
        return switch (dml) {
            case com.legend.sql.SqlDml.InsertValues iv -> {
                SqlWriter writer = newWriter().append("INSERT INTO ").append(qualified(iv.schema(), iv.table(), iv.quoting()))
                        .append(columns(iv.columns(), iv.quotedColumns())).append(" VALUES ");
                for (int r = 0; r < iv.rows().size(); r++) {
                    writer.append(r == 0 ? "(" : ", (").list(iv.rows().get(r)).append(")");
                }
                RenderedStatement insert = writer.append(";").statement();
                if (!insert.binds().isEmpty()) {
                    throw new DialectCapability("plan parameters " + insert.binds() + " in a DML row: a row holds"
                            + " values, and a DML statement is text");
                }
                yield insert.sql();
            }
            case com.legend.sql.SqlDml.InsertFromTable it -> "INSERT INTO " + qualified(it.schema(), it.table(), it.quoting())
                    + columns(it.columns(), it.quotedColumns()) + " SELECT * FROM " + ident(it.source()) + ";";
            case com.legend.sql.SqlDml.DeleteAll da -> "DELETE FROM " + qualified(da.schema(), da.table(), da.quoting()) + ";";
            case com.legend.sql.SqlDml.CreateTempTableAs ct -> tempTableAs(ct);
        };
    }

    /** {@inheritDoc} SQLite runs the standard spelling (measured: ReservedNamesGenerationTest), its query a root as
     *  {@link #render(SqlQuery)} writes one, so its labels are delivered as the trail's text shows them. */
    @Override
    protected String tempTableAs(com.legend.sql.SqlDml.CreateTempTableAs ct) {
        roots.push(ct.query().outputs());
        rootSelects.push(new IdentityHashMap<>());
        try {
            return createTemporaryTableAs(ct);
        } finally {
            roots.pop();
            rootSelects.pop();
        }
    }

    private String columns(List<String> columns, java.util.Set<String> quoted) {
        return columns.isEmpty() ? ""
                : " (" + String.join(", ", columns.stream().map(c -> storedName(c, quoted.contains(c))).toList()) + ")";
    }

    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT, SMALLINT, TINYINT, INTEGER, BIT -> "INTEGER";
                case FLOAT, DOUBLE, REAL -> "REAL";
                case TIMESTAMP, DATE, TIME, TIMESTAMP_NS, JSON, VARCHAR, OTHER, DISTINCT, ARRAY, OBJECT -> "TEXT";
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> z.kind().toUpperCase(Locale.ROOT).contains("BINARY")
                    ? "BLOB" : "TEXT";
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> "REAL";
        };
    }

    /** {@inheritDoc} Its JSON_TYPE answers DuckDB's names (a CASE over json_type). */
    @Override
    protected String jsonKindName(com.legend.sql.JsonKind kind) {
        return switch (kind) {
            case OBJECT -> "OBJECT";
            case ARRAY -> "ARRAY";
            case NULL -> "NULL";
            case STRING -> "VARCHAR";
            case INTEGER -> "BIGINT";
            case UNSIGNED_INTEGER -> "UBIGINT";
            case FLOAT -> "DOUBLE";
            case BOOLEAN -> "BOOLEAN";
        };
    }

    /** {@inheritDoc} Refused, as TYPEOF is here (its type names are SQLite's storage classes). */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        throw new DialectCapability("typeof reached SQLite: its type names are SQLite's storage classes");
    }

    /** {@inheritDoc} Its FLOAT is a REAL value, and NUMERIC its name for a NULL value (measured: no storage class). */
    @Override
    public String wireTypeName(@com.legend.base.Nullable String driverTypeName) {
        return WireNames.fold(driverTypeName == null ? null : switch (driverTypeName) {
            case "FLOAT" -> "DOUBLE";
            case "NUMERIC" -> "NULL";
            default -> driverTypeName;
        });
    }

    /** {@inheritDoc} Its own types, the platform's by design (2026-10-11): a VALUE has a storage class, a column none,
     *  and the driver names a column by its first value's (INTEGER, FLOAT, TEXT, and NUMERIC for a NULL, measured). A
     *  boolean is an INTEGER (1 or 0); a date, a date-time, a list, a struct, a map and a Variant are their text, and a
     *  root decimal its text at its scale; a NULL first value claims no type. */
    @Override
    public boolean deliversOwn(SqlType label, String wire) {
        return switch (wire) {
            case "NULL" -> true;
            case "INTEGER" -> label == SqlType.Scalar.BOOLEAN || label == SqlType.Scalar.HUGEINT;
            case "VARCHAR" -> label == SqlType.Scalar.DATE || label == SqlType.Scalar.TIMESTAMP
                    || label == SqlType.Scalar.TIMESTAMPTZ || label == SqlType.Scalar.JSON
                    || label instanceof SqlType.Array || label instanceof SqlType.Struct || label instanceof SqlType.Map
                    || label instanceof SqlType.Decimal;
            default -> false;
        };
    }
}
