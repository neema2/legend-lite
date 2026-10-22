// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DateFmt;
import com.legend.sql.DatePart;
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
import static com.legend.sql.dialect.ExplosionMarkers.isUnnest;
import static com.legend.sql.dialect.ExplosionMarkers.markerColumn;
import static com.legend.sql.dialect.ExplosionMarkers.nullList;
import static com.legend.sql.dialect.ExplosionMarkers.nullOverNullList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The Google BigQuery EXECUTION dialect (GoogleSQL; 2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md). A sibling of
 * {@link Postgres} and {@link Databricks} over {@link AnsiSqlRenderer}: it changes nothing the other dialects render.
 * Every spelling here was measured on the account (project {@code legend-integration-testing}, Google's JDBC driver
 * 1.6.0, product name {@code Google BigQuery}) before it was trusted; the platform's semantics are DuckDB's, measured
 * on DuckDB 1.4.4 where BigQuery's differ.
 *
 * <p>What differs from DuckDB, measured:
 * <ul>
 *   <li>a string literal reads backslash escapes and refuses {@code ''} (a syntax error): a quote is {@code \'}, a
 *       backslash {@code \\}, a control character {@code \xHH};</li>
 *   <li>{@code 1.5} is a FLOAT64 (DuckDB's is DECIMAL(2,1)): a Float or Decimal literal is {@code BIGNUMERIC '1.5'}
 *       (exact, 38 digits each side), so arithmetic over it stays exact as the numeric charter's Rule 1 wants;
 *       parameterized types ({@code NUMERIC(10,2)}) are refused in a CAST, so a decimal cast is a rounded
 *       BIGNUMERIC;</li>
 *   <li>{@code /} raises on zero (DuckDB's double division answers Infinity): {@code IEEE_DIVIDE}; {@code MOD} has no
 *       FLOAT64 overload; {@code ROUND(x, n, 'ROUND_HALF_EVEN')} exists over NUMERIC/BIGNUMERIC only;</li>
 *   <li>{@code CONCAT}, {@code GREATEST} and {@code LEAST} answer NULL for any NULL argument (DuckDB skips them);</li>
 *   <li>there is no VALUES clause ({@code UNNEST(ARRAY<STRUCT<...>>[STRUCT(...), ...])}), no bare {@code UNION}
 *       ({@code UNION DISTINCT}), no OFFSET without LIMIT, no lambdas, no lateral derived table (a lateral join
 *       unnests its rows as an array of structs, {@link #lateral}), no MAP type (a map is its entries,
 *       {@code ARRAY<STRUCT<key, value>>}), and WITH RECURSIVE only at the top (a closed fold is hoisted there,
 *       {@link FoldsAsRecursiveCtes});</li>
 *   <li>an array holds no array, and a query RESULT holds no array with a NULL element (inside a query it may), and a
 *       NULL array arrives as an empty one; lists are ARRAY&lt;T&gt; and every list operation is a correlated subquery
 *       over {@code UNNEST(xs) AS e WITH OFFSET AS o} (Postgres's {@code WITH ORDINALITY} recipe);</li>
 *   <li>an aggregate's ORDER BY and a RANGE window refuse a non-default NULLS clause: NULLs are placed by a leading
 *       {@code (k) IS NULL} key there;</li>
 *   <li>column names fold case-insensitively ({@code SELECT 1 AS a, 2 AS A} labels the second {@code A_1}): every
 *       identifier is backticked, labels keep their case;</li>
 *   <li>{@code PERCENTILE_CONT}/{@code PERCENTILE_DISC} are analytic only; grouped quantiles index the group's
 *       sorted {@code ARRAY_AGG}; there is no MODE (a mode is its most counted value, {@link #modesCounted});</li>
 *   <li>the session zone is UTC by default ({@code @@time_zone}); Pure's naive DateTime is DATETIME, TIMESTAMPTZ is
 *       TIMESTAMP; JSON objects keep their keys SORTED (DuckDB keeps insertion order).</li>
 * </ul>
 *
 * <p>Every {@link SqlFn} and {@link SqlAgg.Fn} is decided HERE by an exhaustive switch: its spelling, a shared
 * helper of the base named in its arm, or a {@link DialectCapability} wall. A wall is honest; a guessed spelling
 * is a silent wrong answer.
 */
public final class BigQuery extends AnsiSqlRenderer {

    /** BigQuery (GoogleSQL; 2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md): backticks; GoogleSQL's reserved
     *  keywords. The {@code BigQuery} dialect quotes every identifier, so the list decides no spelling there. */
    public static final Lexicon LEXICON = new Lexicon('`', Set.of(
            "all", "and", "any", "array", "as", "asc", "assert_rows_modified", "at", "between", "by", "case", "cast",
            "collate", "contains", "create", "cross", "cube", "current", "default", "define", "desc", "distinct",
            "else", "end", "enum", "escape", "except", "exclude", "exists", "extract", "false", "fetch", "following",
            "for", "from", "full", "group", "grouping", "groups", "hash", "having", "if", "ignore", "in", "inner",
            "intersect", "interval", "into", "is", "join", "lateral", "left", "like", "limit", "lookup", "merge",
            "natural", "new", "no", "not", "null", "nulls", "of", "on", "or", "order", "outer", "over", "partition",
            "preceding", "proto", "qualify", "range", "recursive", "respect", "right", "rollup", "rows", "select",
            "set", "some", "struct", "tablesample", "then", "to", "treat", "true", "unbounded", "union", "unnest",
            "using", "when", "where", "window", "with", "within"));

    /** BigQuery (2026-10-11): the portable strategies (static pivot and as-of emulation, explode placements, list
     *  reductions fused), as on Postgres: arrays are native but lambda-less; FULL OUTER is native. */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(false, true, true, true);

    public BigQuery() {
        super(LEXICON);
    }

    // ==================================================================
    // Session and structure
    // ==================================================================

    /** The engine's H2 text is refused here: BigQuery has no translation of it (decision D2: raw text runs on the
     *  database its connection declares; only the engine's H2 text is translated, for H2, DuckDB and Postgres). */
    @Override
    public String fromEngineText(String statement) {
        throw new DialectCapability("the engine's H2 text reached BigQuery, which has no translation of it");
    }

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "BOOL";
            case INTEGER, BIGINT -> "INT64";
            case HUGEINT -> "BIGNUMERIC";
            case DOUBLE -> "FLOAT64";
            case VARCHAR, LITERAL, TEMPORAL_TEXT -> "STRING";
            case DATE -> "DATE";
            case TIMESTAMP -> "DATETIME";
            case TIMESTAMPTZ -> "TIMESTAMP";
            case JSON -> "JSON";
            case TIME -> "TIME";
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** Nothing to set: a BigQuery session's zone is UTC unless a statement names one (measured {@code @@time_zone}),
     *  which is the platform's naive-UTC contract -- CURRENT_DATETIME() is the UTC wall time, a TIMESTAMP reads as
     *  its UTC DATETIME. (A SET would need a session-mode connection; each JDBC statement is its own job.) */
    @Override
    public List<String> sessionSetup() {
        return List.of();
    }

    /** No native pivot is used: a static pivot is emulated by the carrier strategies (GROUP BY with one filtered
     *  aggregate per value), so a dynamic pivot is discovered first. */
    // the probe plan's facts (2026-10-20): BigQuery: a transaction needs a session script; 10,000 columns in a query result (its documented quota)
    @Override
    public KeysSnapshot keysSnapshot() {
        return KeysSnapshot.NONE;
    }

    @Override
    public @com.legend.base.Nullable Integer resultColumnLimit() {
        return 10000;
    }

    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    /** The portable carrier strategies, a fold that reads nothing of its row as a top-level recursive expression (WITH
     *  RECURSIVE is top-level only, measured), the substring start clamp (BigQuery reads a start of 0 as 1), averages
     *  delivered as DOUBLE (an average of a NUMERIC or BIGNUMERIC is one), and a constant GROUP BY or ORDER BY key as a
     *  typed expression (BigQuery reads an integer one as a position), and a large repeated subexpression written once
     *  (a statement is capped at 1,024K characters: a float's text canon read a dozen times, nested, wrote 2.9M, measured;
     *  the binding's one-row derived table under a scalar subquery correlates here, measured). QUALIFY and
     *  PERCENTILE_* OVER are native; no root numeric cast: a parameterized type cannot be cast to, and the driver
     *  reports no NUMERIC scale. */
    /** A Float's literal spelling over the double's text bound once (its text is an expression here: a five-element
     *  mixed list's sort was 436 KB, measured 2026-10-11). */
    @Override
    protected boolean bindsFloatText() {
        return true;
    }

    /** A Float's print forms over the value and its text bound once: the portable spellings read the text nine times
     *  (Pure's) and over a thousand (legend-engine's), each read this dialect's text of a double, its own subquery (the
     *  served statement for {@code 1.5 + 2.25} was 321 KB, measured 2026-10-22). */
    @Override
    protected boolean bindsFloatPrint() {
        return true;
    }

    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS), new FoldsAsRecursiveCtes(),
                new SubstringClamp(),
                new ExplosionMarkers.LateralUnnestAsMarker(), new ExplosionMarkers.SelectListUnnestAsLateral(),
                new ExplosionMarkers.StarOverExplosionUnwrapped(),
                new AggregatesDeliverDouble(Set.of(SqlAgg.Fn.AVG)), new ConstantKeysAsExpressions(),
                new CommonSubexpressions());
    }

    @Override
    protected boolean supportsQualify() {
        return true;
    }

    /** QUALIFY needs no WHERE, GROUP BY or HAVING beside it (measured). */
    @Override
    protected SqlWriter appendQualify(SqlWriter writer, SqlSelect s, int depth) {
        return nl(writer, depth).append("QUALIFY ").expr(java.util.Objects.requireNonNull(s.qualify(), "qualify"), 0);
    }

    @Override
    protected String starExceptKeyword() {
        return "EXCEPT";
    }

    @Override
    protected String rowOrderColumn() {
        throw new DialectCapability("row order reached BigQuery, which has no row-order column");
    }

    @Override
    protected String asOfJoinClause() {
        throw new DialectCapability("an as-of join reached BigQuery, which has none (the carrier strategies emulate"
                + " the witnessed shapes)");
    }

    /** A static pivot never reaches here (the carrier strategies emulate it); BigQuery's native PIVOT names each
     *  column {@code alias_value} (measured), not the platform's {@code value__|__alias}. */
    @Override
    protected SqlWriter pivotSource(SqlWriter writer, SqlSource.Pivot p, int depth) {
        throw new DialectCapability("a pivot reached BigQuery unemulated (dynamic, several keys, or a COUNT(*)"
                + " using): its native PIVOT names columns alias_value");
    }

    /** UNION is UNION ALL or UNION DISTINCT (a bare UNION is a syntax error, measured); a branch with its own ORDER BY
     *  or LIMIT, or a union, is parenthesized. */
    @Override
    protected SqlWriter query(SqlWriter writer, SqlQuery q, int depth) {
        if (!(q instanceof SqlUnion u)) {
            return super.query(writer, q, depth);
        }
        String op = u.all() ? "UNION ALL" : "UNION DISTINCT";
        for (int i = 0; i < u.branches().size(); i++) {
            if (i > 0) {
                nl(writer, depth).append(op);
                nl(writer, depth);
            }
            SqlQuery b = u.branches().get(i);
            boolean wrap = !(b instanceof SqlSelect s) || !s.orderBy().isEmpty() || s.limit() != null
                    || s.offset() != null;
            if (wrap) {
                writer.append("(");
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

    /** The labels of the statement being rendered, as planned (its root's outputs before this dialect's passes). */
    private final Deque<List<com.legend.sql.OutputCol>> roots = new ArrayDeque<>();

    @Override
    public String render(SqlQuery query) {
        roots.push(query.outputs());
        try {
            return super.render(query);
        } finally {
            roots.pop();
        }
    }

    @Override
    public RenderedStatement renderStatement(SqlQuery query) {
        roots.push(query.outputs());
        try {
            return super.renderStatement(query);
        } finally {
            roots.pop();
        }
    }

    /**
     * OFFSET is only allowed after a LIMIT (measured): an offset alone takes the largest one. A select that only
     * explodes lists (an explosion marker, ExplosionMarkers) is its reads over the marker's positions, in order; the
     * markers of a FROM are in scope of its column reads, and a star over one is spelled out (the positions' own
     * column is no exploded column).
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
            SqlSelect.SortKey position = new SqlSelect.SortKey(SqlExpr.Column.derived(null, alias), true, null, null);
            return select(writer, new SqlSelect(ps, false, new SqlSource.Subselect(s, alias, null), null, List.of(),
                    null, null, List.of(position), null, null, List.of()), depth);
        }
        if (s.from() instanceof SqlSource.Dual && (s.where() != null || !s.groupBy().isEmpty() || s.having() != null)) {
            // "Query without FROM clause cannot have a WHERE clause" (measured): over one row instead
            SqlSelect one = new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(1), "__one", null)),
                    false, new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of());
            return select(writer, new SqlSelect(s.projections(), s.distinct(), new SqlSource.Subselect(one, "__dual",
                    null), s.where(), s.groupBy(), s.having(), s.qualify(), s.orderBy(), s.limit(), s.offset(),
                    s.outputs()), depth);
        }
        Map<SqlExpr, SqlExpr> counts = new LinkedHashMap<>();
        s = modesCounted(s, counts);
        Map<String, Map<String, SqlExpr>> scope = new LinkedHashMap<>();
        lateralsOf(s.from(), scope);
        SqlSelect spelled = rootNulls(scope.isEmpty() ? s : starsSpelled(s), depth);
        laterals.push(scope);
        modeCounts.push(counts);
        try {
            return super.select(writer, spelled.offset() != null && spelled.limit() == null
                    ? spelled.withLimit(Long.MAX_VALUE) : spelled, depth);
        } finally {
            modeCounts.pop();
            laterals.pop();
        }
    }

    /** Per select being written, each MODE in it (a grouped reducer, or a window call) and the column counting its
     *  value's rows ({@link #modesCounted}). */
    private final java.util.Deque<Map<SqlExpr, SqlExpr>> modeCounts = new java.util.ArrayDeque<>();

    /**
     * {@code s}, its FROM given a count per MODE: BigQuery has no mode aggregate or window function, and unnests no
     * aggregate inside its own select (measured). A mode is the most frequent non-NULL value; each one's value is
     * counted over the rows of its group (a grouped mode: the select's GROUP BY keys) or its partition (a window mode),
     * {@code COUNT(v) OVER (PARTITION BY keys, v)}, in a select over the FROM under the FROM's own alias, so every
     * reference of {@code s} still reads it. Then the mode is the value of the most counted row, written where it is
     * ({@link #modeArm}). A FLOAT64 key is partitioned by its {@code TO_JSON_STRING} (BigQuery partitions by no
     * FLOAT64, measured; that text is the double's shortest round trip, so equal values share it and distinct ones do
     * not). On a tie, the least value: DuckDB's own tie is the earliest row scanned, an order SQL rows do not have.
     */
    private SqlSelect modesCounted(SqlSelect s, Map<SqlExpr, SqlExpr> counts) {
        Map<SqlExpr, List<SqlExpr>> modes = new LinkedHashMap<>();
        List<SqlExpr> roots = new ArrayList<>();
        s.projections().forEach(p -> roots.add(p.expr()));
        if (s.having() != null) {
            roots.add(s.having());
        }
        if (s.qualify() != null) {
            roots.add(s.qualify());
        }
        for (SqlExpr e : roots) {
            collectModes(e, s.groupBy(), modes);
        }
        if (modes.isEmpty()) {
            return s;
        }
        SqlSource from = s.from();
        String alias = from.alias();
        if (from instanceof SqlSource.Join || alias == null || from.outputs().isEmpty()) {
            throw new DialectCapability("a MODE over a " + from.getClass().getSimpleName() + " reached BigQuery: its"
                    + " counts are a select over one aliased source");
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (com.legend.sql.OutputCol oc : from.outputs()) {
            ps.add(new SqlSelect.Projection(SqlExpr.Column.of(alias, oc), oc.name(), oc));
        }
        int k = 0;
        for (var m : modes.entrySet()) {
            String name = "__mc" + k++;
            SqlExpr v = modeValue(m.getKey());
            List<SqlExpr> partition = new ArrayList<>();
            for (SqlExpr key : m.getValue()) {
                partition.add(partitionKey(key));
            }
            partition.add(partitionKey(v));
            ps.add(new SqlSelect.Projection(new SqlExpr.WindowCall(new SqlAgg.Reducer(SqlAgg.Fn.COUNT, List.of(v),
                    false, List.of()), partition, List.of(), null), name, null));
            counts.put(m.getKey(), SqlExpr.Column.derived(alias, name));
        }
        SqlSelect counted = new SqlSelect(ps, false, from, null, List.of(), null, null, List.of(), null, null,
                List.of());
        return s.withFrom(new SqlSource.Subselect(counted, alias, null));
    }

    /** Each MODE under {@code e}: a grouped reducer with the select's keys, a window call with its partition. */
    private static void collectModes(SqlExpr e, List<SqlExpr> groupBy, Map<SqlExpr, List<SqlExpr>> out) {
        if (e instanceof SqlAgg.Reducer r && r.fn() == SqlAgg.Fn.MODE) {
            out.put(r, groupBy);
            return;
        }
        if (e instanceof SqlExpr.WindowCall w && w.fn() instanceof SqlAgg.Reducer r && r.fn() == SqlAgg.Fn.MODE) {
            if (!w.orderBy().isEmpty() || w.frame() != null) {
                throw new DialectCapability("a MODE over an ordered or framed window reached BigQuery");
            }
            out.put(w, w.partitionBy());
            return;
        }
        for (SqlExpr c : e.children()) {
            collectModes(c, groupBy, out);
        }
    }

    /** The value a mode counts. */
    private static SqlExpr modeValue(SqlExpr mode) {
        SqlAgg.Reducer r = mode instanceof SqlExpr.WindowCall w ? (SqlAgg.Reducer) w.fn() : (SqlAgg.Reducer) mode;
        if (r.args().size() != 1 || r.distinct()) {
            throw new DialectCapability("a mode of " + r.args().size() + " arguments reached BigQuery");
        }
        return r.args().get(0);
    }

    /** {@code key} as a window partitions by it: a FLOAT64 by its JSON text (see {@link #modesCounted}), spelled as the
     *  casts to JSON and to text this dialect writes {@code TO_JSON_STRING(TO_JSON(key))}. */
    private static SqlExpr partitionKey(SqlExpr key) {
        return typeOf(key) == SqlType.Scalar.DOUBLE
                ? new SqlExpr.Cast(new SqlExpr.Cast(key, SqlType.Scalar.JSON), SqlType.Scalar.VARCHAR) : key;
    }

    /** A mode where it is written: the value of its most counted row, the least value on a tie, NULLs ignored --
     *  grouped, {@code ARRAY_AGG(v IGNORE NULLS ORDER BY count DESC, v LIMIT 1)[SAFE_OFFSET(0)]} (NULL for a group of
     *  NULLs); over a window, {@code FIRST_VALUE(v IGNORE NULLS) OVER (PARTITION BY .. ORDER BY count DESC, v)} over
     *  the whole partition (measured on the PCT's own rows). */
    private SqlWriter modeArm(SqlWriter writer, SqlExpr mode) {
        SqlExpr count = modeCounts.isEmpty() ? null : modeCounts.peek().get(mode);
        if (count == null) {
            throw new IllegalStateException("a MODE reached BigQuery's writer uncounted: modesCounted did not run");
        }
        SqlExpr v = modeValue(mode);
        if (mode instanceof SqlExpr.WindowCall w) {
            writer.append("FIRST_VALUE(").expr(v, 0).append(" IGNORE NULLS) OVER (");
            if (!w.partitionBy().isEmpty()) {
                writer.append("PARTITION BY ").list(w.partitionBy()).append(" ");
            }
            return writer.append("ORDER BY ").expr(count, 0).append(" DESC, ").expr(v, 0)
                    .append(" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");
        }
        return writer.append("ARRAY_AGG(").expr(v, 0).append(" IGNORE NULLS ORDER BY ").expr(count, 0).append(" DESC, ")
                .expr(v, 0).append(" LIMIT 1)[SAFE_OFFSET(0)]");
    }

    /** At the root, a value NULL by its shape ({@link #nullValued}) is its label's NULL (a bare NULL is an INT64 here,
     *  and {@code UNNEST(NULL)} is refused, measured). */
    private SqlSelect rootNulls(SqlSelect s, int depth) {
        List<com.legend.sql.OutputCol> labels = roots.peek();
        if (depth != 0 || labels == null || labels.size() != s.projections().size()
                || s.projections().stream().noneMatch(p -> nullValued(p.expr()))) {
            return s;
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (int i = 0; i < s.projections().size(); i++) {
            SqlSelect.Projection p = s.projections().get(i);
            ps.add(nullValued(p.expr())
                    ? new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.NullLit(), labels.get(i).type()), p.alias(),
                            p.out())
                    : p);
        }
        return new SqlSelect(ps, s.distinct(), s.from(), s.where(), s.groupBy(), s.having(), s.qualify(), s.orderBy(),
                s.limit(), s.offset(), s.outputs());
    }

    /** NULL by its shape: a list's element, min or max over the NULL list, or a COALESCE of NULLs (a bare NULL is an
     *  INT64 here: COALESCE(NULL, NULL) is an INT64 column under a VARCHAR label, measured). */
    private static boolean nullValued(SqlExpr e) {
        return nullOverNullList(e) || e instanceof SqlExpr.Call c && c.fn() == SqlFn.COALESCE
                && c.args().stream().allMatch(x -> x instanceof SqlExpr.NullLit);
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
        if (src instanceof SqlSource.Join j) {
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
                    return element(c, java.util.Objects.requireNonNull(cols.get(c.name()), "list"), c.table());
                }
            } else {
                List<Map.Entry<String, Map<String, SqlExpr>>> owners = scope.entrySet().stream()
                        .filter(e -> e.getValue().containsKey(c.name())).toList();
                if (owners.size() == 1) {
                    return element(c, java.util.Objects.requireNonNull(owners.get(0).getValue().get(c.name()), "list"),
                            owners.get(0).getKey());
                }
            }
        }
        return null;
    }

    /** {@code xs[SAFE_OFFSET(position)]}; a NULL list's element is its type's NULL (a bare NULL is an INT64 here). */
    private SqlWriter.Piece element(SqlExpr.Column c, SqlExpr xs, String position) {
        if (nullList(xs)) {
            SqlType t = typeOf(c);
            return t == null ? w -> w.append("NULL") : w -> w.append("CAST(NULL AS " + castTypeName(t) + ")");
        }
        return w -> w.expr(xs, 0).append("[SAFE_OFFSET(").append(aliasIdent(position)).append(")]");
    }

    /** A marker's positions: {@code UNNEST(GENERATE_ARRAY(0, n - 1))} over its longest list (DuckDB's parallel UNNEST
     *  pads the shorter with NULL; a NULL list is none); a lateral one joins its left side correlated (CROSS JOIN
     *  UNNEST, or LEFT JOIN UNNEST keeping an empty list's row). Any other lateral derived table is {@link #lateral}. */
    @Override
    protected SqlWriter source(SqlWriter writer, SqlSource src, int depth) {
        if (src instanceof SqlSource.Join j && j.right() instanceof SqlSource.Subselect sub && isMarker(sub.inner())
                && (j.kind() == SqlSource.Join.Kind.CROSS_LATERAL || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)) {
            if (j.on() != null && !(j.on() instanceof SqlExpr.BoolLit b && b.value())) {
                throw new DialectCapability("a lateral explosion with a join condition reached BigQuery");
            }
            source(writer, j.left(), depth);
            nl(writer, depth).append(j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? "LEFT JOIN " : "CROSS JOIN ")
                    .piece(positions((SqlSelect) sub.inner())).append(" AS ").append(aliasIdent(sub.alias()));
            return j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? writer.append(" ON TRUE") : writer;
        }
        if (src instanceof SqlSource.Subselect sub && isMarker(sub.inner())) {
            return writer.piece(positions((SqlSelect) sub.inner())).append(" AS ").append(aliasIdent(sub.alias()));
        }
        if (src instanceof SqlSource.Join j && (j.kind() == SqlSource.Join.Kind.CROSS_LATERAL
                || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)) {
            return lateral(writer, j, depth);
        }
        return super.source(writer, src, depth);
    }

    /**
     * A lateral derived table. BigQuery has none -- {@code LATERAL} is "not supported", a correlated subquery in FROM
     * is "Unrecognized name", {@code CROSS APPLY} a syntax error (all measured) -- but an UNNEST correlates, and an
     * ARRAY subquery may read the outer row anywhere inside it, nested derived tables included (measured). So the
     * derived table's rows become an array of structs, each row one struct, unnested where the row is: {@code CROSS
     * JOIN UNNEST(ARRAY(SELECT AS STRUCT * FROM (q))) AS r}, its columns {@code r}'s fields (a {@code SELECT *} over
     * it expands them, measured). A LEFT lateral is {@code LEFT JOIN ... ON <its condition>}, keeping a row with no
     * match as the NULL-extended row (measured); a condition on a CROSS one makes it an inner {@code JOIN}.
     */
    private SqlWriter lateral(SqlWriter writer, SqlSource.Join j, int depth) {
        if (!(j.right() instanceof SqlSource.Subselect sub)) {
            throw new DialectCapability("a lateral join over a " + j.right().getClass().getSimpleName()
                    + " reached BigQuery");
        }
        boolean trivial = j.on() == null || (j.on() instanceof SqlExpr.BoolLit b && b.value());
        boolean left = j.kind() == SqlSource.Join.Kind.LEFT_LATERAL;
        source(writer, j.left(), depth);
        nl(writer, depth).append(left ? "LEFT JOIN " : trivial ? "CROSS JOIN " : "JOIN ")
                .append("UNNEST(ARRAY(SELECT AS STRUCT * FROM (");
        nl(writer, depth + 1);
        query(writer, sub.inner(), depth + 1);
        nl(writer, depth).append("))) AS ").append(aliasIdent(sub.alias()));
        if (left || !trivial) {
            writer.append(" ON ");
            if (trivial) {
                writer.append("TRUE");
            } else {
                writer.expr(java.util.Objects.requireNonNull(j.on(), "on"), 0);
            }
        }
        return writer;
    }

    private static SqlWriter.Piece positions(SqlSelect m) {
        List<SqlExpr> lists = m.projections().stream().map(p -> ((SqlExpr.Call) p.expr()).args().get(0))
                .filter(xs -> !nullList(xs)).toList();
        return w -> {
            w.append("UNNEST(GENERATE_ARRAY(0, ");
            if (lists.isEmpty()) {
                w.append("0");
            } else if (lists.size() == 1) {
                w.append("IFNULL(ARRAY_LENGTH(").expr(lists.get(0), 0).append("), 0)");
            } else {
                w.append("GREATEST(").join(lists, ", ", (v, l) -> v.append("IFNULL(ARRAY_LENGTH(").expr(l, 0)
                        .append("), 0)")).append(")");
            }
            w.append(" - 1))");
        };
    }

    /** No VALUES clause: the rows as a typed array of structs, unnested ({@code STRUCT(...)} rows coerce to the
     *  array's declared field types, so a NULL cell is typed; a single-column row needs STRUCT, a bare tuple of one is
     *  a parenthesized value -- measured). */
    @Override
    protected SqlWriter valuesSource(SqlWriter writer, SqlSource.Values v) {
        if (v.outputs().size() != v.columns().size()) {
            throw new DialectCapability("a VALUES source without a declared type per column reached BigQuery");
        }
        writer.append("(SELECT * FROM UNNEST(ARRAY<STRUCT<");
        for (int i = 0; i < v.columns().size(); i++) {
            int col = i;
            List<SqlExpr> cells = v.rows().stream().map(r -> r.get(col)).toList();
            writer.append(i > 0 ? ", " : "").append(ident(v.columns().get(i))).append(" ")
                    .append(heldType(v.outputs().get(i).type(), cells));
        }
        writer.append(">>[");
        for (int r = 0; r < v.rows().size(); r++) {
            writer.append(r > 0 ? ", " : "").append("STRUCT(").list(v.rows().get(r)).append(")");
        }
        return writer.append("])) AS ").append(aliasIdent(v.alias()));
    }

    // ==================================================================
    // Lexical: identifiers, literals, types
    // ==================================================================

    /** Every identifier in backticks (column names fold case-insensitively, labels keep their case); a double-quoted
     *  declaration ({@code "date"}) is its inner name. */
    @Override
    protected String ident(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")
                && !name.substring(1, name.length() - 1).replace("\"\"", "").contains("\"")) {
            return delimited(name.substring(1, name.length() - 1).replace("\"\"", "\""));
        }
        return delimited(name);
    }

    /** A backtick inside a quoted identifier is escaped by a backslash (GoogleSQL has no doubled-quote escape). */
    @Override
    protected String delimited(String name) {
        return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`";
    }

    /** Backslash escapes; {@code ''} is a syntax error (measured), so a quote is {@code \'}; a quoted literal holds no
     *  raw newline, so control characters are escaped ({@code \x00} is NUL, measured); a {@code ?} is {@code \x3f}:
     *  the driver counts every {@code ?} as a parameter, inside a literal too ("One or more parameters missing",
     *  measured). */
    @Override
    protected String stringLit(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 2).append('\'');
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\' -> sb.append("\\\\");
                case '\'' -> sb.append("\\'");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '?' -> sb.append("\\x3f");
                default -> {
                    if (ch < 0x20 || ch == 0x7f) {
                        sb.append(String.format(Locale.ROOT, "\\x%02x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
            }
        }
        return sb.append('\'').toString();
    }

    /** A naive date-time is a DATETIME; it holds microseconds (a literal with nine digits is refused, measured), so
     *  finer digits are TRUNCATED as DuckDB's TIMESTAMP does. */
    @Override
    protected String timestampLit(String iso) {
        String t = iso.replace('T', ' ');
        int dot = t.lastIndexOf('.');
        if (dot >= 0) {
            int end = dot + 1;
            while (end < t.length() && Character.isDigit(t.charAt(end))) {
                end++;
            }
            if (end - dot - 1 > 6) {
                t = t.substring(0, dot + 7) + t.substring(end);
            }
        }
        return "DATETIME '" + t + "'";
    }

    /** NUMERIC CHARTER Rule 1: a Float literal is typed exact-decimal by the database -- {@code 1.5} alone is a
     *  FLOAT64 here, so it is spelled {@code BIGNUMERIC '1.5'} ({@code 0.1 + 0.2} is 0.3, measured); a magnitude
     *  BIGNUMERIC cannot hold, and NaN and the infinities, are FLOAT64. */
    @Override
    protected String floatLiteral(double v) {
        if (Double.isNaN(v)) {
            return "CAST('nan' AS FLOAT64)";
        }
        if (Double.isInfinite(v)) {
            return v > 0 ? "CAST('inf' AS FLOAT64)" : "CAST('-inf' AS FLOAT64)";
        }
        java.math.BigDecimal plain = com.legend.sql.SqlTyping.floatDecimal(v);
        if (plain != null && fitsBigNumeric(plain)) {
            return "BIGNUMERIC '" + plain.toPlainString() + "'";
        }
        return "CAST('" + v + "' AS FLOAT64)";
    }

    private String decimalLiteral(java.math.BigDecimal v) {
        if (!fitsBigNumeric(v)) {
            throw new DialectCapability("a decimal literal " + v.toPlainString() + " reached BigQuery, whose BIGNUMERIC"
                    + " holds 38 digits each side of the point");
        }
        return "BIGNUMERIC '" + v.toPlainString() + "'";
    }

    private static boolean fitsBigNumeric(java.math.BigDecimal v) {
        java.math.BigDecimal s = v.stripTrailingZeros();
        return Math.max(s.scale(), 0) <= 38 && s.precision() - s.scale() <= 38;
    }

    /** A number BIGNUMERIC holds exactly (a list or a VALUES column of these keeps its exact kind). */
    private static boolean exactNumber(SqlExpr e) {
        return switch (e) {
            case SqlExpr.IntLit i -> true;
            case SqlExpr.NullLit n -> true;
            case SqlExpr.DecimalLit d -> fitsBigNumeric(d.value());
            case SqlExpr.FloatLit f -> {
                java.math.BigDecimal p = Double.isFinite(f.value()) ? com.legend.sql.SqlTyping.floatDecimal(f.value())
                        : null;
                yield p != null && fitsBigNumeric(p);
            }
            default -> false;
        };
    }

    /** The type a list or a VALUES column of {@code type} holds {@code cells} as: a Float column of exact literals
     *  is BIGNUMERIC (DuckDB types them DECIMAL), anything else its own type. */
    private String heldType(SqlType type, List<SqlExpr> cells) {
        if (type == SqlType.Scalar.DOUBLE && cells.stream().allMatch(BigQuery::exactNumber)) {
            return "BIGNUMERIC";
        }
        return bqType(type);
    }

    /** A type's BigQuery spelling: a decimal is an unparameterized BIGNUMERIC (a CAST takes no NUMERIC(p,s),
     *  measured); a list of lists and a map have none. */
    private String bqType(SqlType t) {
        return switch (t) {
            case SqlType.Scalar s -> castTypeName(s);
            case SqlType.Decimal d -> "BIGNUMERIC";
            case SqlType.Array a -> {
                if (a.element() instanceof SqlType.Array) {
                    throw nestedList();
                }
                yield "ARRAY<" + bqType(a.element()) + ">";
            }
            case SqlType.Map m -> mapTypeName(bqType(m.key()), bqType(m.value()));
            case SqlType.Struct st -> {
                StringBuilder sb = new StringBuilder("STRUCT<");
                for (int i = 0; i < st.fields().size(); i++) {
                    sb.append(i > 0 ? ", " : "").append(ident(st.fields().get(i).name())).append(' ')
                            .append(bqType(st.fields().get(i).type()));
                }
                yield sb.append('>').toString();
            }
        };
    }

    private static DialectCapability nestedList() {
        return new DialectCapability("a list of lists reached BigQuery, whose arrays hold no array (measured: \"nested"
                + " arrays are not supported\")");
    }

    @Override
    protected String arrayTypeName(String element) {
        if (element.startsWith("ARRAY<")) {
            throw nestedList();
        }
        return "ARRAY<" + element + ">";
    }

    /** A map is its entries in order, {@code ARRAY<STRUCT<key K, value V>>}: BigQuery has no MAP type, and this is how
     *  it loads one (a repeated key_value record). */
    @Override
    protected String mapTypeName(String key, String value) {
        return "ARRAY<STRUCT<" + ident("key") + " " + key + ", " + ident("value") + " " + value + ">>";
    }

    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        List<java.util.Map.Entry<String, String>> fields = structFields(st);
        StringBuilder sb = new StringBuilder("STRUCT<");
        for (int i = 0; i < fields.size(); i++) {
            sb.append(i > 0 ? ", " : "").append(fields.get(i).getKey()).append(' ').append(fields.get(i).getValue());
        }
        return sb.append('>').toString();
    }

    /** Alias-less projections label EXPLICITLY from the declared output: BigQuery labels an expression {@code f0_}. */
    @Override
    protected @com.legend.base.Nullable String implicitLabel(SqlSelect.Projection p) {
        com.legend.sql.OutputCol out = p.out();
        return out != null ? aliasIdent(out.name()) : super.implicitLabel(p);
    }

    /** A NULL of {@code type}, or a bare one where the type is not known. */
    private String nullOf(@com.legend.base.Nullable SqlType type) {
        return type == null ? "NULL" : "CAST(NULL AS " + bqType(type) + ")";
    }

    /** A projection that IS error() is raised in its column's type: a bare ERROR is typed INT64 here, and a read of
     *  the column as its type is refused before the statement runs (testNewDateError's date(2016, 13), "Invalid cast
     *  from INT64 to DATETIME", on the lane 2026-10-21, where every other lane raised the test's own message). A
     *  DATETIME is the raise typed as text and cast: an IF or a CASE of a DATETIME with an ERROR branch fails before it
     *  raises ("Function is not implemented. id: cast_datetime_to_datetime", with or without a cast around it), where
     *  one of a STRING, a DATE or a TIMESTAMP raises the message (measured on the lane the same day). */
    @Override
    protected SqlWriter projectionValue(SqlWriter writer, SqlSelect.Projection p) {
        if (p.out() != null && p.expr() instanceof SqlExpr.Call call && call.fn() == SqlFn.ERROR) {
            String type = bqType(p.out().type());
            boolean datetime = type.equals("DATETIME");
            writer.append(datetime ? "CAST(IF(TRUE, " : "IF(TRUE, ");
            raiseEnvelope(writer, call.args(), "ERROR");
            return writer.append(datetime ? ", CAST(NULL AS STRING)) AS DATETIME)" : ", CAST(NULL AS " + type + "))");
        }
        return super.projectionValue(writer, p);
    }

    /** A NULL under a list, struct or decimal slot is typed as the slot (a bare NULL is an INT64). */
    @Override
    protected SqlWriter projection(SqlWriter writer, SqlSelect.Projection p) {
        com.legend.sql.OutputCol out = p.out();
        if (p.expr() instanceof SqlExpr.NullLit && out != null && (out.type() instanceof SqlType.Array
                || out.type() instanceof SqlType.Struct || out.type() instanceof SqlType.Decimal)) {
            writer.append("CAST(NULL AS ").append(bqType(out.type())).append(")");
            String alias = p.alias();
            String label = alias != null ? aliasIdent(alias) : implicitLabel(p);
            return label == null ? writer : writer.append(" AS ").append(label);
        }
        return super.projection(writer, p);
    }

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        return switch (e) {
            case SqlExpr.DecimalLit d -> writer.append(decimalLiteral(d.value()));
            case SqlExpr.ArrayLit a -> arrayLiteral(writer, a);
            // DuckDB's list(v ORDER BY k): ascending, NULLs last
            case SqlExpr.OrderedListAgg ola -> writer.append("ARRAY_AGG(").expr(ola.value(), 0).append(" ORDER BY ")
                    .piece(orderKey(w -> w.expr(ola.orderBy(), 0), true, false)).append(")");
            case SqlExpr.Membership m when m.collection() instanceof SqlExpr.PlanParam p -> throw listParameter(p);
            case SqlExpr.Quantified q -> writer.piece(quantified(q));
            case SqlExpr.Column c when explodedColumn(c) != null ->
                    writer.piece(java.util.Objects.requireNonNull(explodedColumn(c), "exploded"));
            default -> super.expr(writer, e, parentPrec);
        };
    }

    /** A CASE mixing a text branch with others is text, as DuckDB coerces it ("all THEN/ELSE arguments must be
     *  coercible to a common type", measured: INT64 and STRING are not); each other branch is cast to STRING, in this
     *  dialect's own text form. */
    @Override
    protected SqlWriter caseExpr(SqlWriter writer, SqlExpr.Case c) {
        List<SqlExpr> branches = new ArrayList<>();
        c.whens().forEach(w -> branches.add(w.then()));
        if (c.otherwise() != null) {
            branches.add(c.otherwise());
        }
        boolean text = branches.stream().anyMatch(BigQuery::textual);
        boolean mixed = text && branches.stream().anyMatch(b -> !textual(b) && !(b instanceof SqlExpr.NullLit));
        if (!mixed) {
            return super.caseExpr(writer, c);
        }
        java.util.function.Function<SqlExpr, SqlExpr> asText = b -> textual(b) || b instanceof SqlExpr.NullLit ? b
                : new SqlExpr.Cast(b, SqlType.Scalar.VARCHAR);
        writer.append("CASE");
        for (SqlExpr.Case.When w : c.whens()) {
            writer.append(" WHEN ").expr(w.condition(), 0).append(" THEN ").expr(asText.apply(w.then()), 0);
        }
        if (c.otherwise() != null) {
            writer.append(" ELSE ").expr(asText.apply(c.otherwise()), 0);
        }
        return writer.append(" END");
    }

    private static boolean textual(SqlExpr e) {
        SqlType t = typeOf(e);
        return e instanceof SqlExpr.StringLit || t == SqlType.Scalar.VARCHAR || t == SqlType.Scalar.LITERAL
                || t == SqlType.Scalar.TEMPORAL_TEXT || t == SqlType.Scalar.DECIMAL_TEXT;
    }

    /** A list plan parameter: binding an ARRAY through this driver is unmeasured (DuckDB's {@code = ANY(?)} is no
     *  GoogleSQL; {@code IN UNNEST(?)} would be). */
    private static DialectCapability listParameter(SqlExpr.PlanParam p) {
        return new DialectCapability("list plan parameter '" + p.name() + "' reached BigQuery: binding an array"
                + " parameter through its driver is unmeasured");
    }

    /** {@code v cmp ANY|ALL (subquery)}: GoogleSQL has no quantified comparison, so the subquery's values are
     *  counted, in SQL's three-valued logic -- ANY is TRUE when one comparison is, FALSE over no rows or when none is
     *  TRUE and none unknown, else NULL; ALL is its dual. The values are the subquery's one column, whatever its name
     *  ({@code UNNEST(ARRAY(subquery))}: BigQuery has no derived-column alias list). */
    private SqlWriter.Piece quantified(SqlExpr.Quantified q) {
        SqlExpr cmp = SqlExpr.Call.of(q.comparison(), q.value(), SqlExpr.Column.derived(null, "__qc"));
        boolean any = q.quantifier() == SqlExpr.Quantifier.ANY;
        return w -> {
            w.append("(SELECT CASE WHEN ");
            if (any) {
                w.append("COUNTIF(").expr(cmp, 0).append(") > 0 THEN TRUE WHEN COUNT(*) = 0 THEN FALSE");
            } else {
                w.append("COUNT(*) = 0 THEN TRUE WHEN COUNTIF(NOT (").expr(cmp, 0).append(")) > 0 THEN FALSE");
            }
            w.append(" WHEN COUNTIF((").expr(cmp, 0).append(") IS NULL) > 0 THEN NULL ELSE ").append(any ? "FALSE"
                    : "TRUE").append(" END FROM UNNEST(ARRAY(");
            inline(w, q.subquery());
            w.append(")) AS ").append(param("__qc")).append(")");
        };
    }

    // ==================================================================
    // Lists: ARRAY<T>, lambda-less -- each operation a correlated subquery over UNNEST(xs) WITH OFFSET
    // ==================================================================

    private static @com.legend.base.Nullable SqlType typeOf(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
    }

    /** The element type of a list, or null for a value that is no list. */
    private static @com.legend.base.Nullable SqlType listElement(SqlExpr e) {
        return typeOf(e) instanceof SqlType.Array a ? a.element() : null;
    }

    /** Whether a NULL list is the bottom-typed empty list {@code []}. */
    private static boolean bottom(SqlExpr xs) {
        return xs instanceof SqlExpr.CompactList cl ? bottom(cl.list())
                : xs.type() instanceof com.legend.sql.TypeFact.Bottom;
    }

    /** A lambda parameter (or an unnest's element or offset) as its references spell it. */
    private String param(String name) {
        return columnRef(SqlExpr.Column.derived(null, name));
    }

    /** {@code UNNEST(xs) AS e WITH OFFSET AS o}: each element bound to {@code e}, its 0-based offset to {@code o}
     *  (an unnest of a NULL list is no rows, measured). A NULL list is typed, since a bare NULL is an INT64 here and
     *  UNNEST refuses it ("Values referenced in UNNEST must be arrays", measured): as a list of its element type, or
     *  -- the empty list {@code []}, bottom-typed -- of Nil's carrier, VARCHAR (the platform's own, PureSql: the
     *  column its element would fill is labelled so). Any other NULL list of no known element type is walled. */
    private SqlWriter.Piece elements(SqlExpr xs, String e, String o) {
        SqlType known = listElement(xs);
        if (nullList(xs) && known == null && !bottom(xs)) {
            throw new DialectCapability("a NULL list of unknown element type reached BigQuery");
        }
        SqlType element = known != null ? known : SqlType.Scalar.VARCHAR;
        SqlWriter.Piece list = nullList(xs)
                ? w -> w.append("CAST(NULL AS " + castTypeName(new SqlType.Array(element)) + ")")
                : w -> w.expr(xs, 0);
        return w -> w.append("UNNEST(").piece(list).append(") AS ").append(param(e)).append(" WITH OFFSET AS ")
                .append(param(o));
    }

    /** {@code body}, or NULL for a NULL list (DuckDB's list functions answer NULL there; an ARRAY subquery over it
     *  would be empty). */
    private static SqlWriter.Piece unlessNull(SqlExpr xs, SqlWriter.Piece body) {
        return w -> w.append("(CASE WHEN ").expr(xs, 4).append(" IS NULL THEN NULL ELSE ").piece(body).append(" END)");
    }

    /** An empty list of {@code e}'s element type, typed when the type is known. */
    private String emptyList(SqlExpr e) {
        SqlType el = listElement(e);
        return el == null ? "[]" : "ARRAY<" + bqType(el) + ">[]";
    }

    private static SqlExpr.Lambda lambdaOf(SqlExpr e, SqlFn fn) {
        if (e instanceof SqlExpr.Lambda l && !l.params().isEmpty()) {
            return l;
        }
        throw new DialectCapability(fn + " without a lambda reached BigQuery");
    }

    /** The offset alias of a lambda's element: unique per lambda, so an inner list operation in its body never
     *  shadows it. */
    private static String offsetOf(SqlExpr.Lambda l) {
        return l.params().get(0) + "__o";
    }

    /** A lambda's body over the elements of {@code xs} (the first parameter, the unnest's alias); DuckDB's index
     *  parameter, 1-based, read from the 0-based offset. An element of a JSON array is a JSON value, and the lowering
     *  leaves a lambda's parameter untyped: so it is typed here, and a cast of it reads JSON as JSON is read
     *  ({@code INT64(j)}, not {@code CAST(j AS INT64)}, which GoogleSQL refuses: "Invalid cast from JSON to INT64",
     *  measured). */
    private static SqlExpr lambdaBody(SqlExpr xs, SqlExpr.Lambda l) {
        SqlExpr body = l.body();
        // the empty list [] has no element to apply the body to: the body is a NULL of its own type (its parameter
        // would be Nil's VARCHAR carrier, which no comparison with a number type-checks; a bare NULL operand of = is
        // refused, measured)
        if (nullList(xs) && listElement(xs) == null && bottom(xs)) {
            if (typeOf(body) == null) {
                throw new DialectCapability("a lambda of no known type over the empty list reached BigQuery");
            }
            return new SqlExpr.Cast(new SqlExpr.NullLit(), java.util.Objects.requireNonNull(typeOf(body)));
        }
        if (listElement(xs) == SqlType.Scalar.JSON) {
            body = substitute(body, l.params().get(0), SqlExpr.Column.of(null, l.params().get(0),
                    SqlType.Scalar.JSON, true, com.legend.sql.OutputCol.Origin.DERIVED));
        }
        if (l.params().size() < 2) {
            return body;
        }
        return substitute(body, l.params().get(1), SqlExpr.Call.of(SqlFn.PLUS,
                SqlExpr.Column.derived(null, offsetOf(l)), new SqlExpr.IntLit(1)));
    }

    private static SqlExpr substitute(SqlExpr e, String name, SqlExpr with) {
        if (e instanceof SqlExpr.Column c && c.table() == null && c.name().equals(name)) {
            return with;
        }
        List<SqlExpr> kids = e.children();
        if (kids.isEmpty()) {
            return e;
        }
        List<SqlExpr> mapped = new ArrayList<>(kids.size());
        boolean changed = false;
        for (SqlExpr k : kids) {
            SqlExpr m = substitute(k, name, with);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? e.withChildren(mapped) : e;
    }

    /** {@code ARRAY(SELECT select FROM UNNEST(xs) AS e WITH OFFSET AS o [WHERE where] ORDER BY o)}, NULL for a NULL
     *  list: the elements kept in list order. */
    private SqlWriter.Piece overElements(SqlExpr xs, String e, String o, SqlWriter.Piece select,
            SqlWriter.@com.legend.base.Nullable Piece where) {
        return unlessNull(xs, w -> {
            w.append("ARRAY(SELECT ").piece(select).append(" FROM ").piece(elements(xs, e, o));
            if (where != null) {
                w.append(" WHERE ").piece(where);
            }
            w.append(" ORDER BY ").append(param(o)).append(")");
        });
    }

    /** {@code (SELECT agg(e) FROM UNNEST(xs) AS e WITH OFFSET AS o)}: a list reduced as rows (an empty or NULL list
     *  reduces over no rows). MODE is the most frequent non-NULL element, the first to occur among ties (DuckDB's
     *  list_mode, measured). */
    private SqlWriter.Piece overRows(SqlExpr xs, SqlAgg.Fn fn, List<SqlExpr> extras) {
        String e = param("__e");
        String o = param("__o");
        if (fn == SqlAgg.Fn.MODE) {
            if (!extras.isEmpty()) {
                throw new DialectCapability("a mode with arguments reached BigQuery");
            }
            return w -> w.append("(SELECT " + e + " FROM ").piece(elements(xs, "__e", "__o"))
                    .append(" WHERE " + e + " IS NOT NULL GROUP BY " + e + " ORDER BY COUNT(*) DESC, MIN(" + o
                            + ") LIMIT 1)");
        }
        SqlExpr el = listElement(xs) == null ? SqlExpr.Column.derived(null, "__e") : SqlExpr.Column.param("__e", xs);
        List<SqlExpr> args = new ArrayList<>();
        args.add(el);
        args.addAll(extras);
        List<SqlSelect.SortKey> order = fn == SqlAgg.Fn.STRING_AGG || fn == SqlAgg.Fn.LIST
                ? List.of(new SqlSelect.SortKey(SqlExpr.Column.derived(null, "__o"), true, null, null)) : List.of();
        SqlAgg.Reducer r = new SqlAgg.Reducer(fn, args, false, order);
        boolean dbl = fn == SqlAgg.Fn.AVG;
        return w -> {
            w.append(dbl ? "(SELECT CAST(" : "(SELECT ");
            reducer(w, r);
            w.append(dbl ? " AS FLOAT64)" : "").append(" FROM ").piece(elements(xs, "__e", "__o")).append(")");
        };
    }

    /** A list literal, typed when its element type is known (so an empty one has a type); a Float list of exact
     *  literals is BIGNUMERIC, as DuckDB types it DECIMAL. A NULL element is held inside the query (only a RESULT
     *  refuses one). */
    private SqlWriter arrayLiteral(SqlWriter writer, SqlExpr.ArrayLit a) {
        SqlType el = listElement(a);
        for (SqlExpr x : a.elements()) {
            if (x instanceof SqlExpr.ArrayLit || typeOf(x) instanceof SqlType.Array) {
                throw nestedList();
            }
        }
        if (el == null) {
            return writer.append("[").list(a.elements()).append("]");
        }
        if (el instanceof SqlType.Array) {
            throw nestedList();
        }
        return writer.append("ARRAY<").append(heldType(el, a.elements())).append(">[").list(a.elements()).append("]");
    }

    @Override
    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        return writer.append("[").list(elements).append("]");
    }

    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit st) {
        // typed when declared, so a NULL field has its type (STRUCT<a INT64, b STRING>(1, NULL), measured)
        if (typeOf(st) instanceof SqlType.Struct s && s.fields().size() == st.fields().size()) {
            return writer.append(bqType(s)).append("(")
                    .join(st.fields(), ", ", (w, f) -> w.expr(f.value(), 0)).append(")");
        }
        return writer.append("STRUCT(")
                .join(st.fields(), ", ", (w, f) -> w.expr(f.value(), 0).append(" AS ").append(ident(f.name())))
                .append(")");
    }

    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        return writer.append("(").expr(g.source(), 0).append(").").append(ident(g.field()));
    }

    @Override
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> args) {
        SqlType st = typeOf(args.get(0));
        if (!(st instanceof SqlType.Struct struct) || !(args.get(1) instanceof SqlExpr.StringLit name)) {
            throw new DialectCapability("struct_insert over an untyped struct reached BigQuery");
        }
        writer.append("STRUCT(");
        boolean first = true;
        for (SqlType.Struct.Field f : struct.fields()) {
            if (f.name().equals(name.value())) {
                continue;
            }
            writer.append(first ? "" : ", ").append("(").expr(args.get(0), 0).append(").").append(ident(f.name()))
                    .append(" AS ").append(ident(f.name()));
            first = false;
        }
        return writer.append(first ? "" : ", ").expr(args.get(2), 0).append(" AS ").append(ident(name.value()))
                .append(")");
    }

    /** exists([]) is false: Pure's empty-collection semantics (LOGICAL_OR over no rows is NULL). */
    @Override
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "LOGICAL_OR", "FALSE", SqlFn.LIST_EXISTS);
    }

    /** forAll([]) is true. */
    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "LOGICAL_AND", "TRUE", SqlFn.LIST_FOR_ALL);
    }

    private SqlWriter listPredicate(SqlWriter writer, List<SqlExpr> args, String agg, String empty, SqlFn fn) {
        SqlExpr.Lambda l = lambdaOf(args.get(1), fn);
        return writer.append("COALESCE((SELECT " + agg + "(").expr(lambdaBody(args.get(0), l), 0).append(") FROM ")
                .piece(elements(args.get(0), l.params().get(0), offsetOf(l))).append("), " + empty + ")");
    }

    /** No duplicates iff the distinct count is the count (a NULL element is no distinct value, as DuckDB's
     *  len(list_distinct(x)) = len(x)); an empty or NULL list is distinct. */
    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        String e = param("__e");
        return writer.append("COALESCE((SELECT COUNT(*) = COUNT(DISTINCT " + e + ") FROM UNNEST(")
                .expr(args.get(0), 0).append(") AS " + e + "), TRUE)");
    }

    /** DuckDB's list_contains: NULL for a NULL list or needle; else found or not -- a NULL element never makes an
     *  absent needle NULL ({@code 4 IN UNNEST([1, NULL])} is NULL, measured, so no IN). */
    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        String e = param("__e");
        return writer.append("(CASE WHEN ").expr(m.collection(), 4).append(" IS NULL OR ").expr(m.needle(), 4)
                .append(" IS NULL THEN NULL ELSE EXISTS(SELECT 1 FROM UNNEST(").expr(m.collection(), 0)
                .append(") AS " + e + " WHERE " + e + " = ").expr(m.needle(), 5).append(") END)");
    }

    @Override
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        return writer.piece(overRows(rc.collection(), rc.reducer(), rc.extras()));
    }

    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("an unnest in a select list reached BigQuery, which has no set-returning"
                + " projection (an explode is placed in FROM by the carrier strategies)");
    }

    @Override
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        throw new DialectCapability("a fold that reads its row reached BigQuery, which has no lambdas and no correlated"
                + " recursion (WITH RECURSIVE is top-level only, measured; a closed fold is hoisted there)");
    }

    /** Pure's splitPart: the non-empty tokens, 1-based, NULL past the end. */
    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        String t = param("__t");
        return writer.append("ARRAY(SELECT " + t + " FROM UNNEST(SPLIT(").expr(a.get(0), 0).append(", ")
                .expr(a.get(1), 0).append(")) AS " + t + " WITH OFFSET AS " + param("__to") + " WHERE " + t
                        + " <> '' ORDER BY " + param("__to") + ")[SAFE_ORDINAL(CAST(").expr(a.get(2), 0)
                .append(" AS INT64))]");
    }

    // ==================================================================
    // Variant: BigQuery's JSON type
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

    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() != 1) {
            throw new DialectCapability("toVariant of " + a.size() + " arguments reached BigQuery");
        }
        return isJson(a.get(0)) ? writer.expr(a.get(0), 0) : writer.append("TO_JSON(").expr(a.get(0), 0).append(")");
    }

    /** A member by key, or a 0-based index (-1 the last; past either end NULL, measured): the JSON subscript, which
     *  answers NULL for a missing key or an out-of-range index, and NULL for a key of an array. */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        SqlExpr key = args.get(1);
        if (key instanceof SqlExpr.StringLit root && "$".equals(root.value())) {
            return writer.expr(args.get(0), 0);
        }
        if (isText(key)) {
            return writer.append("(").expr(args.get(0), 0).append(")[").expr(key, 0).append("]");
        }
        return writer.append("(").expr(args.get(0), 0).append(")[CASE WHEN ").expr(key, 4)
                .append(" < 0 THEN ARRAY_LENGTH(JSON_QUERY_ARRAY(").expr(args.get(0), 0).append(")) + CAST(")
                .expr(key, 0).append(" AS INT64) ELSE CAST(").expr(key, 0).append(" AS INT64) END]");
    }

    /** A Variant array's elements, each a Variant (NULL for a non-array). */
    @Override
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        if (listElement(args.get(0)) != null) {
            return writer.expr(args.get(0), 0);
        }
        return writer.append("JSON_QUERY_ARRAY(").expr(args.get(0), 0).append(")");
    }

    /** A value read out of a Variant: a member read as text is its scalar text (an object or array its JSON text); the
     *  whole value as text is its JSON (CAST(JSON AS STRING) is refused, measured); to a scalar, its text cast. */
    private SqlWriter.Piece jsonAsText(SqlExpr v, boolean member) {
        if (!member) {
            return w -> w.append("TO_JSON_STRING(").expr(v, 0).append(")");
        }
        return w -> w.append("(CASE WHEN JSON_TYPE(").expr(v, 0).append(") IN ('object', 'array') THEN TO_JSON_STRING(")
                .expr(v, 0).append(") ELSE JSON_VALUE(").expr(v, 0).append(") END)");
    }

    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        SqlType target = c.target();
        SqlExpr v = c.value();
        if (target == SqlType.Scalar.TEMPORAL_TEXT || target == SqlType.Scalar.DECIMAL_TEXT) {
            return writer.expr(v, 0);
        }
        if (target == SqlType.Scalar.JSON) {
            if (isJson(v)) {
                return writer.expr(v, 0);
            }
            return isText(v) ? writer.append("PARSE_JSON(").expr(v, 0).append(")")
                    : writer.append("TO_JSON(").expr(v, 0).append(")");
        }
        if (isJson(v)) {
            boolean member = v instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET;
            if (target == SqlType.Scalar.VARCHAR || target == SqlType.Scalar.LITERAL) {
                return writer.piece(jsonAsText(v, member));
            }
            if (target instanceof SqlType.Array arr) {
                if (arr.element() instanceof SqlType.Array) {
                    throw nestedList();
                }
                SqlWriter.Piece each = arr.element() == SqlType.Scalar.JSON ? w -> w.append(param("__j"))
                        : w -> w.piece(castFromText(w2 -> w2.append("JSON_VALUE(" + param("__j") + ")"),
                                arr.element()));
                return writer.append("(CASE WHEN JSON_TYPE(").expr(v, 0).append(") = 'array' THEN ARRAY(SELECT ")
                        .piece(each).append(" FROM UNNEST(JSON_QUERY_ARRAY(").expr(v, 0).append(")) AS ")
                        .append(param("__j")).append(" WITH OFFSET AS ").append(param("__jo")).append(" ORDER BY ")
                        .append(param("__jo")).append(") END)");
            }
            if (target instanceof SqlType.Map m) {
                return writer.piece(variantAsMap(v, m));
            }
            if (target instanceof SqlType.Struct) {
                throw new DialectCapability("a cast of a Variant to " + target + " reached BigQuery");
            }
            return writer.piece(castFromText(w -> w.append("JSON_VALUE(").expr(v, 0).append(")"), target));
        }
        if (target instanceof SqlType.Map m) {
            return writer.piece(mapAs(v, m));
        }
        if (target instanceof SqlType.Decimal d) {
            // DuckDB's decimal cast rounds to the scale (half away from zero, as ROUND does here)
            return writer.append("ROUND(CAST(").expr(v, 0).append(" AS BIGNUMERIC), " + d.scale() + ")");
        }
        if (target instanceof SqlType.Array arr) {
            SqlType from = listElement(v);
            // a text element as LITERAL is the text itself (LITERAL is STRING physically): the mixed sort's ordered ids
            // (2026-10-13, PR #34's review: element-wise, it wrote the aggregate twice)
            if (v instanceof SqlExpr.NullLit || from == null
                    || from == SqlType.Scalar.VARCHAR && arr.element() == SqlType.Scalar.LITERAL) {
                return writer.append("CAST(").expr(v, 0).append(" AS ").append(bqType(target)).append(")");
            }
            // element-wise: BigQuery casts between arrays of compatible element types only (measured)
            String e = param("__c");
            return writer.piece(unlessNull(v, w -> w.append("ARRAY(SELECT ")
                    .piece(castTo(x -> x.append(e), from, arr.element())).append(" FROM UNNEST(").expr(v, 0)
                    .append(") AS " + e + " WITH OFFSET AS " + param("__co") + " ORDER BY " + param("__co") + ")")));
        }
        if (target == SqlType.Scalar.VARCHAR || target == SqlType.Scalar.LITERAL) {
            return writer.piece(asText(v));
        }
        return writer.piece(castTo(w -> w.expr(v, 0), typeOf(v), target));
    }

    /** {@code value} (of type {@code from}, when known) cast to {@code to}. */
    private SqlWriter.Piece castTo(SqlWriter.Piece value, @com.legend.base.Nullable SqlType from, SqlType to) {
        // BigQuery's DATETIME and TIMESTAMP hold microseconds and refuse a text with more fraction digits ("Invalid
        // datetime string \"2026-01-08T00:00:00.000000000\"": the judges' grid decode writes nine, channel B's
        // lead-adjust tests on the lane, 2026-10-21); the fraction is cut to six, as DuckDB's cast reads such a text
        if (from == SqlType.Scalar.VARCHAR && (to == SqlType.Scalar.TIMESTAMP || to == SqlType.Scalar.TIMESTAMPTZ)) {
            return w -> w.append("CAST(REGEXP_REPLACE(").piece(value).append(", ")
                    .append(stringLit("(\\.[0-9]{6})[0-9]+")).append(", ").append(stringLit("\\1")).append(") AS ")
                    .append(bqType(to)).append(")");
        }
        if (to instanceof SqlType.Decimal d) {
            return w -> w.append("ROUND(CAST(").piece(value).append(" AS BIGNUMERIC), " + d.scale() + ")");
        }
        if (to == SqlType.Scalar.VARCHAR || to == SqlType.Scalar.LITERAL) {
            return textOf(value, from);
        }
        return w -> w.append("CAST(").piece(value).append(" AS ").append(bqType(to)).append(")");
    }

    /** A Variant scalar's text cast to {@code to}. */
    private SqlWriter.Piece castFromText(SqlWriter.Piece text, SqlType to) {
        if (to == SqlType.Scalar.VARCHAR || to == SqlType.Scalar.LITERAL) {
            return text;
        }
        return castTo(text, SqlType.Scalar.VARCHAR, to);
    }

    /** A value as text, as DuckDB's CAST to VARCHAR prints it. Only a string literal is written as it is: a column's
     *  text type may be a label the value does not carry (a pivot's columns, measured: "all THEN/ELSE arguments must
     *  be coercible", an INT64 under a VARCHAR label), and a cast of a STRING is none. */
    private SqlWriter.Piece asText(SqlExpr e) {
        if (e instanceof SqlExpr.StringLit) {
            return w -> w.expr(e, 0);
        }
        return textOf(w -> w.expr(e, 0), e instanceof SqlExpr.FloatLit ? SqlType.Scalar.DOUBLE : typeOf(e));
    }

    /** DuckDB's text of a value of {@code type}: a whole Float keeps its {@code .0} (CAST(1.0 AS STRING) is '1' here,
     *  '1.0' on DuckDB, measured); a date-time trims trailing fraction zeros and its point ({@code %E*S}, measured
     *  equal to DuckDB's); a nested value is its JSON text; anything else its CAST. */
    /**
     * A Float's text as DuckDB's cast writes it: the shortest digits that read back as the same double (Java's rule),
     * plain where the exponent is from -4 to 15 with an integral value's {@code .0}, else with its exponent. BigQuery's
     * own cast writes seventeen significant digits (pi() '3.1415926535897931', 1.1 / 2.1 '0.52380952380952384': three
     * PCT tests passing at the merge-base failed on the plan route, which has the database print a Float), and FORMAT's
     * %.16g and %.17g are not correctly rounded (185.46332835263038 as '185.46332835263039'; 630 of 1,837 doubles wrong,
     * measured on the lane 2026-10-21). The plain range's digits come from the double's exact decimal instead: its
     * BIGNUMERIC (38 decimals), rounded to 15, 16 and 17 significant digits, the first whose text reads back as the value
     * taken (a double's shortest digits are at most 17, and at 15 or fewer they are its 15-digit rounding); a tie is
     * rounded to the even digit, as Java breaks one (-754686444319231.25 is -754686444319231.2; ROUND's own half away
     * from zero wrote .3 for six of 1,171 plain-range doubles, measured on the lane against 441f207a). Elsewhere
     * the text is the cast's, which reads back as the value. The value is bound once, in a one-row derived table under a
     * scalar subquery (the form the Float canon's binding takes here, {@link #bindsFloatText}).
     */
    private SqlWriter.Piece floatText(SqlWriter.Piece x) {
        String v = param("__fv");
        String n = param("__fn");
        String e = param("__fe");
        String r = param("__fr");
        SqlWriter.Piece rounded = w -> {
            w.append("CASE");
            for (int digits = 15; digits <= 16; digits++) {
                String round = "ROUND(" + n + ", " + (digits - 1) + " - " + e + ", 'ROUND_HALF_EVEN')";
                w.append(" WHEN SAFE_CAST(CAST(" + round + " AS STRING) AS FLOAT64) = " + v + " THEN " + round);
            }
            w.append(" ELSE ROUND(" + n + ", 16 - " + e + ", 'ROUND_HALF_EVEN') END");
        };
        return w -> w.append("(SELECT CASE WHEN IS_NAN(" + v + ") OR IS_INF(" + v + ") THEN CAST(" + v + " AS STRING)"
                + " WHEN " + v + " = 0 OR (ABS(" + v + ") >= 1e-4 AND ABS(" + v + ") < 1e16) THEN (CASE WHEN STRPOS(" + r
                + ", '.') = 0 THEN CONCAT(" + r + ", '.0') ELSE " + r + " END) ELSE CAST(" + v + " AS STRING) END"
                + " FROM (SELECT " + v + ", CAST(").piece(rounded).append(" AS STRING) AS " + r
                + " FROM (SELECT " + v + ", " + n + ", CASE WHEN " + n + " IS NULL OR " + n + " = 0 THEN 0"
                + " WHEN ABS(" + n + ") >= 1 THEN LENGTH(CAST(TRUNC(ABS(" + n + ")) AS STRING)) - 1"
                + " ELSE -1 - LENGTH(REGEXP_EXTRACT(CAST(ABS(" + n + ") AS STRING), ")
                .append(stringLit("^0\\.(0*)")).append(")) END AS " + e
                + " FROM (SELECT " + v + ", SAFE_CAST(" + v + " AS BIGNUMERIC) AS " + n + " FROM (SELECT ").piece(x)
                .append(" AS " + v + ")))))");
    }

    private SqlWriter.Piece textOf(SqlWriter.Piece value, @com.legend.base.Nullable SqlType type) {
        if (type == SqlType.Scalar.DOUBLE) {
            return floatText(value);
        }
        if (type == SqlType.Scalar.TIMESTAMP) {
            return w -> w.append("FORMAT_DATETIME('%Y-%m-%d %H:%M:%E*S', ").piece(value).append(")");
        }
        if (type == SqlType.Scalar.TIMESTAMPTZ) {
            return w -> w.append("FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S+00', ").piece(value).append(", 'UTC')");
        }
        if (type == SqlType.Scalar.JSON || type instanceof SqlType.Array || type instanceof SqlType.Struct) {
            return w -> w.append("TO_JSON_STRING(").piece(value).append(")");
        }
        // a decimal's text has its scale's digits, as DuckDB's (CAST(BIGNUMERIC '1.0' AS STRING) is '1', measured)
        if (type instanceof SqlType.Decimal d && d.scale() > 0) {
            return w -> w.append("FORMAT(" + stringLit("%." + d.scale() + "f") + ", ").piece(value).append(")");
        }
        return w -> w.append("CAST(").piece(value).append(" AS STRING)");
    }

    // ==================================================================
    // Scalar functions -- every SqlFn decided here
    // ==================================================================

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
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
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> nullSafeArm(writer, c, false);
            case NULL_SAFE_EQUAL -> nullSafeArm(writer, c, true);
            case XOR -> xorAsAndOr(writer, c.args(), parentPrec);
            case LPAD -> padWithSpace(writer, "lpad", c.args());
            case RPAD -> padWithSpace(writer, "rpad", c.args());
            case UC_FIRST -> firstCharCased(writer, "upper", c.args(), parentPrec);
            case LC_FIRST -> firstCharCased(writer, "lower", c.args(), parentPrec);
            case DATE_TRUNC_DAY -> castAs(writer, c.args().get(0), "DATE");
            case ERROR -> raiseEnvelope(writer, c.args(), "ERROR");
            case FLOAT_CANON -> floatCanonByText(writer, c.args(), parentPrec);
            case FLOAT_PRINT, FLOAT_PRINT_ENGINE -> floatPrintByText(writer, c, parentPrec);
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
            case ABS -> writer.function("ABS", c.args());
            case ACOS -> writer.function("ACOS", c.args());
            case ASIN -> writer.function("ASIN", c.args());
            case ATAN -> writer.function("ATAN", c.args());
            case ATAN2 -> writer.function("ATAN2", c.args());
            case CBRT -> writer.function("CBRT", c.args());
            case CHR -> writer.function("CHR", c.args());
            case COALESCE -> coalesceArm(writer, c);
            case COS -> writer.function("COS", c.args());
            case COSH -> writer.function("COSH", c.args());
            case COT -> writer.function("COT", c.args());
            case SIN -> writer.function("SIN", c.args());
            case SINH -> writer.function("SINH", c.args());
            case TAN -> writer.function("TAN", c.args());
            case TANH -> writer.function("TANH", c.args());
            case LOWER -> writer.function("LOWER", c.args());
            case UPPER -> writer.function("UPPER", c.args());
            case LTRIM -> writer.function("LTRIM", c.args());
            case RTRIM -> writer.function("RTRIM", c.args());
            case TRIM -> writer.function("TRIM", c.args());
            case REPLACE -> writer.function("REPLACE", c.args());
            case TRANSLATE -> writer.function("TRANSLATE", c.args());
            case STARTS_WITH -> writer.function("STARTS_WITH", c.args());
            case ENDS_WITH -> writer.function("ENDS_WITH", c.args());
            case STRPOS -> writer.function("STRPOS", c.args());
            case SPLIT -> writer.function("SPLIT", c.args());
            case ASCII_CODE -> writer.function("ASCII", c.args());
            case REVERSE_STRING -> writer.function("REVERSE", c.args());
            case REPEAT_STR -> writer.function("REPEAT", c.args());
            case LEVENSHTEIN -> writer.function("EDIT_DISTANCE", c.args());
            case FLOOR_RAW -> writer.function("FLOOR", c.args());
            case MATCHES -> writer.function("REGEXP_CONTAINS", c.args());
            case SUBSTRING -> writer.function("SUBSTR", c.args());
            case LIST_REVERSE -> writer.function("ARRAY_REVERSE", c.args());
            case LIST_LENGTH -> writer.function("ARRAY_LENGTH", c.args());
            case IN -> in(writer, c, parentPrec);
            // never "--x": that is a comment
            case NEGATE -> writer.append("-(").expr(a.get(0), 0).append(")");

            // ---- arithmetic
            // Pure's divide is a DOUBLE division: x / 0 is +-Infinity or NaN, as DuckDB answers (BigQuery's '/'
            // raises, measured)
            case DIVIDE -> writer.append("IEEE_DIVIDE(CAST(").expr(a.get(0), 0).append(" AS FLOAT64), CAST(")
                    .expr(a.get(1), 0).append(" AS FLOAT64))");
            case MOD, REM -> writer.piece(remainder(c.fn(), a));
            // DuckDB's // truncates toward zero and answers NULL for a zero divisor (measured); so does DIV, which
            // raises instead
            case INT_DIVIDE -> intDivide(writer, c);
            case CEILING -> writer.append("CAST(CEIL(").expr(a.get(0), 0).append(") AS INT64)");
            case FLOOR -> writer.append("CAST(FLOOR(").expr(a.get(0), 0).append(") AS INT64)");
            case SIGN -> writer.append("CAST(SIGN(").expr(a.get(0), 0).append(") AS INT64)");
            // Pure's divide-with-scale is HALF_UP: ROUND rounds half away from zero, a Float through BIGNUMERIC (its
            // shortest decimal, as DuckDB rounds 2.675 to 2.68, measured)
            case ROUND_HALF_UP -> roundHalfUp(writer, a);
            // a Float, as DuckDB's answers: each has a NUMERIC overload here that a BIGNUMERIC literal would pick
            case SQRT -> writer.append("SQRT(CAST(").expr(a.get(0), 0).append(" AS FLOAT64))");
            case EXP -> writer.append("EXP(CAST(").expr(a.get(0), 0).append(" AS FLOAT64))");
            case LN -> writer.append("LN(CAST(").expr(a.get(0), 0).append(" AS FLOAT64))");
            case LOG10 -> writer.append("LOG10(CAST(").expr(a.get(0), 0).append(" AS FLOAT64))");
            case POW -> writer.append("POW(CAST(").expr(a.get(0), 0).append(" AS FLOAT64), CAST(").expr(a.get(1), 0)
                    .append(" AS FLOAT64))");
            case PI -> writer.append("ACOS(-1)");
            // no DEGREES/RADIANS (measured); DuckDB's: x * (180 / pi), x * (pi / 180)
            case DEGREES -> writer.append("(CAST(").expr(a.get(0), 0).append(" AS FLOAT64) * (180 / ACOS(-1)))");
            case RADIANS -> writer.append("(CAST(").expr(a.get(0), 0).append(" AS FLOAT64) * (ACOS(-1) / 180))");
            case BIT_NOT -> writer.append("(~ CAST(").expr(a.get(0), 0).append(" AS INT64))");
            case PARSE_INT -> writer.append("CAST(").expr(a.get(0), 0).append(" AS INT64)");
            case BOOL_TO_TEXT -> writer.append("CAST(").expr(a.get(0), 0).append(" AS STRING)");
            // NULL-skipping, as DuckDB's (BigQuery's are NULL for any NULL argument, measured): each argument stands
            // in for itself, or for another when NULL, so only all-NULL is NULL
            case GREATEST -> writer.piece(nullSkipping("GREATEST", a));
            case LEAST -> writer.piece(nullSkipping("LEAST", a));

            // ---- strings
            // NULL-ignoring, as DuckDB's concat (BigQuery's is NULL if any argument is, measured)
            case CONCAT -> writer.append("CONCAT(").join(flattenConcat(a), ", ",
                    (w, x) -> w.append("IFNULL(").piece(asText(x)).append(", '')")).append(")");
            case CONCAT_JOIN -> writer.append("CONCAT(").join(a, ", ",
                    (w, x) -> w.append("IFNULL(").piece(asText(x)).append(", '')")).append(")");
            case LENGTH -> writer.append("LENGTH(").piece(asText(a.get(0))).append(")");
            // a negative count is all but that many (DuckDB's, measured); BigQuery's LEFT/RIGHT raise on it
            case LEFT, RIGHT -> writer.piece(leftRight(c.fn(), a));
            // DuckDB's split_part: 1-based, negative from the end, out of range '' (measured)
            case SPLIT_PART -> splitPart(writer, a);
            case REGEXP_FULL_MATCH -> writer.append("REGEXP_CONTAINS(").expr(a.get(0), 0).append(", ")
                    .piece(anchored(a.get(1))).append(")");
            case REGEXP_EXTRACT -> regexpExtract(writer, c);
            case REGEXP_EXTRACT_ALL -> regexpExtractAll(writer, a);
            case REGEXP_REPLACE -> writer.piece(regexpReplace(a));
            case ENCODE_BASE64 -> writer.append("TO_BASE64(CAST(").piece(asText(a.get(0))).append(" AS BYTES))");
            // invalid UTF-8 raises, as DuckDB's decode does
            case DECODE_BASE64 -> writer.append("CAST(FROM_BASE64(").expr(a.get(0), 0).append(") AS STRING)");
            case MD5 -> writer.append("TO_HEX(MD5(").piece(asText(a.get(0))).append("))");
            case SHA1 -> writer.append("TO_HEX(SHA1(").piece(asText(a.get(0))).append("))");
            case SHA256 -> writer.append("TO_HEX(SHA256(").piece(asText(a.get(0))).append("))");
            case GUID -> writer.append("GENERATE_UUID()");
            case RANDOM -> writer.append("RAND()");
            // printf-like as DuckDB's: %d takes an INT64, %s a STRING on both (measured)
            case FORMAT -> format(writer, a);
            case JARO_WINKLER -> throw wall(c.fn(), "BigQuery has no Jaro-Winkler similarity");
            case CURRENT_USER_FN -> writer.append("SESSION_USER()");

            // ---- temporal
            case TODAY -> writer.append("CURRENT_DATE()");
            case NOW -> writer.append(typeOf(c) == SqlType.Scalar.TIMESTAMPTZ ? "CURRENT_TIMESTAMP()"
                    : "CURRENT_DATETIME()");
            // an offset or a 'Z' converts to UTC, a naive text reads as UTC (DuckDB's cast)
            case PARSE_DATE -> writer.append("DATETIME(CAST(").expr(a.get(0), 0).append(" AS TIMESTAMP))");
            case EXTRACT -> writer.piece(extract(a));
            case DATE_TRUNC -> writer.piece(dateTrunc(a));
            case MAKE_DATE -> writer.append("DATE(").join(a, ", ", (w, x) -> w.append("CAST(").expr(x, 0)
                    .append(" AS INT64)")).append(")");
            // seconds as a Float: whole microseconds, rounded (DuckDB's, measured)
            case MAKE_TIMESTAMP -> makeTimestamp(writer, a, c);
            // a month past the month's end clamps (2020-01-31 + 1 month is 2020-02-29, measured); a date plus an
            // interval is a DATETIME here, a TIMESTAMP on DuckDB
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> writer.append("(").expr(a.get(2), 5).append(" + INTERVAL CAST(")
                    .expr(a.get(1), 0).append(" AS INT64) ").append(intervalUnit(part(a.get(0))))
                    .append(")");
            case DATE_DIFF -> writer.piece(dateDiff(a));
            case TIME_BUCKET -> writer.piece(timeBucket(a));
            // DATETIME(date, time): the date-time at that time of that day
            case DATE_AT_TIME -> writer.function("DATETIME", c.args());
            // DuckDB's timezone(zone, t): a naive t read in the zone (its instant); an instant to the zone's time
            // (an operand is an instant by its type, or as a timezone() of a naive one: format's zone shift is
            // timezone(zone, timezone('UTC', t)), whose inner call is typed naive but renders a TIMESTAMP)
            case TIMEZONE -> isInstant(a.get(1))
                    ? writer.append("DATETIME(").expr(a.get(1), 0).append(", ").expr(a.get(0), 0).append(")")
                    : writer.append("TIMESTAMP(CAST(").expr(a.get(1), 0).append(" AS DATETIME), ").expr(a.get(0), 0)
                            .append(")");
            // DuckDB's epoch(ts) is DOUBLE seconds; epoch_ms truncates toward zero (measured), as DIV does
            case EPOCH_SECONDS -> writer.append("(CAST(UNIX_MICROS(").piece(instant(a.get(0)))
                    .append(") AS FLOAT64) / 1000000)");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability("a nanosecond epoch: BigQuery keeps"
                    + " microseconds (the Cypher island's nanosecond leg is DuckDB's)");
            case EPOCH_MS -> writer.append("DIV(UNIX_MICROS(").piece(instant(a.get(0))).append("), 1000)");
            case FROM_EPOCH_SECONDS -> writer.piece(naiveUnlessZoned(c, w -> w.append("TIMESTAMP_MICROS(CAST(")
                    .expr(a.get(0), 6).append(" * 1000000 AS INT64))")));
            case FROM_EPOCH_MS -> writer.piece(naiveUnlessZoned(c, w -> w.append("TIMESTAMP_MILLIS(CAST(")
                    .expr(a.get(0), 0).append(" AS INT64))")));
            case DAYNAME -> writer.append("FORMAT_DATETIME('%A', CAST(").expr(a.get(0), 0).append(" AS DATETIME))");
            case MONTHNAME -> writer.append("FORMAT_DATETIME('%B', CAST(").expr(a.get(0), 0).append(" AS DATETIME))");
            case STRFTIME -> strftimeArm(writer, c);
            case STRPTIME -> strptime(writer, a, c);

            // ---- lists (ARRAY<T>; lambdas as correlated subqueries)
            case LIST_FILTER -> listFilter(writer, c);
            case LIST_TRANSFORM -> listTransform(writer, c);
            // NULL ++ xs is xs, as DuckDB's list_concat (measured)
            case LIST_CONCAT -> writer.append("ARRAY_CONCAT(").join(a, ", ", (w, x) -> w.append("IFNULL(").expr(x, 0)
                    .append(", ").append(emptyList(listElement(x) != null ? x : c)).append(")")).append(")");
            case LIST_GET -> listGet(writer, c);
            // DuckDB's list_position: 1-based, NULL when absent, a NULL needle found by IS NOT DISTINCT FROM
            // (measured)
            case LIST_POSITION -> writer.append("(SELECT MIN(" + param("__o") + ") + 1 FROM ")
                    .piece(elements(a.get(0), "__e", "__o")).append(" WHERE " + param("__e") + " IS NOT DISTINCT FROM ")
                    .expr(a.get(1), 4).append(")");
            // DuckDB's list_distinct drops NULLs; the first occurrence's order (DuckDB's own is its hash's)
            case LIST_DISTINCT -> writer.piece(unlessNull(a.get(0), w -> w.append("ARRAY(SELECT " + param("__e")
                    + " FROM ").piece(elements(a.get(0), "__e", "__o")).append(" WHERE " + param("__e")
                    + " IS NOT NULL GROUP BY " + param("__e") + " ORDER BY MIN(" + param("__o") + "))")));
            // list_append(NULL, v) is [v] (measured)
            case LIST_APPEND -> listAppend(writer, c);
            case LIST_SUM -> writer.piece(overRows(a.get(0), SqlAgg.Fn.SUM, List.of()));
            case LIST_MIN -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MIN, List.of()));
            case LIST_MAX -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MAX, List.of()));
            case LIST_AVG -> writer.piece(overRows(a.get(0), SqlAgg.Fn.AVG, List.of()));
            case LIST_MEDIAN -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MEDIAN, List.of()));
            case LIST_MODE -> writer.piece(overRows(a.get(0), SqlAgg.Fn.MODE, List.of()));
            case LIST_BOOL_AND -> writer.piece(overRows(a.get(0), SqlAgg.Fn.BOOL_AND, List.of()));
            case LIST_BOOL_OR -> writer.piece(overRows(a.get(0), SqlAgg.Fn.BOOL_OR, List.of()));
            // ascending / descending, NULLs last both ways (DuckDB's list_sort / list_reverse_sort, measured); a
            // struct by its fields in declared order (BigQuery orders no STRUCT)
            case LIST_SORT, LIST_SORT_DESC -> listSort(writer, c);
            case LIST_TAIL -> writer.piece(overElements(a.get(0), "__e", "__o", w -> w.append(param("__e")),
                    w -> w.append(param("__o") + " > 0")));
            case LIST_INIT -> writer.piece(overElements(a.get(0), "__e", "__o", w -> w.append(param("__e")),
                    w -> w.append(param("__o") + " < ARRAY_LENGTH(").expr(a.get(0), 0).append(") - 1")));
            // DuckDB's list_slice: 1-based, both ends inclusive, a negative end from the end (measured)
            case LIST_SLICE -> writer.piece(overElements(a.get(0), "__e", "__o", w -> w.append(param("__e")),
                    w -> w.append(param("__o") + " + 1 >= ").piece(sliceEnd(a.get(0), a.get(1))).append(" AND "
                            + param("__o") + " + 1 <= ").piece(sliceEnd(a.get(0), a.get(2)))));
            // DuckDB's range: [start, stop) by step (start 0, step 1 by default); GENERATE_ARRAY is inclusive
            case RANGE_FN -> range(writer, a);
            case REPEAT_VALUE -> repeatValue(writer, c);
            case LIST_FLATTEN -> throw nestedList();
            case LIST_ZIP -> throw wall(c.fn(), "list_zip names its struct fields by position, unmeasured");
            case LIST_PRODUCT -> throw wall(c.fn(), "BigQuery has no product aggregate and no fold");
            case LIST_REDUCE -> throw wall(c.fn(), "a reduce that reads its row is a fold: BigQuery has no lambdas"
                    + " (a closed one is hoisted to a top-level recursive expression)");
            case TYPEOF -> throw wall(c.fn(), "its type names are BigQuery's");

            // ---- maps: entries in order, ARRAY<STRUCT<key, value>> (mapTypeName)
            case MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_KEYS, MAP_VALUES, MAP_CONCAT ->
                    maps(writer, c);

            // ---- variant
            case JSON_TYPE -> jsonType(writer, c);
            case JSON_ARRAY_LENGTH -> writer.append("ARRAY_LENGTH(JSON_QUERY_ARRAY(").expr(a.get(0), 0).append("))");
            case JSON_PRETTY -> throw wall(c.fn(), "BigQuery has no JSON pretty-printer");
            case JSON_MERGE_PATCH -> throw wall(c.fn(), "BigQuery has no JSON merge patch");
        };
    }

    /** COALESCE: a text operand beside a JSON one is parsed, as Trino's json_parse (BigQuery's COALESCE takes one type:
     *  "No matching signature for function COALESCE | Argument types: JSON, STRING", an instance canon's empty key
     *  '[]' beside a nested instance's canon object; the zips of Pairs of Pairs, measured on the lane 2026-10-21). Out
     *  of {@link #call} since 2026-10-22, its length. */
    private SqlWriter coalesceArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.stream().anyMatch(BigQuery::isJson) && a.stream().anyMatch(BigQuery::isText))) {
            return writer.function("COALESCE", c.args());
        }
        return writer.append("COALESCE(").join(a, ", ", (w, e) -> {
            if (isText(e)) {
                w.append("PARSE_JSON(").expr(e, 0).append(")");
            } else {
                w.expr(e, 0);
            }
        }).append(")");
    }

    /** LIST_GET: 1-based, negative from the end, 0 and out of range NULL (DuckDB's list_extract, measured); a NULL
     *  list's element is its type's NULL: a bare NULL is an INT64 here, which takes no subscript ([]->head():
     *  "Subscript access using [SAFE_OFFSET()] is not supported on values of type INT64", testHeadOnEmptySet on the
     *  lane, 2026-10-21). */
    private SqlWriter listGet(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (nullList(a.get(0))) {
            return writer.append(nullOf(typeOf(c)));
        }
        return writer.append("(").expr(a.get(0), 0).append(")[SAFE_OFFSET(CASE WHEN ").expr(a.get(1), 4)
                .append(" < 0 THEN ARRAY_LENGTH(").expr(a.get(0), 0).append(") + CAST(").expr(a.get(1), 0)
                .append(" AS INT64) ELSE CAST(").expr(a.get(1), 0).append(" AS INT64) - 1 END)]");
    }

    /** JSON_TYPE, in DuckDB's json_type vocabulary, which the lowering compares against: a whole number is a BIGINT,
     *  any other a DOUBLE (JSON_TYPE answers lowercase names, 'number' for both, measured). */
    private SqlWriter jsonType(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        return writer.append("(CASE JSON_TYPE(").expr(a.get(0), 0)
                .append(") WHEN 'null' THEN 'NULL' WHEN 'string' THEN 'VARCHAR' WHEN 'boolean' THEN 'BOOLEAN'")
                .append(" WHEN 'array' THEN 'ARRAY' WHEN 'object' THEN 'OBJECT' WHEN 'number' THEN CASE WHEN")
                .append(" REGEXP_CONTAINS(TO_JSON_STRING(").expr(a.get(0), 0)
                .append("), " + stringLit("^-?[0-9]+$") + ") THEN 'BIGINT' ELSE 'DOUBLE' END END)");
    }

    /** The {@code EQUAL}, {@code NOT_EQUAL} arm of {@link #call}. */
    private SqlWriter equalArm(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && (composite(a.get(0)) || composite(a.get(1)))) {
            return compositeEquality(writer, a, c.fn() == SqlFn.EQUAL);
        }
        return infix(writer, c, parentPrec);
    }

    /** The null-safe arms of {@link #call}: a NULL composite's JSON text is 'null', no composite value's is. */
    private SqlWriter nullSafeArm(SqlWriter writer, SqlExpr.Call c, boolean equal) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && (composite(a.get(0)) || composite(a.get(1)))) {
            return writer.append("(TO_JSON_STRING(").expr(a.get(0), 0).append(equal ? ") = " : ") <> ")
                    .append("TO_JSON_STRING(").expr(a.get(1), 0).append("))");
        }
        return equal ? notDistinctFrom(writer, a) : distinctFrom(writer, a);
    }

    /** The {@code IN} arm of {@link #call}. */
    private SqlWriter in(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && a.get(1) instanceof SqlExpr.PlanParam p) {
            throw listParameter(p);
        }
        return inList(writer, a, parentPrec);
    }

    /** The {@code INT_DIVIDE} arm of {@link #call}. */
    private SqlWriter intDivide(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.stream().anyMatch(BigQuery::isDouble)) {
            throw wall(c.fn(), "DIV takes no FLOAT64");
        }
        return writer.append("(CASE WHEN ").expr(a.get(1), 4).append(" = 0 THEN NULL ELSE DIV(")
                .expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(") END)");
    }

    /** The {@code REGEXP_EXTRACT} arm of {@link #call}. */
    private SqlWriter regexpExtract(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String pattern = onlyGroup(literal(a.get(1), "a regexp_extract pattern"), group(a));
        return writer.append("(CASE WHEN ").expr(a.get(0), 4)
                .append(" IS NULL THEN NULL ELSE COALESCE(REGEXP_EXTRACT(").expr(a.get(0), 0)
                .append(", " + stringLit(pattern) + "), '') END)");
    }

    /** The {@code STRFTIME} arm of {@link #call}. */
    private SqlWriter strftimeArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit fl)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        return writer.piece(strftime(a.get(0), fl));
    }

    /** The {@code LIST_FILTER} arm of {@link #call}. */
    private SqlWriter listFilter(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlExpr.Lambda l = lambdaOf(a.get(1), c.fn());
        String x = l.params().get(0);
        return writer.piece(overElements(a.get(0), x, offsetOf(l), w -> w.append(param(x)),
                w -> w.expr(lambdaBody(a.get(0), l), 0)));
    }

    /** A list with one element appended (NULL ++ x is [x]; no nested lists). */
    private SqlWriter listAppend(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (typeOf(a.get(1)) instanceof SqlType.Array) {
            throw nestedList();
        }
        return writer.append("ARRAY_CONCAT(IFNULL(").expr(a.get(0), 0).append(", ").append(emptyList(a.get(0)))
                .append("), [").expr(a.get(1), 0).append("])");
    }

    /** A list sorted by its elements, NULLs last either way. */
    private SqlWriter listSort(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String dir = c.fn() == SqlFn.LIST_SORT ? " ASC NULLS LAST" : " DESC NULLS LAST";
        SqlType el = listElement(a.get(0));
        return writer.piece(unlessNull(a.get(0), w -> w.append("ARRAY(SELECT " + param("__e") + " FROM UNNEST(")
                .expr(a.get(0), 0).append(") AS " + param("__e") + " ORDER BY ")
                .append(elementSort(param("__e"), el, dir)).append(")")));
    }

    /** A value repeated n times (no nested lists). */
    private SqlWriter repeatValue(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (typeOf(a.get(0)) instanceof SqlType.Array) {
            throw nestedList();
        }
        return writer.append("ARRAY(SELECT ").expr(a.get(0), 0).append(" FROM UNNEST(GENERATE_ARRAY(1, CAST(")
                .expr(a.get(1), 0).append(" AS INT64))) AS " + param("__g") + ")");
    }

    /** A list transform: each element through the lambda, in order (no nested lists). */
    private SqlWriter listTransform(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (listElement(c) instanceof SqlType.Array) {
            throw nestedList();
        }
        SqlExpr.Lambda l = lambdaOf(a.get(1), c.fn());
        return writer.piece(overElements(a.get(0), l.params().get(0), offsetOf(l),
                w -> w.expr(lambdaBody(a.get(0), l), 0), null));
    }

    private static boolean composite(SqlExpr e) {
        SqlType t = typeOf(e);
        return t instanceof SqlType.Struct || t instanceof SqlType.Array;
    }

    /** {@code =} between structs or arrays: GoogleSQL defines none ("Equality is not defined for arguments of type
     *  STRUCT", measured); DuckDB's compares nested NULLs as equal, as their JSON texts do; a NULL operand is NULL. */
    private SqlWriter compositeEquality(SqlWriter writer, List<SqlExpr> a, boolean equal) {
        return writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL OR ").expr(a.get(1), 4)
                .append(" IS NULL THEN NULL ELSE TO_JSON_STRING(").expr(a.get(0), 0).append(equal ? ") = " : ") <> ")
                .append("TO_JSON_STRING(").expr(a.get(1), 0).append(") END)");
    }

    /** printf: a {@code %s} argument as its text (GoogleSQL's %s takes a STRING only: "Expected STRING; Got BOOL",
     *  measured); the others as they are. */
    private SqlWriter format(SqlWriter writer, List<SqlExpr> a) {
        if (!(a.get(0) instanceof SqlExpr.StringLit f)) {
            return writer.append("FORMAT(").list(a).append(")");
        }
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("%[-+ #0]*[0-9]*(?:\\.[0-9]+)?([a-zA-Z%])")
                .matcher(f.value());
        List<SqlExpr> args = new ArrayList<>(a);
        int arg = 1;
        while (m.find()) {
            char conversion = m.group(1).charAt(0);
            if (conversion == '%') {
                continue;
            }
            if (conversion == 's' && arg < args.size()) {
                args.set(arg, new SqlExpr.Cast(args.get(arg), SqlType.Scalar.VARCHAR));
            }
            arg++;
        }
        return writer.append("FORMAT(").list(args).append(")");
    }

    // ---- call arms too long to stay inline (CodeShapeGuardrailTest: call under 250 lines)

    /** DuckDB's round: a decimal half away from zero; a double by DuckDB's own algorithm, {@code round(x * 10^n) /
     *  10^n} in double (31.415's double is 31.41499..., which an exact ROUND takes to 31.41 and DuckDB to 31.42,
     *  measured), {@code round(x / 10^-n) * 10^-n} for a negative n. */
    private SqlWriter roundHalfUp(SqlWriter writer, List<SqlExpr> a) {
        if (isDouble(a.get(0)) && a.size() > 1) {
            SqlWriter.Piece places = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS INT64)");
            SqlWriter.Piece up = w -> w.append("POW(10, ").piece(places).append(")");
            SqlWriter.Piece down = w -> w.append("POW(10, -").piece(places).append(")");
            return writer.append("(CASE WHEN ").piece(places).append(" >= 0 THEN ROUND(").expr(a.get(0), 6)
                    .append(" * ").piece(up).append(") / ").piece(up).append(" ELSE ROUND(").expr(a.get(0), 6)
                    .append(" / ").piece(down).append(") * ").piece(down).append(" END)");
        }
        writer.append("ROUND(");
        writer.expr(a.get(0), 0);
        if (a.size() > 1) {
            writer.append(", CAST(").expr(a.get(1), 0).append(" AS INT64)");
        }
        return writer.append(")");
    }

    private SqlWriter range(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece start = a.size() == 1 ? w -> w.append("0") : w -> w.append("CAST(").expr(a.get(0), 0)
                .append(" AS INT64)");
        SqlExpr last = a.get(a.size() == 1 ? 0 : 1);
        SqlWriter.Piece stop = w -> w.append("CAST(").expr(last, 0).append(" AS INT64)");
        SqlWriter.Piece step = a.size() > 2 ? w -> w.append("CAST(").expr(a.get(2), 0).append(" AS INT64)")
                : w -> w.append("1");
        String g = param("__g");
        return writer.append("(CASE WHEN (").piece(stop).append(" - ").piece(start).append(") * ").piece(step)
                .append(" > 0 THEN ARRAY(SELECT " + g + " FROM UNNEST(GENERATE_ARRAY(").piece(start)
                .append(", ").piece(stop).append(", ").piece(step).append(")) AS " + g + " WITH OFFSET AS "
                        + param("__go") + " WHERE " + g + " <> ").piece(stop)
                .append(" ORDER BY " + param("__go") + ") ELSE ARRAY<INT64>[] END)");
    }

    private SqlWriter regexpExtractAll(SqlWriter writer, List<SqlExpr> a) {
        int g = group(a);
        String pattern = stringLit(onlyGroup(literal(a.get(1), "a regexp_extract_all pattern"), g));
        if (g == 0) {
            return writer.append("REGEXP_EXTRACT_ALL(").expr(a.get(0), 0).append(", " + pattern + ")");
        }
        // a group that took no part in a match is '' (DuckDB's), never a NULL element
        String m = param("__m");
        return writer.append("ARRAY(SELECT IFNULL(" + m + ", '') FROM UNNEST(REGEXP_EXTRACT_ALL(")
                .expr(a.get(0), 0).append(", " + pattern + ")) AS " + m + " WITH OFFSET AS " + param("__mo")
                        + " ORDER BY " + param("__mo") + ")");
    }

    private SqlWriter makeTimestamp(SqlWriter writer, List<SqlExpr> a, SqlExpr.Call c) {
        if (a.size() != 6) {
            throw wall(c.fn(), "make_timestamp of " + a.size() + " arguments");
        }
        return writer.append("DATETIME_ADD(DATETIME(").join(a.subList(0, 5), ", ", (w, x) -> w.append("CAST(")
                .expr(x, 0).append(" AS INT64)")).append(", 0), INTERVAL CAST(").expr(a.get(5), 6)
                .append(" * 1000000 AS INT64) MICROSECOND)");
    }

    private SqlWriter splitPart(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece parts = w -> w.append("SPLIT(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                .append(")");
        return writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL OR ").expr(a.get(1), 4)
                .append(" IS NULL THEN NULL ELSE COALESCE((").piece(parts)
                .append(")[SAFE_OFFSET(CASE WHEN ").expr(a.get(2), 4).append(" < 0 THEN ARRAY_LENGTH(")
                .piece(parts).append(") + CAST(").expr(a.get(2), 0).append(" AS INT64) ELSE CAST(")
                .expr(a.get(2), 0).append(" AS INT64) - 1 END)], '') END)");
    }

    private SqlWriter strptime(SqlWriter writer, List<SqlExpr> a, SqlExpr.Call c) {
        if (!(a.get(1) instanceof SqlExpr.FormatLit fl)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        return writer.append("PARSE_DATETIME(").append(stringLit(formatText(fl))).append(", ")
                .expr(a.get(0), 0).append(")");
    }

    private static DialectCapability wall(SqlFn fn, String why) {
        return new DialectCapability(fn + " reached BigQuery: " + why);
    }

    private static String literal(SqlExpr e, String what) {
        if (e instanceof SqlExpr.StringLit s) {
            return s.value();
        }
        throw new DialectCapability(what + " that is not a literal reached BigQuery");
    }

    /** Pure's mod (the divisor's sign) and rem (the dividend's): MOD over INT64, NUMERIC or BIGNUMERIC (a Float through
     *  BIGNUMERIC: MOD takes no FLOAT64, measured); a zero divisor is NULL, as DuckDB's % (measured). */
    private SqlWriter.Piece remainder(SqlFn fn, List<SqlExpr> a) {
        boolean dbl = a.stream().anyMatch(BigQuery::isDouble);
        SqlWriter.Piece x = dbl ? w -> w.append("CAST(").expr(a.get(0), 0).append(" AS BIGNUMERIC)")
                : w -> w.expr(a.get(0), 0);
        SqlWriter.Piece y = dbl ? w -> w.append("CAST(").expr(a.get(1), 0).append(" AS BIGNUMERIC)")
                : w -> w.expr(a.get(1), 0);
        SqlWriter.Piece rem = w -> w.append("MOD(").piece(x).append(", ").piece(y).append(")");
        SqlWriter.Piece body = fn == SqlFn.REM ? rem
                : w -> w.append("MOD(").piece(rem).append(" + ").piece(y).append(", ").piece(y).append(")");
        return w -> w.append("(CASE WHEN ").piece(y).append(" = 0 THEN NULL ELSE ").piece(body).append(" END)");
    }

    /** GREATEST/LEAST over the arguments, each replaced by the next non-NULL one when it is NULL: NULL only when every
     *  argument is (measured). */
    private static SqlWriter.Piece nullSkipping(String fn, List<SqlExpr> a) {
        if (a.size() == 1) {
            return w -> w.expr(a.get(0), 0);
        }
        return w -> {
            w.append(fn).append("(");
            for (int i = 0; i < a.size(); i++) {
                w.append(i > 0 ? ", COALESCE(" : "COALESCE(").expr(a.get(i), 0);
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

    private SqlWriter.Piece leftRight(SqlFn fn, List<SqlExpr> a) {
        String name = fn == SqlFn.LEFT ? "LEFT" : "RIGHT";
        if (a.get(1) instanceof SqlExpr.IntLit n && n.value() >= 0) {
            return w -> w.append(name + "(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
        }
        SqlWriter.Piece negative = fn == SqlFn.LEFT
                ? w -> w.append("SUBSTR(").expr(a.get(0), 0).append(", 1, GREATEST(LENGTH(").expr(a.get(0), 0)
                        .append(") + CAST(").expr(a.get(1), 0).append(" AS INT64), 0))")
                : w -> w.append("SUBSTR(").expr(a.get(0), 0).append(", 1 - CAST(").expr(a.get(1), 0)
                        .append(" AS INT64))");
        return w -> w.append("(CASE WHEN ").expr(a.get(1), 4).append(" < 0 THEN ").piece(negative)
                .append(" ELSE " + name + "(").expr(a.get(0), 0).append(", CAST(").expr(a.get(1), 0)
                .append(" AS INT64)) END)");
    }

    /** A slice bound, 1-based: a negative one counts from the end. */
    private static SqlWriter.Piece sliceEnd(SqlExpr xs, SqlExpr i) {
        return w -> w.append("(CASE WHEN ").expr(i, 4).append(" < 0 THEN ARRAY_LENGTH(").expr(xs, 0).append(") + ")
                .expr(i, 6).append(" + 1 ELSE ").expr(i, 0).append(" END)");
    }

    /** The ORDER BY of a list's elements: a struct by its fields in declared order (BigQuery orders no STRUCT). */
    private String elementSort(String x, @com.legend.base.Nullable SqlType el, String dir) {
        if (el instanceof SqlType.Struct st) {
            List<String> keys = new ArrayList<>();
            for (SqlType.Struct.Field f : st.fields()) {
                keys.add(elementSort(x + "." + ident(f.name()), f.type(), dir));
            }
            return String.join(", ", keys);
        }
        return x + dir;
    }

    // ---- regular expressions (RE2 on both sides)

    private static int group(List<SqlExpr> a) {
        if (a.size() < 3) {
            return 0;
        }
        if (a.get(2) instanceof SqlExpr.IntLit g && g.value() >= 0 && g.value() <= 99) {
            return (int) g.value();
        }
        throw new DialectCapability("a regexp group that is not a literal reached BigQuery");
    }

    /**
     * {@code p} with only its {@code keep}-th capturing group capturing (0: none), every other made non-capturing:
     * BigQuery's REGEXP_EXTRACT answers group 1 when the pattern has one and refuses two (measured), DuckDB's answers
     * the whole match or the named group. RE2 syntax: an escape, a {@code \Q...\E} span and a character class are
     * copied as they are; a {@code (?...)} group is non-capturing already, but for a named one ({@code (?P<n>...)}).
     */
    static String onlyGroup(String p, int keep) {
        StringBuilder out = new StringBuilder(p.length() + 8);
        int group = 0;
        boolean inClass = false;
        for (int i = 0; i < p.length(); i++) {
            char ch = p.charAt(i);
            if (ch == '\\' && i + 1 < p.length()) {
                if (p.charAt(i + 1) == 'Q') {
                    int end = p.indexOf("\\E", i + 2);
                    end = end < 0 ? p.length() : end + 2;
                    out.append(p, i, end);
                    i = end - 1;
                } else {
                    out.append(ch).append(p.charAt(++i));
                }
                continue;
            }
            if (inClass) {
                inClass = ch != ']';
                out.append(ch);
                continue;
            }
            if (ch == '[') {
                inClass = true;
                out.append(ch);
                if (i + 1 < p.length() && p.charAt(i + 1) == '^') {
                    out.append(p.charAt(++i));
                }
                if (i + 1 < p.length() && p.charAt(i + 1) == ']') {
                    out.append(p.charAt(++i));
                }
                continue;
            }
            if (ch == '(') {
                boolean named = p.startsWith("(?P<", i) || p.startsWith("(?<", i);
                if (p.startsWith("(?", i) && !named) {
                    out.append(ch);
                    continue;
                }
                group++;
                if (group == keep) {
                    out.append(ch);
                } else {
                    out.append("(?:");
                    if (named) {
                        int close = p.indexOf('>', i);
                        i = close < 0 ? i : close;
                    }
                }
                continue;
            }
            out.append(ch);
        }
        return out.toString();
    }

    /** The pattern anchored at both ends (its inline flags apply inside the group, as RE2 reads them). */
    private SqlWriter.Piece anchored(SqlExpr p) {
        if (p instanceof SqlExpr.StringLit lit) {
            String s = stringLit("^(?:" + lit.value() + ")$");
            return w -> w.append(s);
        }
        return w -> w.append("CONCAT(" + stringLit("^(?:") + ", ").expr(p, 0).append(", ')$')");
    }

    /**
     * DuckDB's regexp_replace(s, p, r[, options]): the FIRST match unless the options say 'g' (measured), 'i' case-
     * insensitive. BigQuery's replaces every match, so a first-match replace matches the shortest prefix before the
     * pattern and keeps it ({@code ^((?s:.)*?)(p)} to {@code \1r}, measured), the replacement's group references
     * shifted past the two groups it adds. Both read RE2's replacement ({@code \1}, {@code \0}, {@code \\}).
     */
    private SqlWriter.Piece regexpReplace(List<SqlExpr> a) {
        String options = a.size() > 3 ? literal(a.get(3), "regexp_replace options") : "";
        boolean global = false;
        boolean caseless = false;
        for (char o : options.toCharArray()) {
            switch (o) {
                case 'g' -> global = true;
                case 'i' -> caseless = true;
                case 'c' -> caseless = false;
                default -> throw new DialectCapability("regexp_replace option '" + o + "' reached BigQuery");
            }
        }
        String flags = caseless ? "(?i)" : "";
        SqlExpr s = a.get(0);
        SqlExpr p = a.get(1);
        if (global) {
            SqlWriter.Piece pattern = p instanceof SqlExpr.StringLit lit ? w -> w.append(stringLit(flags + lit.value()))
                    : flags.isEmpty() ? w -> w.expr(p, 0)
                    : w -> w.append("CONCAT('" + flags + "', ").expr(p, 0).append(")");
            return w -> w.append("REGEXP_REPLACE(").expr(s, 0).append(", ").piece(pattern).append(", ")
                    .expr(a.get(2), 0).append(")");
        }
        String replacement = "\\1" + shiftGroups(literal(a.get(2), "a first-match regexp_replace replacement"));
        SqlWriter.Piece pattern = p instanceof SqlExpr.StringLit lit
                ? w -> w.append(stringLit("^((?s:.)*?)(" + flags + lit.value() + ")"))
                : w -> w.append("CONCAT(" + stringLit("^((?s:.)*?)(" + flags) + ", ").expr(p, 0).append(", ')')");
        return w -> w.append("REGEXP_REPLACE(").expr(s, 0).append(", ").piece(pattern).append(", ")
                .append(stringLit(replacement)).append(")");
    }

    /** A replacement's group references moved past the prefix group and the pattern's own: {@code \0} (the match) is
     *  {@code \2}, {@code \k} is {@code \(k+2)}. */
    private static String shiftGroups(String r) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < r.length(); i++) {
            char ch = r.charAt(i);
            if (ch == '\\' && i + 1 < r.length() && Character.isDigit(r.charAt(i + 1))) {
                int k = r.charAt(++i) - '0' + 2;
                if (k > 9) {
                    throw new DialectCapability("a first-match regexp_replace referring to group " + (k - 2)
                            + " reached BigQuery (RE2 refers to nine)");
                }
                out.append('\\').append(k);
            } else if (ch == '\\' && i + 1 < r.length()) {
                out.append(ch).append(r.charAt(++i));
            } else {
                out.append(ch);
            }
        }
        return out.toString();
    }

    // ---- maps: a map is its entries in order, ARRAY<STRUCT<key, value>> (mapTypeName). DuckDB's semantics, measured
    // (1.5): entries keep their order; a NULL key raises "Map keys can not be NULL.", a repeated one "Map keys must be
    // unique."; map(ks, vs) of unequal lengths raises; a concatenation keeps each key at its first place with the
    // value of the last map holding it, a NULL map adding nothing; an extraction is the key's value as a list, empty
    // when it is absent; a NULL map's keys, values or extraction are NULL.

    private SqlWriter maps(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String e = param("__me");
        String o = param("__mo");
        String key = ident("key");
        String value = ident("value");
        return switch (c.fn()) {
            case MAP_EMPTY -> {
                if (!(typeOf(c) instanceof SqlType.Map m)) {
                    throw wall(c.fn(), "an empty map of no known key and value types");
                }
                yield writer.append(bqType(m) + "[]");
            }
            // each entry a struct of (key, value), by position whatever their names
            case MAP_FROM_ENTRIES -> {
                SqlExpr xs = a.get(0);
                if (!(listElement(xs) instanceof SqlType.Struct entry) || entry.fields().size() != 2) {
                    throw wall(c.fn(), "entries of " + listElement(xs));
                }
                String k = e + "." + ident(entry.fields().get(0).name());
                String v = e + "." + ident(entry.fields().get(1).name());
                yield writer.piece(unlessNull(xs, keysChecked(xs, e, k, w -> w.append("ARRAY(SELECT AS STRUCT " + k
                        + " AS " + key + ", " + v + " AS " + value + " FROM UNNEST(").expr(xs, 0).append(") AS " + e
                        + " WITH OFFSET AS " + o + " ORDER BY " + o + ")"))));
            }
            // DuckDB's map(ks, vs): the keys zipped with the values by position
            case MAP_FROM_LISTS -> {
                SqlExpr ks = a.get(0);
                SqlExpr vs = a.get(1);
                SqlWriter.Piece zipped = w -> w.append("ARRAY(SELECT AS STRUCT " + e + " AS " + key + ", ").expr(vs, 0)
                        .append("[OFFSET(" + o + ")] AS " + value + " FROM UNNEST(").expr(ks, 0).append(") AS " + e
                                + " WITH OFFSET AS " + o + " ORDER BY " + o + ")");
                yield writer.append("(CASE WHEN ").expr(ks, 4).append(" IS NULL OR ").expr(vs, 4)
                        .append(" IS NULL THEN NULL WHEN ARRAY_LENGTH(").expr(ks, 0).append(") <> ARRAY_LENGTH(")
                        .expr(vs, 0).append(") THEN ERROR('The map key list does not align with the map value list.')"
                                + " ELSE ").piece(keysChecked(ks, e, e, zipped)).append(" END)");
            }
            // the entries of every map, each key at its first place (map, then offset) with the last map's value
            // (wrapped in a struct: a NULL value is an entry, and an array holds no NULL)
            case MAP_CONCAT -> {
                String u = param("__mu");
                String src = param("__ms");
                yield writer.append("ARRAY(SELECT AS STRUCT " + u + "." + key + " AS " + key + ", ARRAY_AGG(STRUCT("
                        + u + "." + value + " AS v) ORDER BY " + u + "." + src + " DESC LIMIT 1)[OFFSET(0)].v AS "
                        + value + " FROM (").piece(w -> {
                            for (int i = 0; i < a.size(); i++) {
                                w.append(i > 0 ? " UNION ALL " : "").append("SELECT " + i + " AS " + src + ", " + e
                                        + "." + key + " AS " + key + ", " + e + "." + value + " AS " + value + ", " + o
                                        + " AS " + o + " FROM UNNEST(").expr(a.get(i), 0).append(") AS " + e
                                        + " WITH OFFSET AS " + o);
                            }
                        }).append(") AS " + u + " GROUP BY " + u + "." + key + " ORDER BY MIN(" + u + "." + src
                                + "), ARRAY_AGG(" + u + "." + o + " ORDER BY " + u + "." + src
                                + " LIMIT 1)[OFFSET(0)])");
            }
            case MAP_KEYS, MAP_VALUES -> writer.piece(unlessNull(a.get(0), w -> w.append("ARRAY(SELECT " + e + "."
                    + (c.fn() == SqlFn.MAP_KEYS ? key : value) + " FROM UNNEST(").expr(a.get(0), 0).append(") AS " + e
                    + " WITH OFFSET AS " + o + " ORDER BY " + o + ")")));
            case MAP_EXTRACT -> writer.piece(unlessNull(a.get(0), w -> w.append("ARRAY(SELECT " + e + "." + value
                    + " FROM UNNEST(").expr(a.get(0), 0).append(") AS " + e + " WITH OFFSET AS " + o + " WHERE " + e
                    + "." + key + " = ").expr(a.get(1), 5).append(" ORDER BY " + o + ")")));
            default -> throw new IllegalStateException("not a map call: " + c.fn());
        };
    }

    /** {@code then} when the keys {@code k} (read off each element {@code e} of {@code xs}) are each non-NULL and
     *  distinct; DuckDB's error otherwise. */
    private static SqlWriter.Piece keysChecked(SqlExpr xs, String e, String k, SqlWriter.Piece then) {
        return w -> w.append("(CASE WHEN (SELECT COUNTIF(" + k + " IS NULL) FROM UNNEST(").expr(xs, 0)
                .append(") AS " + e + ") > 0 THEN ERROR('Map keys can not be NULL.') WHEN (SELECT COUNT(DISTINCT " + k
                        + ") FROM UNNEST(").expr(xs, 0).append(") AS " + e + ") < ARRAY_LENGTH(").expr(xs, 0)
                .append(") THEN ERROR('Map keys must be unique.') ELSE ").piece(then).append(" END)");
    }

    /** A map cast to another map's key and value types, entry by entry; the empty map {@code MAP_EMPTY} typed. */
    private SqlWriter.Piece mapAs(SqlExpr m, SqlType.Map to) {
        if (m instanceof SqlExpr.Call c && c.fn() == SqlFn.MAP_EMPTY) {
            return w -> w.append(bqType(to) + "[]");
        }
        if (m instanceof SqlExpr.NullLit) {
            return w -> w.append("CAST(NULL AS " + bqType(to) + ")");
        }
        if (!(typeOf(m) instanceof SqlType.Map from)) {
            throw new DialectCapability("a cast of " + typeOf(m) + " to a map reached BigQuery");
        }
        if (from.equals(to)) {
            return w -> w.expr(m, 0);
        }
        String e = param("__me");
        String o = param("__mo");
        String key = ident("key");
        String value = ident("value");
        return unlessNull(m, w -> w.append("ARRAY(SELECT AS STRUCT ")
                .piece(castTo(x -> x.append(e + "." + key), from.key(), to.key())).append(" AS " + key + ", ")
                .piece(castTo(x -> x.append(e + "." + value), from.value(), to.value())).append(" AS " + value
                        + " FROM UNNEST(").expr(m, 0).append(") AS " + e + " WITH OFFSET AS " + o + " ORDER BY " + o
                        + ")"));
    }

    /** A Variant object as a map: its members, each value read as the map's value type, NULL when it is not an
     *  object. In BigQuery's member order, which is the keys' (a JSON value keeps no document order here: '{"b":1,
     *  "a":2}' reads back {"a":2,"b":1}, measured), where DuckDB keeps the document's. */
    private SqlWriter.Piece variantAsMap(SqlExpr v, SqlType.Map to) {
        String k = param("__mk");
        String o = param("__mo");
        SqlWriter.Piece member = w -> w.expr(v, 0).append("[" + k + "]");
        SqlWriter.Piece read = to.value() == SqlType.Scalar.JSON ? member
                : castFromText(w -> w.append("JSON_VALUE(").piece(member).append(")"), to.value());
        return w -> w.append("(CASE WHEN JSON_TYPE(").expr(v, 0).append(") = 'object' THEN ARRAY(SELECT AS STRUCT ")
                .piece(castTo(x -> x.append(k), SqlType.Scalar.VARCHAR, to.key())).append(" AS " + ident("key")
                        + ", ").piece(read).append(" AS " + ident("value") + " FROM UNNEST(JSON_KEYS(").expr(v, 0)
                .append(", 1)) AS " + k + " WITH OFFSET AS " + o + " ORDER BY " + o + ") END)");
    }

    // ---- temporal helpers

    /** Whether a timestamp is an instant: TIMESTAMPTZ-typed, or a timezone() of a naive one (each timezone() turns
     *  one into the other), as Snowflake decides. */
    private static boolean isInstant(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.TIMEZONE && c.args().size() == 2) {
            return !isInstant(c.args().get(1));
        }
        return typeOf(e) == SqlType.Scalar.TIMESTAMPTZ;
    }

    /** A temporal operand as its UTC instant: a TIMESTAMP is one; a DATETIME (or a DATE) is read in UTC. */
    private static SqlWriter.Piece instant(SqlExpr e) {
        if (typeOf(e) == SqlType.Scalar.TIMESTAMPTZ) {
            return w -> w.expr(e, 0);
        }
        return w -> w.append("TIMESTAMP(CAST(").expr(e, 0).append(" AS DATETIME))");
    }

    /** An instant as the call's type: its UTC DATETIME, unless the call is zoned. */
    private static SqlWriter.Piece naiveUnlessZoned(SqlExpr.Call c, SqlWriter.Piece ts) {
        if (typeOf(c) == SqlType.Scalar.TIMESTAMPTZ) {
            return ts;
        }
        return w -> w.append("DATETIME(").piece(ts).append(")");
    }

    /** A date part of a date, a Pure integer (seconds truncated). DAYOFWEEK is Sunday 1 here; the ISO day of the week
     *  is Monday 1 to Sunday 7, and the week the ISO week (measured). */
    private static SqlWriter.Piece extract(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        SqlWriter.Piece t = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS DATETIME)");
        return switch (part) {
            case YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND -> w -> w.append("EXTRACT(" + unit(part) + " FROM ")
                    .piece(t).append(")");
            case WEEK -> w -> w.append("EXTRACT(ISOWEEK FROM ").piece(t).append(")");
            case DAY_OF_YEAR -> w -> w.append("EXTRACT(DAYOFYEAR FROM ").piece(t).append(")");
            case ISO_DAY_OF_WEEK -> w -> w.append("(MOD(EXTRACT(DAYOFWEEK FROM ").piece(t).append(") + 5, 7) + 1)");
            case MILLISECOND, MICROSECOND, NANOSECOND -> throw new DialectCapability("date part " + part
                    + " has no measured BigQuery spelling");
        };
    }

    /** The base's contract: year/quarter/month/week deliver a DATE, finer parts a DATETIME; DuckDB's week starts on
     *  Monday (BigQuery's WEEK on Sunday, measured). */
    private static SqlWriter.Piece dateTrunc(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        return switch (part) {
            case YEAR, QUARTER, MONTH, WEEK -> w -> w.append("DATE_TRUNC(CAST(").expr(a.get(1), 0)
                    .append(" AS DATE), " + (part == DatePart.WEEK ? "WEEK(MONDAY)" : unit(part)) + ")");
            case DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> w -> w
                    .append("DATETIME_TRUNC(CAST(").expr(a.get(1), 0)
                    .append(" AS DATETIME), " + unit(part) + ")");
            case DAY_OF_YEAR, ISO_DAY_OF_WEEK, NANOSECOND -> throw new DialectCapability("date_trunc part " + part
                    + " has no measured BigQuery spelling");
        };
    }

    /** DuckDB's date_diff counts BOUNDARIES crossed, as DATETIME_DIFF does (23:59:59 to 00:00 is a day, 01-31 to
     *  02-01 a month, an epoch-straddling half millisecond -1, measured); DuckDB's week is whole days over 7,
     *  truncated (Saturday to Monday is 0, -7 days -1, measured). */
    private static SqlWriter.Piece dateDiff(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        SqlWriter.Piece from = w -> w.expr(a.get(1), 0);
        SqlWriter.Piece to = w -> w.expr(a.get(2), 0);
        return switch (part) {
            case YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND ->
                    w -> w.append("DATETIME_DIFF(CAST(").piece(to).append(" AS DATETIME), CAST(").piece(from)
                            .append(" AS DATETIME), " + unit(part) + ")");
            case WEEK -> w -> w.append("DIV(DATE_DIFF(CAST(").piece(to).append(" AS DATE), CAST(").piece(from)
                    .append(" AS DATE), DAY), 7)");
            case DAY_OF_YEAR, ISO_DAY_OF_WEEK, NANOSECOND -> throw new DialectCapability("date_diff part " + part
                    + " has no measured BigQuery spelling");
        };
    }

    /** A unit as BigQuery's EXTRACT, DATE_TRUNC, DATETIME_DIFF and INTERVAL name it. */
    private static String unit(DatePart part) {
        return switch (part) {
            case YEAR -> "YEAR";
            case QUARTER -> "QUARTER";
            case MONTH -> "MONTH";
            case WEEK -> "WEEK";
            case DAY -> "DAY";
            case HOUR -> "HOUR";
            case MINUTE -> "MINUTE";
            case SECOND -> "SECOND";
            case MILLISECOND -> "MILLISECOND";
            case MICROSECOND -> "MICROSECOND";
            case DAY_OF_YEAR, ISO_DAY_OF_WEEK, NANOSECOND -> throw new DialectCapability("a " + part
                    + " unit has no BigQuery spelling");
        };
    }

    private static String intervalUnit(DatePart part) {
        if (part == DatePart.QUARTER) {
            throw new DialectCapability("interval unit " + part + " has no BigQuery spelling");
        }
        return unit(part);
    }

    /** {@code a} floor-divided by a positive {@code b}, in INT64 (DIV truncates toward zero). */
    private static SqlWriter.Piece floorDiv(SqlWriter.Piece a, SqlWriter.Piece b) {
        return w -> w.append("(DIV(").piece(a).append(", ").piece(b).append(") - IF(MOD(").piece(a).append(", ")
                .piece(b).append(") < 0, 1, 0))");
    }

    /** DuckDB's time_bucket with the base's origins (1970-01-01; weeks the Monday 1969-12-29): fixed units binned in
     *  microseconds, floored; months and years by the month index since 1970-01, floored to the bucket's size in
     *  months (before 1970 too, measured on DuckDB). A DATETIME. */
    private static SqlWriter.Piece timeBucket(List<SqlExpr> a) {
        DatePart unit = part(a.get(0));
        SqlWriter.Piece t = w -> w.append("CAST(").expr(a.get(2), 0).append(" AS DATETIME)");
        SqlWriter.Piece n = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS INT64)");
        Long micros = bucketMicros(unit);
        if (micros != null) {
            long origin = unit == DatePart.WEEK ? -3L * 86400000000L : 0L;
            SqlWriter.Piece m = w -> w.append("(UNIX_MICROS(TIMESTAMP(").piece(t).append(")) - (" + origin + "))");
            SqlWriter.Piece size = w -> w.append("(").piece(n).append(" * " + micros + ")");
            return w -> w.append("DATETIME(TIMESTAMP_MICROS(").piece(floorDiv(m, size)).append(" * ").piece(size)
                    .append(" + (" + origin + ")))");
        }
        int months = unit == DatePart.MONTH ? 1 : unit == DatePart.YEAR ? 12 : 0;
        if (months == 0) {
            throw new DialectCapability("a " + unit + " time bucket has no BigQuery spelling");
        }
        SqlWriter.Piece k = w -> w.append("((EXTRACT(YEAR FROM ").piece(t)
                .append(") - 1970) * 12 + EXTRACT(MONTH FROM ").piece(t).append(") - 1)");
        SqlWriter.Piece size = w -> w.append("(").piece(n).append(" * " + months + ")");
        return w -> w.append("CAST(DATE_ADD(DATE '1970-01-01', INTERVAL ").piece(floorDiv(k, size)).append(" * ")
                .piece(size).append(" MONTH) AS DATETIME)");
    }

    /** A fixed-length bucket's length in microseconds; null for a calendar one. */
    private static @com.legend.base.Nullable Long bucketMicros(DatePart unit) {
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

    // ==================================================================
    // Date format codes (FORMAT_DATETIME / PARSE_DATETIME)
    // ==================================================================

    /** A part's FORMAT_DATETIME code, or null for one with none (spelled by {@link #strftime}). */
    private static @com.legend.base.Nullable String code(DateFmt p) {
        return switch (p) {
            case DateFmt.Text t -> t.s().replace("%", "%%");
            case DateFmt.Part part -> switch (part) {
                case YEAR4 -> "%Y";
                case MONTH2 -> "%m";
                case DAY2 -> "%d";
                case HOUR2 -> "%H";
                case MIN2 -> "%M";
                case SEC2 -> "%S";
                case MONTH_ABBREV -> "%b";
                case MONTH_NAME -> "%B";
                case WEEKDAY_NAME -> "%A";
                case HOUR12 -> "%I";
                case AMPM -> "%p";
                // DuckDB's %g: the milliseconds, three digits (the part's engine convention, as Snowflake's FF3)
                case SUBSEC_MICRO, SUBSEC_NANO, SUBSEC_MIN, HOUR12_NOPAD -> null;
            };
        };
    }

    /** Seconds, a point and three (six, nine) fraction digits as one {@code %E3S} ({@code %E6S}): seconds with that many
     *  digits, measured. */
    private static @com.legend.base.Nullable String secondsWithFraction(List<DateFmt> parts, int i) {
        if (parts.get(i) == DateFmt.Part.SEC2 && i + 2 < parts.size() && parts.get(i + 1) instanceof DateFmt.Text t
                && ".".equals(t.s())) {
            if (parts.get(i + 2) == DateFmt.Part.SUBSEC_MICRO) {
                return "%E6S";
            }
            if (parts.get(i + 2) == DateFmt.Part.SUBSEC_MIN) {
                return "%E3S";
            }
            if (parts.get(i + 2) == DateFmt.Part.SUBSEC_NANO) {
                return "%E6S000";
            }
        }
        return null;
    }

    /** The format for PARSE_DATETIME (and any format literal): every part a code. */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        List<DateFmt> parts = fl.parts();
        for (int i = 0; i < parts.size(); i++) {
            String combined = secondsWithFraction(parts, i);
            if (combined != null) {
                out.append(combined);
                i += 2;
                continue;
            }
            String code = code(parts.get(i));
            if (code == null) {
                throw new DialectCapability("date format part " + parts.get(i) + " has no BigQuery code outside a"
                        + " formatted date");
            }
            out.append(code);
        }
        return out.toString();
    }

    /** A date-time formatted by DuckDB's strftime parts: runs of codes as FORMAT_DATETIME, concatenated with the
     *  parts that have no code -- a fraction without its seconds ({@code SUBSTR(FORMAT_DATETIME('%E6S', t), 4)}) and
     *  the unpadded 12-hour clock ({@code LTRIM} of the space-padded {@code %l}), measured. */
    private SqlWriter.Piece strftime(SqlExpr value, SqlExpr.FormatLit fl) {
        SqlWriter.Piece t = w -> w.append("CAST(").expr(value, 0).append(" AS DATETIME)");
        List<SqlWriter.Piece> pieces = new ArrayList<>();
        StringBuilder run = new StringBuilder();
        List<DateFmt> parts = fl.parts();
        for (int i = 0; i < parts.size(); i++) {
            String combined = secondsWithFraction(parts, i);
            if (combined != null) {
                run.append(combined);
                i += 2;
                continue;
            }
            DateFmt p = parts.get(i);
            String code = code(p);
            if (code != null) {
                run.append(code);
                continue;
            }
            if (run.length() > 0) {
                pieces.add(formatted(run.toString(), t));
                run.setLength(0);
            }
            SqlWriter.Piece micro = w -> w.append("SUBSTR(").piece(formatted("%E6S", t)).append(", 4)");
            SqlWriter.Piece milli = w -> w.append("SUBSTR(").piece(formatted("%E3S", t)).append(", 4)");
            pieces.add(p == DateFmt.Part.SUBSEC_MICRO ? micro
                    : p == DateFmt.Part.SUBSEC_MIN ? milli
                    : p == DateFmt.Part.SUBSEC_NANO ? w -> w.append("CONCAT(").piece(micro).append(", '000')")
                    : w -> w.append("LTRIM(").piece(formatted("%l", t)).append(")"));
        }
        if (run.length() > 0 || pieces.isEmpty()) {
            pieces.add(formatted(run.toString(), t));
        }
        if (pieces.size() == 1) {
            return pieces.get(0);
        }
        return w -> w.append("CONCAT(").join(pieces, ", ", (x, piece) -> x.piece(piece)).append(")");
    }

    private SqlWriter.Piece formatted(String format, SqlWriter.Piece t) {
        String f = stringLit(format);
        return w -> w.append("FORMAT_DATETIME(" + f + ", ").piece(t).append(")");
    }

    // ==================================================================
    // Hooks of the base
    // ==================================================================

    /** Pure's hashCode is a signed 64-bit integer: FARM_FINGERPRINT is one, of the value's text as JSON (several
     *  values as one JSON array). The JSON, not the bare text: FARM_FINGERPRINT(NULL) is NULL (measured), and the hash
     *  of the empty collection is a number (DuckDB's hash(NULL) is one); a NULL's JSON is 'null', no value's is. */
    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() == 1) {
            return writer.append("FARM_FINGERPRINT(TO_JSON_STRING(").piece(asText(a.get(0))).append("))");
        }
        return writer.append("FARM_FINGERPRINT(TO_JSON_STRING([").join(a, ", ", (w, x) -> w.piece(asText(x)))
                .append("]))");
    }

    /** Pure round is half-EVEN: ROUND(x, n, 'ROUND_HALF_EVEN') exists over NUMERIC and BIGNUMERIC only (measured), so
     *  the value is a BIGNUMERIC (a FLOAT64's shortest decimal: 2.675 rounds to 2.68, as DuckDB's round_even does). */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        writer.append("ROUND(CAST(").expr(a.get(0), 0).append(" AS BIGNUMERIC), ");
        if (a.size() > 1) {
            writer.append("CAST(").expr(a.get(1), 0).append(" AS INT64)");
        } else {
            writer.append("0");
        }
        return writer.append(", 'ROUND_HALF_EVEN')");
    }

    /** INT64 bit operators; {@code >>} is a LOGICAL shift here (-16 >> 2 is 4611686018427387900, measured), DuckDB's
     *  arithmetic (-4): a negative value shifts its complement. */
    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        SqlWriter.Piece x = w -> w.append("CAST(").expr(a.get(0), 0).append(" AS INT64)");
        SqlWriter.Piece y = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS INT64)");
        return switch (fnName) {
            case BIT_AND -> writer.append("(").piece(x).append(" & ").piece(y).append(")");
            case BIT_OR -> writer.append("(").piece(x).append(" | ").piece(y).append(")");
            case BIT_XOR -> writer.append("(").piece(x).append(" ^ ").piece(y).append(")");
            case BIT_SHIFT_LEFT -> writer.append("(").piece(x).append(" << ").piece(y).append(")");
            case BIT_SHIFT_RIGHT -> writer.append("(CASE WHEN ").piece(x).append(" < 0 THEN ~((~").piece(x)
                    .append(") >> ").piece(y).append(") ELSE ").piece(x).append(" >> ").piece(y).append(" END)");
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
    }

    // ==================================================================
    // Aggregates and windows
    // ==================================================================

    /** An ORDER BY key of an aggregate or a window, its NULLs placed by a leading {@code (k) IS NULL} key: BigQuery
     *  refuses a non-default NULLS clause in both (measured), and the extra key changes no peer group. */
    private static SqlWriter.Piece orderKey(SqlWriter.Piece key, boolean ascending, boolean nullsFirst) {
        return w -> w.append("(").piece(key).append(") IS NULL").append(nullsFirst ? " DESC, " : " ASC, ").piece(key)
                .append(ascending ? " ASC" : " DESC");
    }

    /** An aggregate's own order key: its declared null placement, else DuckDB's default (NULLs last). */
    private SqlWriter.Piece aggregateKey(SqlSelect.SortKey k) {
        return orderKey(w -> w.expr(k.expr(), 0), k.ascending(),
                k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST);
    }

    /** A window's order key: its declared null placement, else NULL is largest (the base's bare-key rule). */
    private SqlWriter.Piece windowKey(SqlSelect.SortKey k) {
        return orderKey(w -> w.expr(k.expr(), 0), k.ascending(), nullsFirst(k));
    }

    private static boolean nullsFirst(SqlSelect.SortKey k) {
        return k.nullOrder() != null ? k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST : !k.ascending();
    }

    private static SqlWriter.Piece aggregateArgs(SqlAgg.Reducer r) {
        return w -> w.append(r.distinct() ? "DISTINCT " : "").list(r.args());
    }

    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM -> aggregate(unordered(writer, r), "SUM", r);
            case COUNT -> aggregate(unordered(writer, r), "COUNT", r);
            case AVG -> aggregate(unordered(writer, r), "AVG", r);
            case MIN -> aggregate(unordered(writer, r), "MIN", r);
            case MAX -> aggregate(unordered(writer, r), "MAX", r);
            case ANY_VALUE -> aggregate(unordered(writer, r), "ANY_VALUE", r);
            case STDDEV_SAMP -> aggregate(unordered(writer, r), "STDDEV_SAMP", r);
            case STDDEV_POP -> aggregate(unordered(writer, r), "STDDEV_POP", r);
            case VAR_SAMP -> aggregate(unordered(writer, r), "VAR_SAMP", r);
            case VAR_POP -> aggregate(unordered(writer, r), "VAR_POP", r);
            case MEDIAN -> median(writer, r);
            case MODE -> modeArm(writer, r);
            case STRING_AGG -> orderedAggregate(writer, "STRING_AGG", r);
            case LIST -> arrayAgg(writer, r);
            case QUANTILE_CONT, QUANTILE_DISC -> percentile(writer, r);
            case CORR -> aggregate(unordered(writer, r), "CORR", r);
            case COVAR_SAMP -> aggregate(unordered(writer, r), "COVAR_SAMP", r);
            case COVAR_POP -> aggregate(unordered(writer, r), "COVAR_POP", r);
            // the value at the key's greatest (least) row, rows of a NULL key ignored
            case ARG_MAX -> byExtreme(writer, "MAX_BY", r);
            case ARG_MIN -> byExtreme(writer, "MIN_BY", r);
            case BOOL_AND -> unordered(writer, r).append("LOGICAL_AND(").piece(aggregateArgs(r)).append(")");
            case BOOL_OR -> unordered(writer, r).append("LOGICAL_OR(").piece(aggregateArgs(r)).append(")");
            case VARIANCE -> aggregate(unordered(writer, r), "VARIANCE", r);
            case STDDEV -> aggregate(unordered(writer, r), "STDDEV", r);
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // a window function is a ranking or value function, never an aggregate (windowCall writes it)
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

    /** {@code writer}, for an aggregate BigQuery cannot order: an ordered one is refused. */
    private static SqlWriter unordered(SqlWriter writer, SqlAgg.Reducer r) {
        if (!r.orderBy().isEmpty()) {
            throw new DialectCapability("an ordered " + r.fn() + " reached BigQuery");
        }
        return writer;
    }

    /** {@code name([DISTINCT ]args[ ORDER BY keys])}: the string and list aggregates. */
    private SqlWriter orderedAggregate(SqlWriter writer, String name, SqlAgg.Reducer r) {
        writer.append(name).append("(").piece(aggregateArgs(r));
        if (!r.orderBy().isEmpty()) {
            writer.append(" ORDER BY ").join(r.orderBy(), ", ", (w, k) -> w.piece(aggregateKey(k)));
        }
        return writer.append(")");
    }

    /** The {@code LIST} arm of {@link #reducer}: ARRAY_AGG of one value that is no list. */
    private SqlWriter arrayAgg(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1 || typeOf(r.args().get(0)) instanceof SqlType.Array) {
            throw r.args().size() != 1 ? new DialectCapability("a LIST aggregate of " + r.args().size()
                    + " arguments reached BigQuery") : nestedList();
        }
        return orderedAggregate(writer, "ARRAY_AGG", r);
    }

    /** The {@code MEDIAN} arm of {@link #reducer}. */
    private SqlWriter median(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1 || r.distinct()) {
            throw new DialectCapability("a median of this shape reached BigQuery");
        }
        return writer.piece(quantile(SqlAgg.Fn.QUANTILE_CONT, r.args().get(0), w -> w.append("0.5"), r));
    }

    /** The {@code QUANTILE_CONT} and {@code QUANTILE_DISC} arms of {@link #reducer}. */
    private SqlWriter percentile(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct() || r.orderBy().size() > 1) {
            throw new DialectCapability(r.fn() + " of this shape reached BigQuery");
        }
        return writer.piece(quantile(r.fn(), r.args().get(0), w -> w.expr(r.args().get(1), 0), r));
    }

    /** {@code MAX_BY(value, key)} / {@code MIN_BY}: the value at the key's greatest (least) row. */
    private static SqlWriter byExtreme(SqlWriter writer, String name, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct() || !r.orderBy().isEmpty()) {
            throw new DialectCapability(r.fn() + " of this shape reached BigQuery");
        }
        return writer.append(name).append("(").list(r.args()).append(")");
    }

    /**
     * A grouped quantile (PERCENTILE_CONT/DISC are analytic only here, measured): the group's non-NULL values sorted
     * ({@code ARRAY_AGG(x IGNORE NULLS ORDER BY x)}, its own order key's direction) and indexed -- continuous: the
     * value at {@code q * (n - 1)} interpolated linearly; discrete: the first value whose cumulative share reaches
     * {@code q}, {@code ceil(n * q) - 1} (both DuckDB's, measured: 2.8 and 3 at 0.6 over 1..4). No value is NULL.
     */
    private static SqlWriter.Piece quantile(SqlAgg.Fn fn, SqlExpr x, SqlWriter.Piece q, SqlAgg.Reducer r) {
        boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
        SqlWriter.Piece sorted = w -> w.append("ARRAY_AGG(").expr(x, 0).append(" IGNORE NULLS ORDER BY ").expr(x, 0)
                .append(desc ? " DESC)" : " ASC)");
        SqlWriter.Piece n = w -> w.append("COUNT(").expr(x, 0).append(")");
        SqlWriter.Piece qd = w -> w.append("CAST(").piece(q).append(" AS FLOAT64)");
        if (fn == SqlAgg.Fn.QUANTILE_DISC) {
            return w -> w.piece(sorted).append("[SAFE_OFFSET(GREATEST(CAST(CEIL(").piece(n).append(" * ").piece(qd)
                    .append(") AS INT64) - 1, 0))]");
        }
        SqlWriter.Piece pos = w -> w.append("(").piece(qd).append(" * (").piece(n).append(" - 1))");
        SqlWriter.Piece lo = w -> w.append("CAST(").piece(sorted).append("[SAFE_OFFSET(CAST(FLOOR(").piece(pos)
                .append(") AS INT64))] AS FLOAT64)");
        SqlWriter.Piece hi = w -> w.append("CAST(").piece(sorted).append("[SAFE_OFFSET(CAST(CEIL(").piece(pos)
                .append(") AS INT64))] AS FLOAT64)");
        return w -> w.append("(").piece(lo).append(" + (").piece(hi).append(" - ").piece(lo).append(") * (")
                .piece(pos).append(" - FLOOR(").piece(pos).append(")))");
    }

    private static final Set<SqlAgg.Fn> ORDERED_SETS = Set.of(SqlAgg.Fn.MEDIAN, SqlAgg.Fn.MODE,
            SqlAgg.Fn.QUANTILE_CONT, SqlAgg.Fn.QUANTILE_DISC);

    /** A windowed average is a DOUBLE (the pass sees only grouped ones); a median or quantile over a whole partition is
     *  the native analytic PERCENTILE_CONT/DISC (no ORDER BY or frame allowed there). */
    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        switch (w.fn()) {
            case SqlAgg.Reducer r when ORDERED_SETS.contains(r.fn()) -> {
                return orderedSetOver(writer, r, w);
            }
            case SqlAgg.Reducer r when r.fn() == SqlAgg.Fn.AVG -> {
                writer.append("CAST(");
                reducer(writer, r);
                return bqOver(writer, w).append(" AS FLOAT64)");
            }
            case SqlAgg.Reducer r -> reducer(writer, r);
            case SqlAgg.RankingFn r -> writer.append(aggregateName(r.fn())).append("(").list(r.args()).append(")");
            case SqlAgg.ValueFn v -> writer.append(aggregateName(v.fn())).append("(").list(v.args()).append(")");
        }
        return bqOver(writer, w);
    }

    private SqlWriter orderedSetOver(SqlWriter writer, SqlAgg.Reducer r, SqlExpr.WindowCall w) {
        if (r.fn() == SqlAgg.Fn.MODE) {
            return modeArm(writer, w);
        }
        if (!w.orderBy().isEmpty() || w.frame() != null || r.distinct()) {
            throw new DialectCapability(r.fn() + " over an ordered or framed window reached BigQuery, whose"
                    + " PERCENTILE_* take a partition only");
        }
        if (!r.orderBy().isEmpty() && !r.orderBy().get(0).ascending()) {
            return descendingOver(writer, r, w);
        }
        if (r.fn() == SqlAgg.Fn.MEDIAN) {
            writer.append("PERCENTILE_CONT(").expr(r.args().get(0), 0).append(", 0.5)");
        } else {
            writer.append(r.fn() == SqlAgg.Fn.QUANTILE_CONT ? "PERCENTILE_CONT(" : "PERCENTILE_DISC(")
                    .expr(r.args().get(0), 0).append(", ").expr(r.args().get(1), 0).append(")");
        }
        return bqOver(writer, w);
    }

    /**
     * A descending continuous quantile over a window: PERCENTILE_CONT orders ascending only, and its fraction must be a
     * literal (measured). Written as the DuckDB lane writes it, so its values are that lane's: over the values negated,
     * sorted ascending, the value at {@code q * (n - 1)} interpolated linearly, negated back --
     * {@code -(lo + (pos - FLOOR(pos)) * (hi - lo))}. The position is exact, as DuckDB's (its {@code 0.6} is a DECIMAL):
     * a literal fraction is a BIGNUMERIC of its spelling, so {@code 0.6 * 2} is 1.2 and the fraction 0.2 (a FLOAT64
     * {@code 0.6} would give 0.19999999999999996, and 1.4000000000000001 for 1.4, measured). The sorted values are an
     * analytic {@code ARRAY_AGG} in the window's order (measured), each a one-field struct: an analytic ARRAY_AGG takes
     * no IGNORE NULLS, and an array holds no NULL element, but a struct with a NULL field is one; NULLs sort last and no
     * position reaches them. A discrete one, or one of a value that is no number, is refused.
     */
    private SqlWriter descendingOver(SqlWriter writer, SqlAgg.Reducer r, SqlExpr.WindowCall w) {
        SqlExpr x = r.args().get(0);
        SqlType t = typeOf(x);
        boolean numeric = t == SqlType.Scalar.INTEGER || t == SqlType.Scalar.BIGINT || t == SqlType.Scalar.DOUBLE
                || t instanceof SqlType.Decimal;
        if (!numeric || r.fn() != SqlAgg.Fn.QUANTILE_CONT) {
            throw new DialectCapability("a descending " + r.fn() + " over a window of " + t + " reached BigQuery");
        }
        SqlExpr q = r.args().get(1);
        SqlWriter.Piece exactQ = q instanceof SqlExpr.FloatLit f
                ? wr -> wr.append("BIGNUMERIC '" + new java.math.BigDecimal(Double.toString(f.value())).toPlainString()
                        + "'")
                : q instanceof SqlExpr.DecimalLit d
                        ? wr -> wr.append("BIGNUMERIC '" + d.value().toPlainString() + "'")
                        : wr -> wr.expr(q, 0);
        SqlWriter.Piece partition = wr -> {
            if (!w.partitionBy().isEmpty()) {
                wr.append("PARTITION BY ").list(w.partitionBy());
            }
        };
        SqlWriter.Piece sorted = wr -> wr.append("ARRAY_AGG(STRUCT(-(").expr(x, 0).append(") AS v)) OVER (")
                .piece(partition).append(w.partitionBy().isEmpty() ? "" : " ").append("ORDER BY -(").expr(x, 0)
                .append(") ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)");
        SqlWriter.Piece pos = wr -> wr.append("(").piece(exactQ).append(" * (COUNT(").expr(x, 0).append(") OVER (")
                .piece(partition).append(") - 1))");
        SqlWriter.Piece lo = wr -> wr.piece(sorted).append("[SAFE_OFFSET(CAST(FLOOR(").piece(pos)
                .append(") AS INT64))].v");
        SqlWriter.Piece hi = wr -> wr.piece(sorted).append("[SAFE_OFFSET(CAST(CEIL(").piece(pos)
                .append(") AS INT64))].v");
        boolean dbl = typeOf(w) == SqlType.Scalar.DOUBLE;
        writer.append(dbl ? "CAST(-(" : "(-(").piece(lo).append(" + (").piece(pos).append(" - FLOOR(").piece(pos)
                .append(")) * (").piece(hi).append(" - ").piece(lo).append("))");
        return writer.append(dbl ? " AS FLOAT64)" : ")");
    }

    /** A window's {@code OVER (...)}. Its order keys place NULLs by a leading IS NULL key; a RANGE frame with an
     *  offset takes ONE numeric key (measured) and no non-default NULLS clause: a date or date-time key is its day
     *  or microsecond number (an interval offset in that unit), and NULLs placed after the values as +inf. */
    private SqlWriter bqOver(SqlWriter writer, SqlExpr.WindowCall w) {
        writer.append(" OVER (");
        boolean any = false;
        if (!w.partitionBy().isEmpty()) {
            writer.append("PARTITION BY ").list(w.partitionBy());
            any = true;
        }
        SqlExpr.WindowCall.Frame f = w.frame();
        Long scale = null;
        if (!w.orderBy().isEmpty()) {
            writer.append(any ? " " : "").append("ORDER BY ");
            if (f != null && f.kind() == SqlExpr.WindowCall.Frame.Kind.RANGE && (offset(f.from()) || offset(f.to()))) {
                if (w.orderBy().size() != 1) {
                    throw new DialectCapability("a RANGE frame over " + w.orderBy().size() + " keys reached BigQuery");
                }
                SqlSelect.SortKey k = w.orderBy().get(0);
                boolean interval = intervalBound(f.from()) || intervalBound(f.to());
                SqlType kt = typeOf(k.expr());
                SqlWriter.Piece key;
                if (interval && kt == SqlType.Scalar.DATE) {
                    key = x -> x.append("UNIX_DATE(").expr(k.expr(), 0).append(")");
                    scale = -1L;
                } else if (interval && kt == SqlType.Scalar.TIMESTAMP) {
                    key = x -> x.append("UNIX_MICROS(TIMESTAMP(").expr(k.expr(), 0).append("))");
                    scale = 1L;
                } else if (interval) {
                    throw new DialectCapability("an interval RANGE frame over a " + kt + " key reached BigQuery");
                } else {
                    key = x -> x.expr(k.expr(), 0);
                }
                // BigQuery's own placement is ASC NULLS FIRST, DESC NULLS LAST
                boolean moved = nullsFirst(k) == !k.ascending();
                SqlWriter.Piece placed = moved ? x -> x.append("IFNULL(CAST(").piece(key)
                        .append(" AS FLOAT64), CAST('inf' AS FLOAT64))") : key;
                writer.piece(placed).append(k.ascending() ? " ASC" : " DESC");
            } else {
                writer.join(w.orderBy(), ", ", (x, k) -> x.piece(windowKey(k)));
            }
            any = true;
        }
        if (f != null) {
            writer.append(any ? " " : "").append(f.kind().toString()).append(" BETWEEN ")
                    .append(frameBound(f.from(), scale)).append(" AND ").append(frameBound(f.to(), scale));
        }
        return writer.append(")");
    }

    private static boolean offset(SqlExpr.WindowCall.Frame.Bound b) {
        return b instanceof SqlExpr.WindowCall.Frame.Bound.Preceding
                || b instanceof SqlExpr.WindowCall.Frame.Bound.Following || intervalBound(b);
    }

    private static boolean intervalBound(SqlExpr.WindowCall.Frame.Bound b) {
        return b instanceof SqlExpr.WindowCall.Frame.Bound.IntervalPreceding
                || b instanceof SqlExpr.WindowCall.Frame.Bound.IntervalFollowing;
    }

    /** A frame bound; an interval offset in the key's number unit ({@code scale}: -1 days, 1 microseconds). */
    private String frameBound(SqlExpr.WindowCall.Frame.Bound b, @com.legend.base.Nullable Long scale) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> bound(b);
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> bound(b);
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow cr -> bound(b);
            case SqlExpr.WindowCall.Frame.Bound.Following fo -> bound(b);
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> bound(b);
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> intervalOffset(p.n(), p.unit(), scale)
                    + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing fo -> intervalOffset(fo.n(), fo.unit(), scale)
                    + " FOLLOWING";
        };
    }

    private static long intervalOffset(long n, String unit, @com.legend.base.Nullable Long scale) {
        String u = unit.toUpperCase(Locale.ROOT);
        Long micros = FRAME_MICROS.get(u);
        if (scale == null || micros == null || scale < 0 && micros % 86400000000L != 0) {
            throw new DialectCapability("a " + unit + " window frame offset over this key reached BigQuery");
        }
        return Math.multiplyExact(n, scale < 0 ? micros / 86400000000L : micros);
    }

    /** DurationUnit name to microseconds (months and years have no fixed length). */
    private static final Map<String, Long> FRAME_MICROS = Map.of("WEEKS", 7L * 86400000000L, "DAYS", 86400000000L,
            "HOURS", 3600000000L, "MINUTES", 60000000L, "SECONDS", 1000000L, "MILLISECONDS", 1000L);

    // ==================================================================
    // JSON constructors (JSON objects keep their keys sorted, measured)
    // ==================================================================

    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        return writer.append("JSON_OBJECT(").list(j.kv()).append(")");
    }

    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("JSON_ARRAY(").list(j.elements()).append(")");
    }

    /** A JSON array of the values, NULLs kept as JSON nulls; over no rows the empty array (TO_JSON of a NULL array is
     *  JSON null, not SQL NULL, measured -- so the empty array is put in before). */
    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        writer.append("TO_JSON(IFNULL(ARRAY_AGG(TO_JSON(").expr(j.value(), 0).append(")");
        if (!j.orderKeys().isEmpty()) {
            writer.append(" ORDER BY ").join(j.orderKeys(), ", ",
                    (w, k) -> w.piece(orderKey(x -> x.expr(k.expr(), 0), !k.desc(), false)));
        }
        return writer.append("), ARRAY<JSON>[]))");
    }

    // ==================================================================
    // Plan parameters
    // ==================================================================

    /** A date-time parameter is cast to DATETIME (the naive type); finer than a microsecond, its first six digits. */
    @Override
    protected RenderedStatement.TypeSpelling holeType(com.legend.sql.ValueKind kind) {
        return switch (kind) {
            case DATE -> new RenderedStatement.TypeSpelling("DATE", RenderedStatement.Digits.NONE);
            case DATE_TIME -> new RenderedStatement.TypeSpelling("DATETIME", RenderedStatement.Digits.NONE);
            case DATE_TIME_NANOS -> new RenderedStatement.TypeSpelling("DATETIME", RenderedStatement.Digits.NONE, 6);
            case INTEGER, DECIMAL, FLOATING -> super.holeType(kind);
        };
    }

    // ==================================================================
    // Values: what the driver hands back
    // ==================================================================

    /** The driver hands an ARRAY as a java.sql.Array and a STRUCT as a java.sql.Struct (measured: BigQueryJsonArray,
     *  BigQueryJsonStruct), which the executor unpacks; a JSON cell is its text; DATETIME and TIMESTAMP are
     *  java.sql.Timestamp, NUMERIC and BIGNUMERIC BigDecimal (scale as written: trailing zeros are not kept). A struct
     *  arriving as JSON text is decoded to the platform's map of its fields. */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            @com.legend.base.Nullable SqlType type) {
        if (jdbcValue instanceof String s && type instanceof SqlType.Struct && s.startsWith("{")) {
            return com.legend.sql.Json.parse(s);
        }
        // a BIGNUMERIC drops a trailing zero's scale (BIGNUMERIC '3.0' reads 3, measured): the declared scale is the
        // value's own, so the digits are padded back -- never rounded
        if (jdbcValue instanceof java.math.BigDecimal b && type instanceof SqlType.Decimal d && b.scale() < d.scale()) {
            return b.setScale(d.scale());
        }
        return jdbcValue;
    }

    // ==================================================================
    // DDL and DML (test fixtures)
    // ==================================================================

    /** GoogleSQL DDL: a temporary table lives in a session (a connection opened with {@code EnableSession}; outside
     *  one BigQuery refuses it, measured), unqualified; a key is declared NOT ENFORCED. */
    @Override
    public String render(com.legend.sql.SqlDdl ddl) {
        return switch (ddl) {
            case com.legend.sql.SqlDdl.CreateTable ct -> {
                StringBuilder sb = new StringBuilder(ct.temporary() ? "CREATE TEMP TABLE " : "CREATE TABLE ")
                        .append(qualified(ct.schema(), ct.table(), ct.quoting())).append(" (");
                List<String> keys = new ArrayList<>();
                for (int i = 0; i < ct.columns().size(); i++) {
                    com.legend.sql.SqlDdl.Column col = ct.columns().get(i);
                    sb.append(i > 0 ? ", " : "").append(ident(col.name())).append(' ').append(ddlType(col.type()))
                            .append(col.primaryKey() || col.notNull() ? " NOT NULL" : "");
                    if (col.primaryKey()) {
                        keys.add(ident(col.name()));
                    }
                }
                if (!keys.isEmpty()) {
                    sb.append(", PRIMARY KEY (").append(String.join(", ", keys)).append(") NOT ENFORCED");
                }
                yield sb.append(");").toString();
            }
            case com.legend.sql.SqlDdl.DropTable dt -> "DROP TABLE IF EXISTS " + qualified(dt.schema(), dt.table(), dt.quoting())
                    + ";";
            case com.legend.sql.SqlDdl.CreateSchema cs -> "CREATE SCHEMA IF NOT EXISTS " + storedName(cs.schema(), cs.quoted())
                    + ";";
            case com.legend.sql.SqlDdl.DropSchema ds -> "DROP SCHEMA IF EXISTS " + storedName(ds.schema(), ds.quoted())
                    + " CASCADE;";
        };
    }

    /** A DELETE needs a WHERE clause in GoogleSQL. */
    @Override
    public String render(com.legend.sql.SqlDml dml) {
        if (dml instanceof com.legend.sql.SqlDml.DeleteAll da) {
            return "DELETE FROM " + qualified(da.schema(), da.table(), da.quoting()) + " WHERE TRUE;";
        }
        return super.render(dml);
    }

    private String qualified(@com.legend.base.Nullable String schema, String table, com.legend.sql.NameQuoting quoting) {
        return schema == null || schema.isEmpty() || "default".equals(schema)
                ? storedName(table, quoting.table())
                : storedName(schema, quoting.schema()) + "." + storedName(table, quoting.table());
    }

    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT, SMALLINT, TINYINT, INTEGER -> "INT64";
                case FLOAT, DOUBLE, REAL -> "FLOAT64";
                case BIT -> "BOOL";
                case TIMESTAMP -> "DATETIME";
                case DATE -> "DATE";
                case TIME -> "TIME";
                case JSON -> "JSON";
                case VARCHAR -> "STRING";
                // a nanosecond timestamp column: BigQuery's DATETIME keeps microseconds
                case TIMESTAMP_NS, OTHER, DISTINCT, ARRAY, OBJECT -> throw new DialectCapability("a " + p.kind()
                        + " column reached BigQuery's DDL");
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> z.kind().toUpperCase(Locale.ROOT).contains("BINARY")
                    ? "BYTES" : "STRING";
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> s.scale() <= 9 && s.precision() - s.scale() <= 29
                    ? "NUMERIC(" + s.precision() + ", " + s.scale() + ")"
                    : "BIGNUMERIC(" + s.precision() + ", " + s.scale() + ")";
        };
    }

    /** {@inheritDoc} Its JSON_TYPE answers DuckDB's names (a CASE over JSON_TYPE). */
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

    /** {@inheritDoc} Refused, as TYPEOF is here (its type names are BigQuery's). */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        throw new DialectCapability("typeof reached BigQuery: its type names are BigQuery's");
    }

    /** {@inheritDoc} INT64, FLOAT64 and DATETIME (the naive date-time; its TIMESTAMP, an instant, folds with every
     *  TIMESTAMP spelling) are its own names (2026-10-11). */
    @Override
    public String wireTypeName(@com.legend.base.Nullable String driverTypeName) {
        return WireNames.fold(driverTypeName == null ? null : switch (driverTypeName) {
            case "INT64" -> "BIGINT";
            case "FLOAT64" -> "DOUBLE";
            case "DATETIME" -> "TIMESTAMP";
            default -> driverTypeName;
        });
    }

    /** {@inheritDoc} Its exact decimal carrier (2026-10-11): every decimal is a BIGNUMERIC (a parameterized type cannot
     *  be cast to; its 38 digits each side hold every DECIMAL(p,s) and HUGEINT value), and a Float-declared column may
     *  arrive as it (the numeric charter's Rule 2, as a DECIMAL does). A map is carried as its entries, a list of
     *  (key, value) structs: an ARRAY at the wire, BigQuery having no map type. */
    @Override
    public boolean deliversOwn(SqlType label, String wire) {
        if (label instanceof SqlType.Map && wire.equals("ARRAY")) {
            return true;
        }
        if (!wire.equals("BIGNUMERIC")) {
            return false;
        }
        return label == SqlType.Scalar.HUGEINT || label == SqlType.Scalar.DOUBLE
                || label instanceof SqlType.Decimal d && d.scale() <= 38 && d.precision() - d.scale() <= 38;
    }
}
