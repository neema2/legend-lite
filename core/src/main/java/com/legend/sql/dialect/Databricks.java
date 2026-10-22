// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.List;
import java.util.Set;

/**
 * The Databricks SQL EXECUTION dialect (a SQL warehouse; 2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md). A sibling
 * of {@link Postgres} and {@link DuckDb} over {@link AnsiSqlRenderer}: it changes nothing the other dialects render.
 * Every spelling here was measured on the account before it was trusted (the probe statements are the doc's §8).
 *
 * <p>Spark SQL is DuckDB's nearest relative: typed {@code ARRAY<T>} with lambdas ({@code transform}, {@code filter},
 * {@code aggregate}, {@code exists}, {@code forall}), {@code STRUCT}, {@code MAP}, {@code VARIANT}, {@code QUALIFY},
 * native {@code PIVOT} and {@code SELECT * EXCEPT}. What differs, measured:
 * <ul>
 *   <li>a string literal reads backslash escapes, and two adjacent literals concatenate ({@code 'it''s'} is
 *       {@code its}): a quote is written {@code \'} and a backslash {@code \\};</li>
 *   <li>a bare integer literal is a 32-bit INT, whose arithmetic overflows at 2^31: every integer literal is a
 *       BIGINT ({@code 1L});</li>
 *   <li>the warehouse's ANSI mode is off by default (overflow wraps, a bad cast is NULL, x/0 is NULL): the session
 *       turns it on, so each raises as DuckDB does;</li>
 *   <li>{@code concat} is NULL-propagating ({@code concat_ws('', ...)} skips NULLs, as DuckDB's concat does);
 *       {@code split} and {@code regexp_extract} take a Java regex, the latter group 1 by default;</li>
 *   <li>a lambda's index parameter is 0-based (DuckDB's is 1-based);</li>
 *   <li>arrays, structs, maps and variants arrive over JDBC as JSON text ({@link #normalize}).</li>
 * </ul>
 *
 * <p>Every {@link SqlFn} and {@link SqlAgg.Fn} is decided HERE by an exhaustive switch: its spelling, a shared
 * helper of the base named in its arm, or a {@link DialectCapability} wall. A wall is honest; a
 * guessed spelling is a silent wrong answer.
 */
public final class Databricks extends AnsiSqlRenderer {

    /** The engine's H2 text is refused here: Databricks has no translation of it (decision D2: raw text runs on the
     *  database its connection declares; only the engine's H2 text is translated, for H2, DuckDB and Postgres). */
    @Override
    public String fromEngineText(String statement) {
        throw new DialectCapability("the engine's H2 text reached Databricks, which has no translation of it");
    }

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "BOOLEAN";
            case INTEGER -> "INT";
            case BIGINT -> "BIGINT";
            case HUGEINT -> "DECIMAL(38,0)";
            case DOUBLE -> "DOUBLE";
            case VARCHAR, LITERAL, TEMPORAL_TEXT -> "STRING";
            case DATE -> "DATE";
            case TIMESTAMP, TIMESTAMPTZ -> "TIMESTAMP";
            case JSON -> "VARIANT";
            // Databricks SQL has no TIME type
            case TIME -> throw new DialectCapability("a time of day: Databricks has no TIME type");
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** No java.time re-fetch (measured 2026-10-11): its driver answers getObject(int, LocalDateTime.class)
     *  with the timestamp as text. A timestamp cell keeps the driver's Timestamp;
     *  a BC date-time is designed, not built (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §15). */
    @Override
    public boolean refetchesTimestampAsJavaTime() {
        return false;
    }

    /** Databricks SQL (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md): backticks. The {@code Databricks}
     * dialect quotes EVERY identifier, so this list never decides a spelling there; it is Databricks' documented
     * reserved set (ANSI mode's reserved words) for any reader that asks. */
    public static final Lexicon LEXICON = new Lexicon('`', Set.of(
            "all", "alter", "and", "any", "array", "as", "at", "authorization", "between", "both", "by", "case",
            "cast", "check", "collate", "column", "commit", "constraint", "create", "cross", "cube", "current",
            "current_date", "current_time", "current_timestamp", "current_user", "delete", "describe", "distinct",
            "drop", "else", "end", "escape", "except", "exists", "external", "false", "fetch", "filter", "for",
            "foreign", "from", "full", "function", "global", "grant", "group", "grouping", "having", "in", "inner",
            "insert", "intersect", "interval", "into", "is", "join", "lateral", "leading", "left", "like", "local",
            "natural", "no", "not", "null", "of", "on", "only", "or", "order", "out", "outer", "overlaps",
            "partition", "position", "primary", "range", "references", "revoke", "right", "rollback", "rollup",
            "row", "rows", "select", "session_user", "set", "some", "start", "table", "tablesample", "then", "time",
            "to", "trailing", "true", "union", "unique", "unknown", "update", "user", "using", "values", "when",
            "where", "window", "with"));

    /** Databricks (2026-10-11): typed native arrays with lambdas, as DuckDB's (docs/CLOUD_BACKENDS_PCT_2026_10_11.md). */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(true, true, true, true);

    public Databricks() {
        super(LEXICON);
    }

    // ==================================================================
    // Session and structure
    // ==================================================================

    /** The platform's naive-UTC temporal contract, and ANSI mode: overflow, a bad cast and a division by zero raise,
     *  as DuckDB's do (the warehouse's default wraps, answers NULL, and NULL). */
    @Override
    public List<String> sessionSetup() {
        return List.of("SET TIME ZONE 'UTC'", "SET ANSI_MODE = true");
    }

    /** Native PIVOT takes its values from the query: a dynamic pivot is discovered first. */
    // the probe plan's facts (2026-10-20): Databricks: no multi-statement transaction; no result column limit documented
    @Override
    public KeysSnapshot keysSnapshot() {
        return KeysSnapshot.NONE;
    }

    @Override
    public @com.legend.base.Nullable Integer resultColumnLimit() {
        return null;
    }

    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    /** Native lists (the carrier strategies stand aside, as on DuckDB), the pivot's unqualified arguments, the
     *  substring start clamp (Spark reads a start of 0 as 1), averages delivered as DOUBLE (an average of a decimal
     *  is a decimal here), a constant GROUP BY or ORDER BY key as a typed expression (Spark reads an integer one
     *  as a position), and the root's DECIMAL(p,s) and HUGEINT columns cast to their declared types (Spark widens a
     *  computed decimal's precision, and a HUGEINT sum is a DECIMAL(20,0)). QUALIFY is native. */
    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS),
                new UnqualifyPivotArgs(), new CheckedDefectsToLists(),
                new SubstringClamp(), new AggregatesDeliverDouble(java.util.Set.of(SqlAgg.Fn.AVG)),
                new ConstantKeysAsExpressions(), new RootNumericTypes());
    }

    /** Several lists exploded side by side: DuckDB's unnests in one select are ALIGNED (the shorter padded with NULL),
     *  where Spark's generators would cross; one {@code inline(arrays_zip(...))} aligns them (measured). The form
     *  where every projection is such an unnest; any other mix is refused by name. */
    @Override
    protected SqlWriter select(SqlWriter writer, SqlSelect s, int depth) {
        if (s.from() instanceof SqlSource.Join j && j.kind() == SqlSource.Join.Kind.ASOF_LEFT) {
            return select(writer, asOf(s, j), depth);
        }
        long unnests = s.projections().stream().filter(p -> p.expr() instanceof SqlExpr.Call c
                && c.fn() == SqlFn.UNNEST).count();
        if (unnests < 2) {
            return super.select(writer, s, depth);
        }
        if (unnests != s.projections().size() || !(s.from() instanceof SqlSource.Dual) || s.where() != null
                || !s.groupBy().isEmpty() || s.qualify() != null || !s.orderBy().isEmpty() || s.limit() != null
                || s.offset() != null || s.distinct()) {
            throw new DialectCapability("several unnests beside other columns or clauses reached Databricks");
        }
        writer.append("SELECT inline(arrays_zip(").join(s.projections(), ", ",
                (w, p) -> w.piece(list(((SqlExpr.Call) p.expr()).args().get(0)))).append(")) AS (");
        return writer.join(s.projections(), ", ", (w, p) -> w.append(aliasIdent(java.util.Objects.requireNonNull(
                p.alias() != null ? p.alias() : p.outputName(), "an unnest's column name"))))
                .append(")");
    }

    @Override
    protected boolean supportsQualify() {
        return true;
    }

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
        throw new DialectCapability("row order reached Databricks, which has no row-order column");
    }

    @Override
    protected String asOfJoinClause() {
        throw new IllegalStateException("an as-of join reached the join clause: Databricks writes its select (asOf)");
    }

    /**
     * {@code s}, whose FROM is an as-of join, as Spark runs it: Spark has no as-of join, and refuses the portable
     * form's correlated MAX in a join condition (measured). A LEFT JOIN on the same condition, each left row keeping
     * its matches whose right key is the extreme one -- {@code QUALIFY k = MAX(k) OVER (PARTITION BY every left
     * column) OR MAX(k) OVER (...) IS NULL}, the second arm keeping a left row with no match (its null-extended row).
     * The portable form's rows, ties included. Only under a select that filters, groups and windows nothing of its
     * own (the lowering's {@code SELECT *} over the join): those would see the join's rows before the pick.
     */
    private static SqlSelect asOf(SqlSelect s, SqlSource.Join j) {
        CarrierStrategies.AsOfKey key = CarrierStrategies.asOfKey(j);
        if (key == null) {
            throw new DialectCapability("an as-of join with no inequality between its sides reached Databricks");
        }
        if (s.where() != null || !s.groupBy().isEmpty() || s.having() != null || s.qualify() != null
                || s.projections().stream().anyMatch(p -> windowed(p.expr()))) {
            throw new DialectCapability("an as-of join under a select that filters, groups or windows reached"
                    + " Databricks: its pick is that select's QUALIFY");
        }
        String left = j.left().alias();
        if (left == null || j.left().outputs().isEmpty()) {
            throw new DialectCapability("an as-of join whose left side has no alias and columns reached Databricks");
        }
        List<SqlExpr> partition = new java.util.ArrayList<>();
        for (com.legend.sql.OutputCol oc : j.left().outputs()) {
            partition.add(SqlExpr.Column.of(left, oc));
        }
        SqlExpr extreme = new SqlExpr.WindowCall(new SqlAgg.Reducer(key.pickMax() ? SqlAgg.Fn.MAX : SqlAgg.Fn.MIN,
                List.of(key.rightKey()), false, List.of()), partition, List.of(), null);
        SqlExpr pick = SqlExpr.Call.of(SqlFn.OR, SqlExpr.Call.of(SqlFn.EQUAL, key.rightKey(), extreme),
                SqlExpr.Call.of(SqlFn.IS_NULL, extreme));
        return s.withFrom(new SqlSource.Join(j.left(), j.right(), SqlSource.Join.Kind.LEFT, j.on()))
                .withQualify(pick);
    }

    private static boolean windowed(SqlExpr e) {
        if (e instanceof SqlExpr.WindowCall) {
            return true;
        }
        for (SqlExpr k : e.children()) {
            if (windowed(k)) {
                return true;
            }
        }
        return false;
    }

    /** An inline table, {@code VALUES (..), (..) AS t(a, b)}: a parenthesized VALUES is a syntax error here
     *  (measured). */
    @Override
    protected SqlWriter valuesSource(SqlWriter writer, SqlSource.Values v) {
        writer.append("VALUES ");
        for (int r = 0; r < v.rows().size(); r++) {
            List<SqlExpr> row = v.rows().get(r);
            writer.append(r > 0 ? ", (" : "(");
            for (int c = 0; c < row.size(); c++) {
                writer.append(c > 0 ? ", " : "");
                // a column of NULLs alone is VOID to Spark: its first row says its declared type
                if (r == 0 && c < v.outputs().size() && allNull(v, c)) {
                    writer.append("CAST(NULL AS ").append(castTypeName(v.outputs().get(c).type())).append(")");
                } else {
                    writer.expr(row.get(c), 0);
                }
            }
            writer.append(")");
        }
        return writer.append(" AS ").append(aliasIdent(v.alias())).append("(")
                .append(String.join(", ", v.columns().stream().map(this::aliasIdent).toList())).append(")");
    }

    private static boolean allNull(SqlSource.Values v, int column) {
        return v.rows().stream().allMatch(row -> row.get(column) instanceof SqlExpr.NullLit);
    }

    /** Native PIVOT over the source; each value's column is named {@code value__|__agg}, as the platform names it:
     *  with one aggregate the value's alias is the whole name (Spark names the column by it alone), with several
     *  Spark joins the value's alias and the aggregate's with {@code _}, so the aggregate's carries the tail. */
    @Override
    protected SqlWriter pivotSource(SqlWriter writer, SqlSource.Pivot p, int depth) {
        // a dynamic pivot a cast names pivots on the named values (SqlSource.Pivot.values, step 9, 2026-10-19)
        List<SqlExpr> values = p.values();
        if (values.isEmpty()) {
            throw new IllegalStateException("a dynamic pivot reached Databricks: needsStaticPivot discovers its values");
        }
        if (p.on().size() != 1) {
            throw new DialectCapability("a pivot on " + p.on().size() + " columns reached Databricks");
        }
        boolean one = p.usings().size() == 1;
        writer.append("(SELECT * FROM ");
        source(writer, p.source(), depth);
        writer.append(" PIVOT (").join(p.usings(), ", ", (w, u) -> reducer(w, u.agg()).append(" AS ")
                .append(ident(one ? u.alias() : "_|__" + u.alias())));
        writer.append(" FOR ");
        SqlExpr on = p.on().get(0);
        if (on instanceof SqlExpr.Column c) {
            writer.append(delimited(c.name()));
        } else {
            writer.expr(on, 0);
        }
        writer.append(" IN (").join(values, ", ", (w, v) -> w.expr(v, 0).append(" AS ")
                .append(ident(one ? pivotValueText(v) + "__|__" + p.usings().get(0).alias() : pivotValueText(v))));
        return writer.append("))) AS ").append(ident(p.alias()));
    }

    /** A pivot value as the platform names its column. */
    private static String pivotValueText(SqlExpr v) {
        return switch (v) {
            case SqlExpr.StringLit s -> s.value();
            case SqlExpr.IntLit i -> String.valueOf(i.value());
            case SqlExpr.BoolLit b -> String.valueOf(b.value());
            default -> throw new DialectCapability("a pivot value " + v.getClass().getSimpleName()
                    + " reached Databricks without a column name");
        };
    }

    // ==================================================================
    // Lexical: identifiers, literals, types
    // ==================================================================

    /** Every identifier in backticks; a double-quoted declaration ({@code "date"}) is its inner name. */
    @Override
    protected String ident(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")
                && !name.substring(1, name.length() - 1).replace("\"\"", "").contains("\"")) {
            return delimited(name.substring(1, name.length() - 1).replace("\"\"", "\""));
        }
        return delimited(name);
    }

    /** A backslash and a quote escaped by a backslash (measured: {@code 'it''s'} is two literals, {@code its}). */
    @Override
    protected String stringLit(String value) {
        if (value.indexOf('\u0000') >= 0) {
            String[] parts = value.split("\u0000", -1);
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) {
                    sb.append(" || chr(0) || ");
                }
                sb.append(quoted(parts[i]));
            }
            return sb.append(')').toString();
        }
        return quoted(value);
    }

    private static String quoted(String value) {
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    @Override
    protected String arrayTypeName(String element) {
        return "ARRAY<" + element + ">";
    }

    @Override
    protected String mapTypeName(String key, String value) {
        return "MAP<" + key + ", " + value + ">";
    }

    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        List<java.util.Map.Entry<String, String>> fields = structFields(st);
        StringBuilder sb = new StringBuilder("STRUCT<");
        for (int i = 0; i < fields.size(); i++) {
            sb.append(i > 0 ? ", " : "").append(fields.get(i).getKey()).append(": ").append(fields.get(i).getValue());
        }
        return sb.append(">").toString();
    }

    /** Alias-less projections label EXPLICITLY from the declared output: Spark labels an expression by its text. */
    @Override
    protected @com.legend.base.Nullable String implicitLabel(SqlSelect.Projection p) {
        return p.out() != null ? aliasIdent(p.out().name()) : super.implicitLabel(p);
    }

    /** A value that can only be NULL (bottom-typed: a NULL literal, {@code coalesce(NULL, NULL)}) is spelled in its
     *  declared slot's type: Spark types it VOID, which no reader of the column can use (an {@code explode} refuses
     *  it, measured). */
    @Override
    protected SqlWriter projection(SqlWriter writer, SqlSelect.Projection p) {
        // a list of unknown element type exploded (the empty collection, compacted) is an array of VOID to Spark: its
        // elements are typed by the column's declared type
        if (p.out() != null && p.expr() instanceof SqlExpr.Call u && u.fn() == SqlFn.UNNEST && u.args().size() == 1
                && listElement(u.args().get(0)) == null) {
            writer.append("explode(CAST(").piece(list(u.args().get(0))).append(" AS ")
                    .append(arrayTypeName(castTypeName(p.out().type()))).append("))");
            String label = p.alias() != null ? aliasIdent(p.alias()) : implicitLabel(p);
            return label == null ? writer : writer.append(" AS ").append(label);
        }
        // (never a generator: Spark refuses explode inside an expression, measured); a raise is VOID too
        if (p.out() != null && (p.expr().type() instanceof com.legend.sql.TypeFact.Bottom || isRaise(p.expr()))
                && !(p.expr() instanceof SqlExpr.Call call && call.fn() == SqlFn.UNNEST)) {
            writer.append("CAST(").expr(p.expr(), 0).append(" AS ").append(castTypeName(p.out().type())).append(")");
            String label = p.alias() != null ? aliasIdent(p.alias()) : implicitLabel(p);
            return label == null ? writer : writer.append(" AS ").append(label);
        }
        return super.projection(writer, p);
    }

    private static boolean isRaise(SqlExpr e) {
        return e instanceof SqlExpr.Call c && c.fn() == SqlFn.ERROR;
    }

    /** A branch that IS the raise is cast to the CASE's own type: raise_error is VOID, which a CASE of nothing else
     *  leaves VOID (as Postgres's raise is cast to its slot). */
    @Override
    protected SqlWriter caseExpr(SqlWriter writer, SqlExpr.Case c) {
        SqlType slot = typeOf(c);
        writer.append("CASE");
        for (SqlExpr.Case.When w : c.whens()) {
            writer.append(" WHEN ").expr(w.condition(), 0).append(" THEN ").piece(branch(w.then(), slot));
        }
        if (c.otherwise() != null) {
            writer.append(" ELSE ").piece(branch(c.otherwise(), slot));
        }
        return writer.append(" END");
    }

    private SqlWriter.Piece branch(SqlExpr e, @com.legend.base.Nullable SqlType slot) {
        if (isRaise(e) && slot != null) {
            String type = castTypeName(slot);
            return w -> w.append("CAST(").expr(e, 0).append(" AS " + type + ")");
        }
        return w -> w.expr(e, 0);
    }

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        return switch (e) {
            // a BIGINT: a bare literal is a 32-bit INT, whose arithmetic overflows at 2^31
            case SqlExpr.IntLit i -> writer.append(i.value() + "L");
            case SqlExpr.OrderedListAgg ola -> writer.piece(collected(w -> w.expr(ola.value(), 0),
                    List.of(new SortKey(w -> w.expr(ola.orderBy(), 0), true, false))));
            case SqlExpr.Quantified q -> quantified(writer, q);
            default -> super.expr(writer, e, parentPrec);
        };
    }

    /** {@code x op ALL|ANY (subquery)}: Spark has no quantified comparison, so each is its SQL truth table over the
     *  subquery's one column -- ALL is FALSE when some row compares false, else NULL when some compares unknown, else
     *  TRUE (the empty subquery included); ANY is TRUE when some row compares true, else NULL when some compares
     *  unknown, else FALSE. */
    private SqlWriter quantified(SqlWriter writer, SqlExpr.Quantified q) {
        String op = switch (q.comparison()) {
            case EQUAL -> " = ";
            case NOT_EQUAL -> " <> ";
            case LESS -> " < ";
            case LESS_EQUAL -> " <= ";
            case GREATER -> " > ";
            case GREATER_EQUAL -> " >= ";
            default -> throw new DialectCapability("a quantified " + q.comparison() + " reached Databricks");
        };
        // the subquery's one column named by a derived-column alias (its outputs may be undeclared)
        String column = "`__q`.`__c`";
        boolean all = q.quantifier() == SqlExpr.Quantifier.ALL;
        SqlWriter.Piece cmp = w -> w.append("(").expr(q.value(), 5).append(op + column + ")");
        writer.append("(CASE WHEN EXISTS (SELECT 1 FROM (");
        inline(writer, q.subquery());
        writer.append(") AS `__q`(`__c`) WHERE ").append(all ? "NOT " : "").piece(cmp).append(") THEN ")
                .append(all ? "FALSE" : "TRUE").append(" WHEN EXISTS (SELECT 1 FROM (");
        inline(writer, q.subquery());
        return writer.append(") AS `__q`(`__c`) WHERE ").piece(cmp).append(" IS NULL) THEN NULL ELSE ")
                .append(all ? "TRUE" : "FALSE").append(" END)");
    }

    // ==================================================================
    // Lists: native arrays with lambdas
    // ==================================================================

    /** The element type of a list, or null for a value that is no list. */
    private static @com.legend.base.Nullable SqlType listElement(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof SqlType.Array a
                ? a.element() : null;
    }

    private static @com.legend.base.Nullable SqlType typeOf(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
    }

    /** A list argument: the empty collection (a bare NULL, untyped) as a NULL array whose VOID elements coerce to
     *  any type (Spark refuses a bare NULL where an array is required, measured). */
    private SqlWriter.Piece list(SqlExpr xs) {
        return xs instanceof SqlExpr.NullLit ? w -> w.append("CAST(NULL AS ARRAY<VOID>)") : w -> w.expr(xs, 0);
    }

    @Override
    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        return writer.append("array(").list(elements).append(")");
    }

    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit st) {
        // a NULL member is VOID to Spark, which a list function over it refuses (`transform` of an instance's absent
        // to-many property, and of a to-many property read off an absent to-one instance, measured 2026-10-19; step 9,
        // the Databricks lane's testAssignNewInstance): a composite member's NULL is written in its declared type
        return writer.append("named_struct(")
                .join(st.fields(), ", ", (w, f) -> {
                    w.append(stringLit(f.name())).append(", ");
                    if (f.value() instanceof SqlExpr.NullLit && (f.declared() instanceof SqlType.Array
                            || f.declared() instanceof SqlType.Struct || f.declared() instanceof SqlType.Map)) {
                        w.append("CAST(NULL AS ").append(castTypeName(f.declared())).append(")");
                    } else {
                        w.expr(f.value(), 0);
                    }
                })
                .append(")");
    }

    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        return writer.append("(").expr(g.source(), 8).append(").").append(ident(g.field()));
    }

    @Override
    protected SqlWriter lambda(SqlWriter writer, SqlExpr.Lambda l) {
        return writer.append(l.params().size() == 1 ? param(l.params().get(0))
                : "(" + String.join(", ", l.params().stream().map(this::param).toList()) + ")")
                .append(" -> ").expr(l.body(), 0);
    }

    /** A lambda parameter as its references spell it. */
    private String param(String name) {
        return columnRef(SqlExpr.Column.derived(null, name));
    }

    /** {@code l} with its index parameter (DuckDB's, 1-based) read from Spark's 0-based one. */
    private static SqlExpr.Lambda oneBasedIndex(SqlExpr.Lambda l) {
        if (l.params().size() < 2) {
            return l;
        }
        String index = l.params().get(1);
        String zero = index + "__0";
        SqlExpr body = substitute(l.body(), index, SqlExpr.Call.of(SqlFn.PLUS, SqlExpr.Column.derived(null, zero),
                new SqlExpr.IntLit(1)));
        return new SqlExpr.Lambda(List.of(l.params().get(0), zero), body);
    }

    private static SqlExpr substitute(SqlExpr e, String name, SqlExpr with) {
        if (e instanceof SqlExpr.Column c && c.table() == null && c.name().equals(name)) {
            return with;
        }
        List<SqlExpr> kids = e.children();
        if (kids.isEmpty()) {
            return e;
        }
        List<SqlExpr> mapped = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (SqlExpr k : kids) {
            SqlExpr m = substitute(k, name, with);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? e.withChildren(mapped) : e;
    }

    /** {@code (SELECT agg FROM explode(xs) AS __v(__e))}: a list reduced as rows (a correlated scalar subquery,
     *  measured); an empty or NULL list reduces over no rows. */
    private SqlWriter.Piece overRows(SqlExpr xs, SqlAgg.Fn fn, List<SqlExpr> extras) {
        SqlType element = listElement(xs);
        SqlExpr e = element == null ? SqlExpr.Column.derived(null, "__e")
                : SqlExpr.Column.param("__e", xs);
        List<SqlExpr> args = new java.util.ArrayList<>();
        args.add(e);
        args.addAll(extras);
        SqlAgg.Reducer r = new SqlAgg.Reducer(fn, args, false, List.of());
        return w -> {
            w.append("(SELECT ");
            reducer(w, r);
            w.append(" FROM explode(").piece(list(xs)).append(") AS `__v`(").append(param("__e")).append("))");
        };
    }

    @Override
    protected SqlWriter listCall(SqlWriter writer, SqlFn fnName, List<SqlExpr> args) {
        SqlExpr xs = args.get(0);
        return switch (fnName) {
            case LIST_FILTER -> writer.append("filter(").piece(list(xs)).append(", ")
                    .expr(oneBasedIndex((SqlExpr.Lambda) args.get(1)), 0).append(")");
            case LIST_TRANSFORM -> writer.append("transform(").piece(list(xs)).append(", ")
                    .expr(oneBasedIndex((SqlExpr.Lambda) args.get(1)), 0).append(")");
            // NULL || xs is xs, as DuckDB's list_concat
            // (array() is an ARRAY<VOID>, which coerces to any array type: measured)
            case LIST_CONCAT -> writer.append("concat(").join(args, ", ", (w, a) -> w.append("coalesce(").piece(list(a))
                    .append(", array())")).append(")");
            // 1-based, negative from the end, out of range NULL (DuckDB's list_extract); 0 is out of range
            case LIST_GET -> writer.append("(CASE WHEN ").expr(args.get(1), 0).append(" = 0 THEN NULL ELSE try_element_at(")
                    .piece(list(xs)).append(", CAST(").expr(args.get(1), 0).append(" AS INT)) END)");
            // DuckDB's list_position: NULL when absent (array_position answers 0)
            case LIST_POSITION -> writer.append("nullif(array_position(").piece(list(xs)).append(", ").expr(args.get(1), 0)
                    .append("), 0)");
            // DuckDB's list_distinct drops NULLs; array_distinct keeps the first occurrence's order
            case LIST_DISTINCT -> writer.append("array_distinct(filter(").piece(list(xs)).append(", ")
                    .append(param("__x")).append(" -> ").append(param("__x")).append(" IS NOT NULL))");
            case LIST_APPEND -> writer.append("array_append(").piece(list(xs)).append(", ").expr(args.get(1), 0).append(")");
            case LIST_SUM -> writer.piece(overRows(xs, SqlAgg.Fn.SUM, List.of()));
            case LIST_MIN -> writer.append("array_min(").piece(list(xs)).append(")");
            case LIST_MAX -> writer.append("array_max(").piece(list(xs)).append(")");
            case LIST_AVG -> writer.append("CAST(").piece(overRows(xs, SqlAgg.Fn.AVG, List.of())).append(" AS DOUBLE)");
            case LIST_MEDIAN -> writer.piece(overRows(xs, SqlAgg.Fn.MEDIAN, List.of()));
            case LIST_MODE -> writer.piece(overRows(xs, SqlAgg.Fn.MODE, List.of()));
            case LIST_BOOL_AND -> writer.piece(overRows(xs, SqlAgg.Fn.BOOL_AND, List.of()));
            case LIST_BOOL_OR -> writer.piece(overRows(xs, SqlAgg.Fn.BOOL_OR, List.of()));
            case LIST_PRODUCT -> writer.append("aggregate(").piece(list(xs)).append(", CAST(1 AS DOUBLE), ")
                    .append("(`__a`, `__x`) -> `__a` * coalesce(`__x`, 1))");
            // array_sort: ascending, NULLs last; sort_array(xs, false): descending, NULLs last (DuckDB's defaults)
            case LIST_SORT -> writer.append("array_sort(").piece(list(xs)).append(")");
            case LIST_SORT_DESC -> writer.append("sort_array(").piece(list(xs)).append(", false)");
            case LIST_REVERSE -> writer.append("reverse(").piece(list(xs)).append(")");
            case LIST_TAIL -> writer.append("filter(").piece(list(xs)).append(", (`__x`, `__i`) -> `__i` > 0)");
            case LIST_INIT -> writer.append("filter(").piece(list(xs)).append(", (`__x`, `__i`) -> `__i` < array_size(")
                    .piece(list(xs)).append(") - 1)");
            // DuckDB's list_slice: 1-based, both ends inclusive
            case LIST_SLICE -> writer.append("filter(").piece(list(xs)).append(", (`__x`, `__i`) -> `__i` + 1 >= ")
                    .expr(args.get(1), 6).append(" AND `__i` + 1 <= ").expr(args.get(2), 6).append(")");
            case LIST_ZIP -> throw new DialectCapability("list_zip reached Databricks: arrays_zip names its fields");
            // DuckDB's list_reduce: the first element is the accumulator's start, the lambda (acc, x) over the rest
            case LIST_REDUCE -> writer.append("aggregate(filter(").piece(list(xs)).append(", (`__x`, `__i`) -> `__i` > 0), ")
                    .append("element_at(").piece(list(xs)).append(", 1), ").expr(args.get(1), 0).append(")");
            // Spark's own name of the value's type, compared only with typeofName's (the judges' grid sides read a
            // declared timestamp's DATE cells by it since the plan path program's step 6, 2026-10-20)
            case TYPEOF -> writer.function("typeof", args);
            case RANGE_FN, REPEAT_VALUE -> throw new IllegalStateException(fnName + " is rendered by call");
            default -> throw new IllegalStateException("not a list call: " + fnName);
        };
    }

    @Override
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("coalesce(exists(").piece(list(args.get(0))).append(", ").expr(args.get(1), 0)
                .append("), FALSE)");
    }

    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("coalesce(forall(").piece(list(args.get(0))).append(", ").expr(args.get(1), 0)
                .append("), TRUE)");
    }

    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("coalesce(array_size(array_distinct(").piece(list(args.get(0))).append(")) = array_size(")
                .piece(list(args.get(0))).append("), TRUE)");
    }

    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        return writer.append("array_contains(").piece(list(m.collection())).append(", ").expr(m.needle(), 0).append(")");
    }

    /** A list reduced: by a higher-order function where one says it exactly (a subquery is refused inside a lambda,
     *  measured), else as rows ({@link #overRows}). NULL elements are skipped, as an aggregate skips them. */
    @Override
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        SqlWriter.Piece xs = list(rc.collection());
        SqlWriter.Piece present = w -> w.append("filter(").piece(xs).append(", `__x` -> `__x` IS NOT NULL)");
        return switch (rc.reducer()) {
            // NULL over no present element, as string_agg over no row
            case STRING_AGG -> rc.extras().size() == 1
                    ? writer.append("(CASE WHEN array_size(").piece(present).append(") = 0 THEN NULL ELSE array_join(")
                            .append("transform(").piece(present).append(", `__x` -> CAST(`__x` AS STRING)), ")
                            .expr(rc.extras().get(0), 0).append(") END)")
                    : writer.piece(overRows(rc.collection(), rc.reducer(), rc.extras()));
            case MIN -> writer.append("array_min(").piece(xs).append(")");
            case MAX -> writer.append("array_max(").piece(xs).append(")");
            case COUNT -> writer.append("array_size(").piece(present).append(")");
            case STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP, VARIANCE, STDDEV ->
                    writer.piece(moments(present, rc.reducer()));
            default -> writer.piece(overRows(rc.collection(), rc.reducer(), rc.extras()));
        };
    }

    /**
     * A list's variance or standard deviation as DuckDB computes it, so the same doubles: its own update, in its order
     * of operations (count, then {@code mean + (x - mean) / count}, then {@code M2 + (x - new mean) * (x - mean)}), an
     * aggregate over the present elements whose finish is the result. Spark's own moments are another update, equal
     * in algebra but not in rounding (3.14's sample standard deviation came back 3.1399999999999997, measured). A
     * sample's needs two elements (NULL otherwise); a population's one, 0 alone; a NULL list is NULL.
     */
    private static SqlWriter.Piece moments(SqlWriter.Piece present, SqlAgg.Fn fn) {
        String x = "CAST(`__x` AS DOUBLE)";
        String count = "(`__a`.`n` + 1)";
        String mean = "(`__a`.`m` + (" + x + " - `__a`.`m`) / " + count + ")";
        String step = "named_struct('n', " + count + ", 'm', " + mean + ", 'q', `__a`.`q` + (" + x + " - " + mean
                + ") * (" + x + " - `__a`.`m`))";
        boolean sample = fn == SqlAgg.Fn.STDDEV_SAMP || fn == SqlAgg.Fn.VAR_SAMP || fn == SqlAgg.Fn.VARIANCE
                || fn == SqlAgg.Fn.STDDEV;
        boolean root = fn == SqlAgg.Fn.STDDEV_SAMP || fn == SqlAgg.Fn.STDDEV_POP || fn == SqlAgg.Fn.STDDEV;
        String variance = sample
                ? "CASE WHEN `__a`.`n` > 1 THEN `__a`.`q` / (`__a`.`n` - 1) END"
                : "CASE WHEN `__a`.`n` > 1 THEN `__a`.`q` / `__a`.`n` WHEN `__a`.`n` = 1 THEN CAST(0 AS DOUBLE) END";
        String finish = root ? "sqrt(" + variance + ")" : variance;
        return w -> w.append("aggregate(").piece(present).append(", named_struct('n', CAST(0 AS DOUBLE), 'm', CAST(0"
                + " AS DOUBLE), 'q', CAST(0 AS DOUBLE)), (`__a`, `__x`) -> " + step + ", `__a` -> " + finish + ")");
    }

    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        if (args.size() != 1) {
            throw new DialectCapability("an unnest of " + args.size() + " lists reached Databricks");
        }
        return writer.append("explode(").piece(list(args.get(0))).append(")");
    }

    /** Pure's fold, {@code (element, acc)}, as Spark's {@code aggregate(xs, init, (acc, x) -> ...)}: its accumulator
     *  may be any type, a list included; an empty or NULL list folds to the init. */
    @Override
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        if (!(f.type() instanceof com.legend.sql.TypeFact.Typed t)) {
            throw new DialectCapability("a fold of unknown type reached Databricks");
        }
        String type = castTypeName(t.type());
        String elem = param(f.lambda().params().get(0));
        String acc = param(f.lambda().params().get(1));
        writer.append("aggregate(coalesce(").piece(list(f.source())).append(", array()), CAST(").expr(f.init(), 0).append(" AS " + type + "), (" + acc + ", " + elem + ") -> CAST(")
                .expr(f.lambda().body(), 0).append(" AS " + type + "))");
        return writer;
    }

    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        // Pure's splitPart: the non-empty tokens, 1-based, NULL past the end
        return writer.append("try_element_at(filter(").piece(split(a.get(0), a.get(1)))
                .append(", `__t` -> `__t` <> ''), CAST(").expr(a.get(2), 0).append(" AS INT))");
    }

    /**
     * Jaro-Winkler similarity, exactly DuckDB's (rapidfuzz's; the algorithm is spelled out on Snowflake's): Databricks
     * has no Jaro-Winkler function. Spark's lambdas read their enclosing lambdas' parameters, so the arguments are
     * bound once, by a one-element transform, and read where they are needed: an aggregate over {@code t}'s positions
     * matches each character in its window ({@code f} flags {@code p}'s matched characters, {@code tm} is {@code t}'s
     * matched characters in order), its finish an aggregate along {@code p} counting the transpositions ({@code n}
     * matched so far, {@code x} unequal to {@code tm}'s character in the same place), whose finish is the score, in
     * DOUBLE in DuckDB's order of operations (measured: 1,585 pairs, every one equal to DuckDB's).
     */
    private static SqlWriter.Piece jaroWinkler(SqlExpr p, SqlExpr t) {
        String o = "`__jo`";
        String s = "`__js`";
        String u = "`__ju`";
        String v = "`__jv`";
        String j = "`__jj`";
        String i = "`__ji`";
        String lp = "length(" + o + ".p)";
        String lt = "length(" + o + ".t)";
        String window = "greatest(0, greatest(" + lp + ", " + lt + ") div 2 - 1)";
        String m = "CAST(" + v + ".n AS DOUBLE)";
        String jaro = "((" + m + " / CAST(" + lp + " AS DOUBLE) + " + m + " / CAST(" + lt + " AS DOUBLE) + (" + m
                + " - CAST(" + v + ".x div 2 AS DOUBLE)) / " + m + ") / CAST(3 AS DOUBLE))";
        List<String> prefix = new java.util.ArrayList<>();
        for (int k = 1; k <= 4; k++) {
            prefix.add("CASE WHEN " + k + " <= least(" + lp + ", " + lt + ") AND left(" + o + ".p, " + k + ") = left("
                    + o + ".t, " + k + ") THEN 1 ELSE 0 END");
        }
        String score = "CASE WHEN " + v + ".n = 0 THEN CAST(0 AS DOUBLE) WHEN " + jaro + " > CAST(0.7 AS DOUBLE) THEN "
                + jaro + " + CAST(" + String.join(" + ", prefix) + " AS DOUBLE) * CAST(0.1 AS DOUBLE) * (CAST(1 AS"
                + " DOUBLE) - " + jaro + ") ELSE " + jaro + " END";
        String take = "aggregate(sequence(greatest(1, " + j + " - " + window + "), " + j + " + " + window
                + "), named_struct('f', " + s + ".f, 'tm', " + s + ".tm, 'k', false), (" + u + ", " + i
                + ") -> CASE WHEN NOT " + u + ".k AND substr(" + u + ".f, " + i + ", 1) = '0' AND substr(" + o + ".p, "
                + i + ", 1) = substr(" + o + ".t, " + j + ", 1) THEN named_struct('f', overlay(" + u + ".f placing '1'"
                + " from " + i + " for 1), 'tm', concat(" + u + ".tm, substr(" + o + ".t, " + j + ", 1)), 'k', true)"
                + " ELSE " + u + " END, " + u + " -> named_struct('f', " + u + ".f, 'tm', " + u + ".tm))";
        String counted = "aggregate(sequence(1, " + lp + "), named_struct('n', 0, 'x', 0), (" + v + ", " + i
                + ") -> CASE WHEN substr(" + s + ".f, " + i + ", 1) = '1' THEN named_struct('n', " + v + ".n + 1, 'x', "
                + v + ".x + CASE WHEN substr(" + o + ".p, " + i + ", 1) = substr(" + s + ".tm, " + v + ".n + 1, 1)"
                + " THEN 0 ELSE 1 END) ELSE " + v + " END, " + v + " -> " + score + ")";
        String body = "CASE WHEN " + o + ".p IS NULL OR " + o + ".t IS NULL THEN CAST(NULL AS DOUBLE) WHEN " + lp
                + " = 0 OR " + lt + " = 0 THEN CAST(0 AS DOUBLE) ELSE aggregate(sequence(1, " + lt
                + "), named_struct('f', repeat('0', " + lp + "), 'tm', ''), (" + s + ", " + j + ") -> " + take + ", "
                + s + " -> " + counted + ") END";
        return w -> w.append("element_at(transform(array(named_struct('p', ").expr(p, 0).append(", 't', ")
                .expr(t, 0).append(")), " + o + " -> " + body + "), 1)");
    }

    /** A session's read-only table is a TEMPORARY VIEW over its rows: a temporary TABLE is not read inside an EXISTS
     *  (UC_COMMAND_NOT_SUPPORTED, "Data source v2", measured; a scalar or IN subquery reads it), a view is, and it is
     *  one statement (0.2 s, where a table and its INSERT took 3). Each column is cast to its declared type in the
     *  view's projection, so a column whose cells are all NULL, or a view with no rows, is still typed (measured). */
    @Override
    public List<String> sessionTable(com.legend.sql.SqlDdl.CreateTable table,
            com.legend.sql.SqlDml.@com.legend.base.Nullable InsertValues rows) {
        if (!table.temporary()) {
            throw new IllegalArgumentException("a session table is temporary: " + table.table());
        }
        List<com.legend.sql.SqlDdl.Column> cols = table.columns();
        SqlWriter writer = newWriter().append("CREATE TEMPORARY VIEW ")
                .append(storedName(table.table(), table.quoting().table()))
                .append(" AS SELECT ");
        for (int c = 0; c < cols.size(); c++) {
            com.legend.sql.SqlDdl.Column col = cols.get(c);
            writer.append(c > 0 ? ", " : "").append("CAST(").append(rows == null ? "NULL" : "`c" + c + "`")
                    .append(" AS " + ddlType(col.type()) + ") AS ")
                    .append(ddlIdentifier(col.name(), col.declaredQuoted()));
        }
        if (rows == null) {
            writer.append(" WHERE FALSE");
        } else {
            writer.append(" FROM VALUES ");
            for (int r = 0; r < rows.rows().size(); r++) {
                writer.append(r == 0 ? "(" : ", (").list(rows.rows().get(r)).append(")");
            }
            writer.append(" AS `t`(");
            for (int c = 0; c < cols.size(); c++) {
                writer.append(c > 0 ? ", `c" : "`c").append(c + "`");
            }
            writer.append(")");
        }
        RenderedStatement view = writer.statement();
        if (!view.binds().isEmpty()) {
            throw new DialectCapability("plan parameters " + view.binds() + " in a session table's rows: a row holds"
                    + " values, and a DDL statement is text");
        }
        return List.of(view.sql());
    }

    /** A pure collection holds no empties: {@code array_compact} drops the NULL elements -- not {@code filter}'s lambda,
     *  which Spark refuses around a subquery (no higher-order function's argument may hold one, measured: the mixed
     *  sort's ordered subquery under the PCT root's compaction). The same rows: a NULL list stays NULL, an empty one
     *  empty, a nested list's own NULLs kept (measured). */
    @Override
    protected SqlWriter compactList(SqlWriter writer, SqlExpr.CompactList cl, int parentPrec) {
        return writer.append("array_compact(").piece(list(cl.list())).append(")");
    }

    /** Spark DDL: a nullable column says nothing (an explicit NULL is a syntax error, measured). */
    @Override
    protected String ddlNullability(boolean notNull) {
        return notNull ? " NOT NULL" : "";
    }

    /** Spark DDL: STRING for an unbounded VARCHAR (a VARCHAR needs a length: DATATYPE_MISSING_SIZE, measured),
     *  BOOLEAN for BIT (Spark has no BIT). */
    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT -> "BIGINT";
                case SMALLINT -> "SMALLINT";
                case TINYINT -> "TINYINT";
                case INTEGER -> "INTEGER";
                case FLOAT -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                case REAL -> "REAL";
                case BIT -> "BOOLEAN";
                case TIMESTAMP -> "TIMESTAMP";
                case DATE -> "DATE";
                case JSON -> "JSON";
                case VARCHAR -> "STRING";
                // a time of day: Databricks has no TIME type (the TIME scalar's own wall); a nanosecond timestamp: its
                // TIMESTAMP keeps microseconds
                case TIME, TIMESTAMP_NS, OTHER, DISTINCT, ARRAY, OBJECT -> throw DdlSpelling.unspelled(p.kind());
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> DdlSpelling.sized(z);
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> DdlSpelling.scaled(s);
        };
    }

    /** {@code s} split on the LITERAL text {@code d}: Spark's split takes a regex, so the delimiter is quoted. */
    private SqlWriter.Piece split(SqlExpr s, SqlExpr d) {
        return w -> {
            w.append("split(").expr(s, 0).append(", ");
            if (d instanceof SqlExpr.StringLit lit) {
                w.append(stringLit(java.util.regex.Pattern.quote(lit.value())));
            } else {
                w.append("concat('\\\\Q', replace(").expr(d, 0).append(", '\\\\E', '\\\\E\\\\\\\\E\\\\Q'), '\\\\E')");
            }
            w.append(", -1)");
        };
    }

    @Override
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> args) {
        SqlType st = typeOf(args.get(0));
        if (!(st instanceof SqlType.Struct struct) || !(args.get(1) instanceof SqlExpr.StringLit name)) {
            throw new DialectCapability("struct_insert over an untyped struct reached Databricks");
        }
        writer.append("named_struct(");
        boolean first = true;
        for (SqlType.Struct.Field f : struct.fields()) {
            if (f.name().equals(name.value())) {
                continue;
            }
            writer.append(first ? "" : ", ").append(stringLit(f.name())).append(", (").expr(args.get(0), 8)
                    .append(").").append(ident(f.name()));
            first = false;
        }
        return writer.append(first ? "" : ", ").append(stringLit(name.value())).append(", ").expr(args.get(2), 0)
                .append(")");
    }

    // ==================================================================
    // Variant (VARIANT, Databricks' own semi-structured type)
    // ==================================================================

    private static boolean isJson(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.JSON;
    }

    private static boolean isText(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.VARCHAR;
    }

    /** A value as a VARIANT: text parsed? No -- a STRING is a variant string (DuckDB's to_json); a struct, map or
     *  list converts recursively ({@code CAST(struct AS VARIANT)} is refused, measured); a scalar casts. */
    private SqlWriter.Piece toVariant(SqlExpr x) {
        SqlType t = typeOf(x);
        if (t == SqlType.Scalar.JSON) {
            return w -> w.expr(x, 0);
        }
        if (t instanceof SqlType.Struct || t instanceof SqlType.Map || t instanceof SqlType.Array) {
            return w -> w.append("to_variant_object(").expr(x, 0).append(")");
        }
        return w -> w.append("CAST(").expr(x, 0).append(" AS VARIANT)");
    }

    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() != 1) {
            throw new DialectCapability("toVariant of " + a.size() + " arguments reached Databricks");
        }
        return writer.piece(toVariant(a.get(0)));
    }

    /** A key (a member, or NULL) or a 0-based index (-1 the last, past the end NULL) of a variant: through a cast to
     *  a map or an array of variants (a variant takes no subscript, measured), NULL when it is not one. */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        SqlExpr key = args.get(1);
        if (key instanceof SqlExpr.StringLit root && "$".equals(root.value())) {
            return writer.expr(args.get(0), 0);
        }
        if (isText(key) || key instanceof SqlExpr.StringLit) {
            return writer.append("try_element_at(try_cast(").expr(args.get(0), 0).append(" AS MAP<STRING, VARIANT>), ")
                    .expr(key, 0).append(")");
        }
        return writer.append("try_element_at(try_cast(").expr(args.get(0), 0).append(" AS ARRAY<VARIANT>), CAST(CASE WHEN ")
                .expr(key, 0).append(" >= 0 THEN ").expr(key, 6).append(" + 1 ELSE ").expr(key, 0).append(" END AS INT))");
    }

    @Override
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        if (listElement(args.get(0)) != null) {
            return writer.expr(args.get(0), 0);
        }
        return writer.append("try_cast(").expr(args.get(0), 0).append(" AS ARRAY<VARIANT>)");
    }

    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        if (c.target() == SqlType.Scalar.TEMPORAL_TEXT || c.target() == SqlType.Scalar.DECIMAL_TEXT) {
            return super.variantAwareCast(writer, c);
        }
        if (c.target() == SqlType.Scalar.JSON) {
            return isText(c.value()) ? writer.append("parse_json(").expr(c.value(), 0).append(")")
                    : writer.piece(toVariant(c.value()));
        }
        // a Float's text: Spark spells the exponent E (1.3421E-8), DuckDB e, which the lowering reads
        if (c.target() == SqlType.Scalar.VARCHAR && (typeOf(c.value()) == SqlType.Scalar.DOUBLE
                || c.value() instanceof SqlExpr.FloatLit)) {
            return writer.append("lower(CAST(").expr(c.value(), 0).append(" AS STRING))");
        }
        if (isJson(c.value())) {
            // a member's value as text is its text (a variant string casts unquoted); the whole value as text is
            // its JSON, as DuckDB's cast
            boolean member = c.value() instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET;
            if (c.target() == SqlType.Scalar.VARCHAR && !member) {
                return writer.append("to_json(").expr(c.value(), 0).append(")");
            }
            if (c.target() instanceof SqlType.Array || c.target() instanceof SqlType.Map) {
                return writer.append("try_cast(").expr(c.value(), 0).append(" AS ").append(castTypeName(c.target()))
                        .append(")");
            }
        }
        return super.variantAwareCast(writer, c);
    }

    // ==================================================================
    // Scalar functions -- every SqlFn decided here
    // ==================================================================

    /** The list functions whose answer over the empty collection (a NULL list) is NULL, as DuckDB's are. */
    private static final java.util.Set<SqlFn> NULL_OVER_EMPTY = java.util.Set.of(SqlFn.LIST_FILTER,
            SqlFn.LIST_TRANSFORM, SqlFn.LIST_GET, SqlFn.LIST_POSITION, SqlFn.LIST_DISTINCT, SqlFn.LIST_SUM, SqlFn.LIST_MIN,
            SqlFn.LIST_MAX, SqlFn.LIST_AVG, SqlFn.LIST_MEDIAN, SqlFn.LIST_MODE, SqlFn.LIST_SORT, SqlFn.LIST_SORT_DESC,
            SqlFn.LIST_REVERSE, SqlFn.LIST_TAIL, SqlFn.LIST_INIT, SqlFn.LIST_SLICE, SqlFn.LIST_PRODUCT, SqlFn.LIST_LENGTH,
            SqlFn.LIST_FLATTEN);

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        // over the empty collection the answer is NULL: spelled in its declared type, never Spark's VOID
        if (NULL_OVER_EMPTY.contains(c.fn()) && !a.isEmpty() && a.get(0) instanceof SqlExpr.NullLit
                && c.type() instanceof com.legend.sql.TypeFact.Typed t) {
            return writer.append("CAST(NULL AS ").append(castTypeName(t.type())).append(")");
        }
        return switch (c.fn()) {
            // ---- each function as this dialect spells it, by name (the dialect program's Phase 1, D1, 2026-10-13:
            // an exhaustive switch; a spelling several databases share is a named helper of the base)
            // the ANSI operators
            case AND, OR, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES ->
                    infix(writer, c, parentPrec);
            // the shared spellings (the base's named helpers)
            case NOT -> notPrefix(writer, c.args(), parentPrec);
            case NEGATE -> negation(writer, c.args());
            case IS_NULL -> isNullTest(writer, c.args());
            case IS_NOT_NULL -> isNotNullTest(writer, c.args());
            case IN -> inList(writer, c.args(), parentPrec);
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> distinctFrom(writer, c.args());
            case NULL_SAFE_EQUAL -> notDistinctFrom(writer, c.args());
            case XOR -> xorAsAndOr(writer, c.args(), parentPrec);
            // 64-bit parse (PCT Long.MIN/MAX round-trips)
            case PARSE_INT -> castAs(writer, c.args().get(0), "BIGINT");
            // parseDate(text): the ISO text as a timestamp
            case PARSE_DATE -> castAs(writer, c.args().get(0), "TIMESTAMP");
            case PI -> writer.append("pi()");
            case CEILING -> bigintOf(writer, "ceil", c.args());
            case FLOOR -> bigintOf(writer, "floor", c.args());
            case SIGN -> bigintOf(writer, "sign", c.args());
            case LPAD -> padWithSpace(writer, "lpad", c.args());
            case RPAD -> padWithSpace(writer, "rpad", c.args());
            case UC_FIRST -> firstCharCased(writer, "upper", c.args(), parentPrec);
            case LC_FIRST -> firstCharCased(writer, "lower", c.args(), parentPrec);
            case TODAY -> writer.append("current_date");
            case DATE_TRUNC_DAY -> castAs(writer, c.args().get(0), "DATE");
            // Pure's divide is a Float division: both operands DOUBLE
            case DIVIDE -> divideAsDoubles(writer, c.args());
            case MOD -> modFloored(writer, c.args());
            case REM -> modTruncated(writer, c.args());
            // Pure's divide-with-scale is HALF_UP: plain ROUND (half away from zero)
            case ROUND_HALF_UP -> writer.function("ROUND", c.args());
            case ERROR -> raiseEnvelope(writer, c.args(), "raise_error");
            case DATE_TRUNC -> dateTruncDateGrained(writer, c.args(), stringLit(truncUnit(part(c.args().get(0)))));
            case FLOAT_CANON -> floatCanonByText(writer, c.args(), parentPrec);
            case FLOAT_PRINT, FLOAT_PRINT_ENGINE -> floatPrintByText(writer, c, parentPrec);
            // the hooks: this dialect's own (or the base's refusal)
            case ROUND -> roundHalfEven(writer, c.args());
            case HASH -> hashSigned(writer, c.args());
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), c.args());
            case UNNEST -> unnestProjection(writer, c.args());
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION, LIST_ZIP, LIST_DISTINCT,
                 LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, LIST_BOOL_AND,
                 LIST_BOOL_OR, TYPEOF -> listCall(writer, c.fn(), c.args());
            case STRUCT_INSERT -> structInsert(writer, c.args());
            case PURE_SPLIT_PART -> splitPartCall(writer, c.args());
            case LIST_EXISTS -> listExists(writer, c.args());
            case ALL_DISTINCT -> allDistinct(writer, c.args());
            case LIST_FOR_ALL -> listForAll(writer, c.args());
            case TO_VARIANT -> variantConstruct(writer, c.args());
            case VARIANT_ELEMENTS -> variantElements(writer, c.args());
            case VARIANT_GET -> variantGet(writer, c.args());
            // this database's plain spellings: name(args)
            case ABS -> writer.function("abs", c.args());
            case ACOS -> writer.function("acos", c.args());
            case ASCII_CODE -> writer.function("ascii", c.args());
            case ASIN -> writer.function("asin", c.args());
            case ATAN -> writer.function("atan", c.args());
            case ATAN2 -> writer.function("atan2", c.args());
            case CBRT -> writer.function("cbrt", c.args());
            case CHR -> writer.function("chr", c.args());
            case COALESCE -> writer.function("coalesce", c.args());
            case COS -> writer.function("cos", c.args());
            case COSH -> writer.function("cosh", c.args());
            case COT -> writer.function("cot", c.args());
            case DEGREES -> writer.function("degrees", c.args());
            case EXP -> writer.function("exp", c.args());
            case GREATEST -> writer.function("greatest", c.args());
            case LEAST -> writer.function("least", c.args());
            case LEFT -> writer.function("left", c.args());
            case LENGTH -> writer.function("length", c.args());
            case LEVENSHTEIN -> writer.function("levenshtein", c.args());
            case LIST_FLATTEN -> writer.function("flatten", c.args());
            case LN -> writer.function("ln", c.args());
            case LOG10 -> writer.function("log10", c.args());
            case LOWER -> writer.function("lower", c.args());
            case MAKE_DATE -> writer.function("make_date", c.args());
            case MAP_KEYS -> writer.function("map_keys", c.args());
            case MAP_VALUES -> writer.function("map_values", c.args());
            case MD5 -> writer.function("md5", c.args());
            case POW -> writer.function("power", c.args());
            case RADIANS -> writer.function("radians", c.args());
            case REPEAT_STR -> writer.function("repeat", c.args());
            case REPLACE -> writer.function("replace", c.args());
            case TRANSLATE -> writer.function("translate", c.args());
            case REVERSE_STRING, LIST_REVERSE -> writer.function("reverse", c.args());
            case RIGHT -> writer.function("right", c.args());
            case SHA1 -> writer.function("sha1", c.args());
            case SIN -> writer.function("sin", c.args());
            case SINH -> writer.function("sinh", c.args());
            case SPLIT_PART -> writer.function("split_part", c.args());
            case SQRT -> writer.function("sqrt", c.args());
            case SUBSTRING -> writer.function("substr", c.args());
            case TAN -> writer.function("tan", c.args());
            case TANH -> writer.function("tanh", c.args());
            case UPPER -> writer.function("upper", c.args());
            case STARTS_WITH -> writer.function("startswith", c.args());
            case ENDS_WITH -> writer.function("endswith", c.args());
            case STRPOS -> writer.function("instr", c.args());
            case MAP_FROM_LISTS -> writer.function("map_from_arrays", c.args());
            case MAP_FROM_ENTRIES -> writer.function("map_from_entries", c.args());

            // NULL-ignoring, as DuckDB's concat (Spark's concat is NULL if any argument is)
            case CONCAT -> writer.append("concat_ws('', ").join(flattenConcat(a), ", ", this::asText).append(")");
            case CONCAT_JOIN -> writer.append("concat_ws('', ").join(a, ", ", this::asText).append(")");
            case INT_DIVIDE -> writer.append("(").expr(a.get(0), 6).append(" div ").expr(a.get(1), 6).append(")");
            // the base spells VARCHAR, which needs a length here
            case BOOL_TO_TEXT -> writer.append("CAST(").expr(a.get(0), 0).append(" AS STRING)");
            case BIT_NOT -> writer.append("(~ CAST(").expr(a.get(0), 0).append(" AS BIGINT))");
            // floor without the BIGINT conversion (Spark's floor of a double is a BIGINT: past 2^63 it overflows)
            case FLOOR_RAW -> writer.append("(CASE WHEN abs(").expr(a.get(0), 0).append(") >= 4503599627370496 THEN ")
                    .expr(a.get(0), 0).append(" ELSE CAST(floor(").expr(a.get(0), 0).append(") AS DOUBLE) END)");

            // ---- strings
            case MATCHES -> writer.append("regexp_like(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
            case REGEXP_FULL_MATCH -> writer.append("regexp_like(").expr(a.get(0), 0).append(", ")
                    .piece(anchored(a.get(1))).append(")");
            // group 0 (the whole match) unless one is named: Spark's default is group 1
            case REGEXP_EXTRACT -> writer.append("regexp_extract(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                    .append(", ").piece(group(a)).append(")");
            case REGEXP_EXTRACT_ALL -> writer.append("regexp_extract_all(").expr(a.get(0), 0).append(", ")
                    .expr(a.get(1), 0).append(", ").piece(group(a)).append(")");
            // DuckDB replaces the first match unless 'g'; Spark replaces every one: the first alone is the match of
            // ^(.*?)(?:p) -- the shortest prefix before p's leftmost match -- replaced by that prefix and r, r's group
            // references shifted by one
            case REGEXP_REPLACE -> regexpReplace(writer, c);
            case SPLIT -> writer.piece(split(a.get(0), a.get(1)));
            // Spark's two-argument trims take the characters FIRST (measured: rtrim('a00', '0') is ''): the
            // standard form says which is which
            case TRIM, LTRIM, RTRIM -> trim(writer, c);
            // base64 without line breaks (Spark may chunk at 76 characters)
            case ENCODE_BASE64 -> writer.append("replace(replace(base64(CAST(").expr(a.get(0), 0)
                    .append(" AS BINARY)), chr(13), ''), chr(10), '')");
            case DECODE_BASE64 -> writer.append("CAST(unbase64(").expr(a.get(0), 0).append(") AS STRING)");
            case SHA256 -> writer.append("sha2(").expr(a.get(0), 0).append(", 256)");
            case GUID -> writer.append("uuid()");
            case RANDOM -> writer.append("rand()");
            // format_string is Java's formatter over Spark's values, and its %f takes no Spark Decimal: a Float
            // literal, written as its decimal digits (NUMERIC CHARTER Rule 1), is handed over as the double it is
            // (channel B's testFormatFloatWithRounding and its siblings, 2026-10-20)
            case FORMAT -> writer.append("format_string(").join(a, ", ", (w, x) -> {
                if (x instanceof SqlExpr.FloatLit) {
                    w.append("CAST(").expr(x, 0).append(" AS DOUBLE)");
                } else {
                    w.expr(x, 0);
                }
            }).append(")");
            case JARO_WINKLER -> writer.piece(jaroWinkler(a.get(0), a.get(1)));
            case CURRENT_USER_FN -> writer.append("current_user()");

            // ---- temporal
            case NOW -> writer.append("current_timestamp()");
            case EXTRACT -> writer.piece(extract(a));
            case MAKE_TIMESTAMP -> makeTimestamp(writer, c);
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> writer.append("timestampadd(")
                    .append(intervalUnit(part(a.get(0)))).append(", ").expr(a.get(1), 0)
                    .append(", ").expr(a.get(2), 0).append(")");
            case DATE_DIFF -> writer.piece(dateDiff(a));
            case TIME_BUCKET -> writer.piece(timeBucket(a));
            case DATE_AT_TIME -> throw new DialectCapability("a time of day: Databricks has no TIME type");
            // the zone's wall time of the instant t (the platform's timestamps are UTC instants): measured by the
            // PCT's format with a zone (13:07:44 UTC is 08:07:44 in EST)
            case TIMEZONE -> writer.append("from_utc_timestamp(").expr(a.get(1), 0).append(", ").expr(a.get(0), 0)
                    .append(")");
            // DuckDB's epoch(ts) is DOUBLE seconds; epoch_ms truncates toward zero
            case EPOCH_SECONDS -> writer.append("(CAST(unix_micros(").expr(a.get(0), 0).append(") AS DOUBLE) / 1000000)");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability("a nanosecond epoch: Databricks keeps"
                    + " microseconds (the Cypher island's nanosecond leg is DuckDB's)");
            case EPOCH_MS -> writer.append("(unix_micros(").expr(a.get(0), 0).append(") div 1000)");
            case FROM_EPOCH_SECONDS -> writer.append("timestamp_seconds(").expr(a.get(0), 0).append(")");
            case FROM_EPOCH_MS -> writer.append("timestamp_millis(CAST(").expr(a.get(0), 0).append(" AS BIGINT))");
            case DAYNAME -> writer.append("date_format(").expr(a.get(0), 0).append(", 'EEEE')");
            case MONTHNAME -> writer.append("date_format(").expr(a.get(0), 0).append(", 'MMMM')");
            case STRFTIME -> strftime(writer, c);
            case STRPTIME -> strptime(writer, c);

            // ---- lists whose first argument is no list
            case LIST_LENGTH -> writer.append("array_size(").piece(list(a.get(0))).append(")");
            // DuckDB's range: [start, stop) by step (start 0, step 1 by default); Spark's sequence is inclusive and
            // refuses a step away from the stop
            case RANGE_FN -> rangeFn(writer, c);
            case REPEAT_VALUE -> writer.append("array_repeat(").expr(a.get(0), 0).append(", CAST(").expr(a.get(1), 0)
                    .append(" AS INT))");

            // ---- maps
            case MAP_EMPTY -> writer.append("map()");
            // DuckDB's map_extract: a list of the key's value, empty when absent
            case MAP_EXTRACT -> writer.append("(CASE WHEN map_contains_key(").expr(a.get(0), 0).append(", ")
                    .expr(a.get(1), 0).append(") THEN array(element_at(").expr(a.get(0), 0).append(", ")
                    .expr(a.get(1), 0).append(")) ELSE array() END)");
            // DuckDB's map_concat keeps a duplicate key's LAST value; Spark's refuses the duplicate (its dedup policy is
            // not a warehouse setting): each map less the keys a later one sets, then concatenated
            case MAP_CONCAT -> mapConcat(writer, c);

            // ---- variant
            // in DuckDB's json_type vocabulary, which the lowering compares against
            case JSON_TYPE -> jsonType(writer, c);
            case JSON_ARRAY_LENGTH -> writer.append("array_size(try_cast(").expr(a.get(0), 0)
                    .append(" AS ARRAY<VARIANT>))");
            case JSON_PRETTY -> throw wall(c.fn(), "Databricks has no JSON pretty-printer");
            case JSON_MERGE_PATCH -> throw wall(c.fn(), "Databricks has no JSON merge patch");
        };
    }

    /** The {@code REGEXP_REPLACE} arm of {@link #call}. */
    private SqlWriter regexpReplace(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        boolean global = a.size() > 3 && a.get(3) instanceof SqlExpr.StringLit o && o.value().contains("g");
        if (!(a.get(2) instanceof SqlExpr.StringLit r)) {
            throw wall(c.fn(), "a computed regexp replacement");
        }
        if (global) {
            return writer.append("regexp_replace(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                    .append(", ").append(stringLit(javaReplacement(r.value(), 0))).append(")");
        }
        if (a.get(1) instanceof SqlExpr.StringLit p && p.value().matches("(?s).*\\\\[0-9].*")) {
            throw wall(c.fn(), "a first-match replacement whose pattern has a back reference");
        }
        writer.append("regexp_replace(").expr(a.get(0), 0).append(", ");
        if (a.get(1) instanceof SqlExpr.StringLit p) {
            writer.append(stringLit("^(?s:(.*?))(?:" + p.value() + ")"));
        } else {
            writer.append("concat('^(?s:(.*?))(?:', ").expr(a.get(1), 0).append(", ')')");
        }
        return writer.append(", ").append(stringLit("$1" + javaReplacement(r.value(), 1))).append(")");
    }

    /** The {@code TRIM}, {@code LTRIM}, {@code RTRIM} arm of {@link #call}. */
    private SqlWriter trim(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String fn = c.fn() == SqlFn.TRIM ? "trim" : c.fn() == SqlFn.LTRIM ? "ltrim" : "rtrim";
        if (a.size() == 1) {
            return writer.append(fn + "(").expr(a.get(0), 0).append(")");
        }
        String side = c.fn() == SqlFn.TRIM ? "BOTH" : c.fn() == SqlFn.LTRIM ? "LEADING" : "TRAILING";
        return writer.append("TRIM(" + side + " ").expr(a.get(1), 0).append(" FROM ").expr(a.get(0), 0).append(")");
    }

    /** The {@code MAKE_TIMESTAMP} arm of {@link #call}. */
    private SqlWriter makeTimestamp(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() != 6) {
            throw wall(c.fn(), "make_timestamp of " + a.size() + " arguments");
        }
        return writer.append("make_timestamp(").list(a.subList(0, 5)).append(", CAST(").expr(a.get(5), 0)
                .append(" AS DECIMAL(16,6)))");
    }

    /** The {@code STRFTIME} arm of {@link #call}. */
    private SqlWriter strftime(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        List<com.legend.sql.DateFmt> parts = ((SqlExpr.FormatLit) a.get(1)).parts();
        if (!parts.contains(com.legend.sql.DateFmt.Part.YEAR4)) {
            return writer.append("date_format(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
        }
        // 'yyyy' pads to four digits and signs a year past 9999 ('+25489', measured 2026-10-19; step 9, the
        // Databricks lane's testAdjustByMinutesBigNumber), where DuckDB's %Y and Pure write 25489: the year is
        // formatted alone with that sign removed, each other run of the format as it is, in order (concat: NULL
        // when the date-time is NULL, as date_format is). A BC year keeps its '-' (measured: -21457).
        writer.append("concat(");
        List<com.legend.sql.DateFmt> run = new java.util.ArrayList<>();
        boolean first = true;
        for (com.legend.sql.DateFmt p : parts) {
            if (p != com.legend.sql.DateFmt.Part.YEAR4) {
                run.add(p);
                continue;
            }
            if (!run.isEmpty()) {
                formatRun(writer.append(first ? "" : ", "), a.get(0), run);
                run = new java.util.ArrayList<>();
                first = false;
            }
            writer.append(first ? "" : ", ").append("regexp_replace(");
            formatRun(writer, a.get(0), List.of(p)).append(", '^[+]', '')");
            first = false;
        }
        if (!run.isEmpty()) {
            formatRun(writer.append(", "), a.get(0), run);
        }
        return writer.append(")");
    }

    /** {@code date_format} of {@code value} by the format {@code parts}. */
    private static SqlWriter formatRun(SqlWriter writer, SqlExpr value, List<com.legend.sql.DateFmt> parts) {
        return writer.append("date_format(").expr(value, 0).append(", ").expr(new SqlExpr.FormatLit(parts), 0)
                .append(")");
    }

    /** The {@code STRPTIME} arm of {@link #call}. */
    private SqlWriter strptime(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        return writer.append("to_timestamp(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
    }

    /** The {@code RANGE_FN} arm of {@link #call}. */
    private SqlWriter rangeFn(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlWriter.Piece start = a.size() == 1 ? w -> w.append("0L") : w -> w.append("CAST(").expr(a.get(0), 0)
                .append(" AS BIGINT)");
        SqlWriter.Piece stop = w -> w.append("CAST(").expr(a.get(a.size() == 1 ? 0 : 1), 0).append(" AS BIGINT)");
        SqlWriter.Piece step = a.size() > 2 ? w -> w.append("CAST(").expr(a.get(2), 0).append(" AS BIGINT)")
                : w -> w.append("1L");
        return writer.append("(CASE WHEN (").piece(stop).append(" - ").piece(start).append(") * ").piece(step)
                .append(" > 0 THEN filter(sequence(").piece(start).append(", ").piece(stop).append(", ")
                .piece(step).append("), `__g` -> `__g` <> ").piece(stop)
                .append(") ELSE CAST(array() AS ARRAY<BIGINT>) END)");
    }

    /** The {@code MAP_CONCAT} arm of {@link #call}. */
    private SqlWriter mapConcat(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        writer.append("map_concat(");
        for (int i = 0; i < a.size(); i++) {
            writer.append(i > 0 ? ", " : "");
            if (i == a.size() - 1) {
                writer.expr(a.get(i), 0);
                continue;
            }
            writer.append("map_filter(").expr(a.get(i), 0).append(", (`__k`, `__v`) -> NOT (");
            for (int j = i + 1; j < a.size(); j++) {
                writer.append(j > i + 1 ? " OR " : "").append("coalesce(map_contains_key(").expr(a.get(j), 0)
                        .append(", `__k`), FALSE)");
            }
            writer.append("))");
        }
        return writer.append(")");
    }

    /** The {@code JSON_TYPE} arm of {@link #call}. */
    private SqlWriter jsonType(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlWriter.Piece s = w -> w.append("schema_of_variant(").expr(a.get(0), 0).append(")");
        return writer.append("(CASE WHEN ").expr(a.get(0), 0).append(" IS NULL THEN NULL WHEN ").piece(s)
                .append(" = 'VOID' THEN 'NULL' WHEN ").piece(s).append(" = 'STRING' THEN 'VARCHAR' WHEN ")
                .piece(s).append(" = 'BOOLEAN' THEN 'BOOLEAN' WHEN startswith(").piece(s)
                .append(", 'ARRAY') THEN 'ARRAY' WHEN startswith(").piece(s)
                .append(", 'OBJECT') THEN 'OBJECT' WHEN ").piece(s)
                .append(" IN ('BIGINT', 'INT', 'SMALLINT', 'TINYINT') OR regexp_like(").piece(s)
                .append(", '^DECIMAL\\\\([0-9]+,0\\\\)$') THEN 'BIGINT' ELSE 'DOUBLE' END)");
    }

    /** A CONCAT argument as text: Spark's concat_ws takes strings only. */
    private void asText(SqlWriter w, SqlExpr e) {
        if (isText(e) || e instanceof SqlExpr.StringLit) {
            w.expr(e, 0);
        } else {
            w.append("CAST(").expr(e, 0).append(" AS STRING)");
        }
    }

    private static DialectCapability wall(SqlFn fn, String why) {
        return new DialectCapability(fn + " reached Databricks: " + why);
    }

    /** A regexp extraction's group: the named one, else 0. */
    private static SqlWriter.Piece group(List<SqlExpr> a) {
        return a.size() > 2 ? w -> w.append("CAST(").expr(a.get(2), 0).append(" AS INT)") : w -> w.append("0");
    }

    /** The pattern anchored at both ends (its inline flags apply inside the group, as Java reads them). */
    private SqlWriter.Piece anchored(SqlExpr p) {
        if (p instanceof SqlExpr.StringLit lit) {
            String s = stringLit("^(?:" + lit.value() + ")$");
            return w -> w.append(s);
        }
        return w -> w.append("concat('^(?:', ").expr(p, 0).append(", ')$')");
    }

    /** DuckDB's replacement text ({@code \1} a group, {@code $} itself) as Java's ({@code $1}, {@code \$}), each group
     *  number raised by {@code shift}. */
    private static String javaReplacement(String duck, int shift) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < duck.length(); i++) {
            char ch = duck.charAt(i);
            if (ch == '\\' && i + 1 < duck.length() && Character.isDigit(duck.charAt(i + 1))) {
                out.append('$').append(Character.digit(duck.charAt(++i), 10) + shift);
            } else if (ch == '\\' && i + 1 < duck.length()) {
                out.append('\\').append(duck.charAt(++i));
            } else if (ch == '$') {
                out.append("\\$");
            } else {
                out.append(ch);
            }
        }
        return out.toString();
    }

    // ---- temporal helpers

    /** A date part as Spark's extract field; a Pure integer (seconds truncated). */
    private SqlWriter.Piece extract(List<SqlExpr> a) {
        com.legend.sql.DatePart part = part(a.get(0));
        String field = switch (part) {
            case YEAR -> "YEAR";
            case QUARTER -> "QUARTER";
            case MONTH -> "MONTH";
            case WEEK -> "WEEK";
            case DAY -> "DAY";
            case HOUR -> "HOUR";
            case MINUTE -> "MINUTE";
            case SECOND -> "SECOND";
            case DAY_OF_YEAR -> "DOY";
            case ISO_DAY_OF_WEEK -> "DOW_ISO";
            case MILLISECOND, MICROSECOND, NANOSECOND -> throw new DialectCapability("date part " + part
                    + " has no probed Databricks spelling");
        };
        return w -> w.append("CAST(floor(extract(" + field + " FROM ").expr(a.get(1), 0).append(")) AS BIGINT)");
    }

    /** A unit as timestampadd names it. */
    private static String intervalUnit(com.legend.sql.DatePart unit) {
        return switch (unit) {
            case YEAR -> "YEAR";
            case MONTH -> "MONTH";
            case WEEK -> "WEEK";
            case DAY -> "DAY";
            case HOUR -> "HOUR";
            case MINUTE -> "MINUTE";
            case SECOND -> "SECOND";
            case MILLISECOND -> "MILLISECOND";
            case MICROSECOND -> "MICROSECOND";
            case QUARTER, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("interval unit " + unit
                    + " has no Databricks spelling");
        };
    }

    /** A unit as date_trunc names it. */
    private static String truncUnit(com.legend.sql.DatePart unit) {
        return switch (unit) {
            case YEAR -> "year";
            case QUARTER -> "quarter";
            case MONTH -> "month";
            case WEEK -> "week";
            case DAY -> "day";
            case HOUR -> "hour";
            case MINUTE -> "minute";
            case SECOND -> "second";
            case MILLISECOND -> "millisecond";
            case MICROSECOND -> "microsecond";
            case DAY_OF_YEAR, ISO_DAY_OF_WEEK, NANOSECOND -> throw new DialectCapability("date_trunc to " + unit
                    + " has no Databricks spelling");
        };
    }

    /** DuckDB's date_diff counts BOUNDARIES crossed (timestampdiff counts whole units, measured): year, quarter and
     *  month by the calendar, day by dates, the sub-day parts by the floored epoch in their unit. */
    private SqlWriter.Piece dateDiff(List<SqlExpr> a) {
        com.legend.sql.DatePart part = part(a.get(0));
        SqlWriter.Piece from = w -> w.expr(a.get(1), 0);
        SqlWriter.Piece to = w -> w.expr(a.get(2), 0);
        SqlWriter.Piece y = w -> w.append("(year(").piece(to).append(") - year(").piece(from).append("))");
        SqlWriter.Piece body = switch (part) {
            case YEAR -> y;
            case QUARTER -> w -> w.piece(y).append(" * 4 + (quarter(").piece(to).append(") - quarter(").piece(from)
                    .append("))");
            case MONTH -> w -> w.piece(y).append(" * 12 + (month(").piece(to).append(") - month(").piece(from)
                    .append("))");
            case DAY -> w -> w.append("datediff(CAST(").piece(to).append(" AS DATE), CAST(").piece(from)
                    .append(" AS DATE))");
            case HOUR -> epochBoundaries(from, to, 3600000000L);
            case MINUTE -> epochBoundaries(from, to, 60000000L);
            case SECOND -> epochBoundaries(from, to, 1000000L);
            case MILLISECOND -> epochBoundaries(from, to, 1000L);
            case MICROSECOND -> w -> w.append("(unix_micros(CAST(").piece(to).append(" AS TIMESTAMP)) - unix_micros(CAST(")
                    .piece(from).append(" AS TIMESTAMP)))");
            case WEEK, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_diff part " + part
                    + " has no probed Databricks spelling");
        };
        return w -> w.append("CAST(").piece(body).append(" AS BIGINT)");
    }

    /** The floored epoch (in microseconds per unit) of {@code to} less {@code from}'s: floor, before 1970 too. */
    private static SqlWriter.Piece epochBoundaries(SqlWriter.Piece from, SqlWriter.Piece to, long perUnit) {
        return w -> w.append("(floor(unix_micros(CAST(").piece(to).append(" AS TIMESTAMP)) / " + perUnit
                + ") - floor(unix_micros(CAST(").piece(from).append(" AS TIMESTAMP)) / " + perUnit + "))");
    }

    /** DuckDB's time_bucket: fixed units binned from 1970-01-01 (weeks from the Monday 1969-12-29) in microseconds,
     *  floored; months and years by the month index since 1970-01, floored to the bucket's size in months. */
    private SqlWriter.Piece timeBucket(List<SqlExpr> a) {
        com.legend.sql.DatePart unit = part(a.get(0));
        Long micros = bucketMicros(unit);
        SqlWriter.Piece t = w -> w.append("CAST(").expr(a.get(2), 0).append(" AS TIMESTAMP)");
        SqlWriter.Piece n = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS BIGINT)");
        if (micros != null) {
            long origin = unit == com.legend.sql.DatePart.WEEK ? -3L * 86400000000L : 0L;
            return w -> w.append("timestamp_micros(CAST(floor((unix_micros(").piece(t).append(") - (" + origin
                    + ")) / (").piece(n).append(" * " + micros + ")) AS BIGINT) * ").piece(n).append(" * " + micros
                    + " + (" + origin + "))");
        }
        Integer months = unit == com.legend.sql.DatePart.MONTH ? Integer.valueOf(1)
                : unit == com.legend.sql.DatePart.YEAR ? Integer.valueOf(12) : null;
        if (months == null) {
            throw new DialectCapability("a " + unit + " time bucket has no Databricks spelling");
        }
        SqlWriter.Piece size = w -> w.append("(").piece(n).append(" * " + months + ")");
        return w -> w.append("CAST(add_months(DATE '1970-01-01', CAST(floor(((year(").piece(t)
                .append(") - 1970) * 12 + month(").piece(t).append(") - 1) / ").piece(size).append(") * ").piece(size)
                .append(" AS INT)) AS TIMESTAMP)");
    }

    /** A fixed-length bucket's length in microseconds; null for a calendar one. */
    private static @com.legend.base.Nullable Long bucketMicros(com.legend.sql.DatePart unit) {
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
    // Hooks of the base
    // ==================================================================

    /** Pure's hashCode is a signed 64-bit integer: xxhash64 is one. */
    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("xxhash64(").list(a).append(")");
    }

    /** Pure round is half-EVEN: bround (round is half-up, measured). */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        writer.append("bround(").expr(a.get(0), 0);
        if (a.size() > 1) {
            writer.append(", CAST(").expr(a.get(1), 0).append(" AS INT)");
        }
        return writer.append(")");
    }

    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        SqlWriter.Piece x = w -> w.append("CAST(").expr(a.get(0), 0).append(" AS BIGINT)");
        SqlWriter.Piece y = w -> w.expr(a.get(1), 0);
        return switch (fnName) {
            case BIT_AND -> writer.append("(").piece(x).append(" & CAST(").piece(y).append(" AS BIGINT))");
            case BIT_OR -> writer.append("(").piece(x).append(" | CAST(").piece(y).append(" AS BIGINT))");
            case BIT_XOR -> writer.append("(").piece(x).append(" ^ CAST(").piece(y).append(" AS BIGINT))");
            case BIT_SHIFT_LEFT -> writer.append("shiftleft(").piece(x).append(", CAST(").piece(y).append(" AS INT))");
            case BIT_SHIFT_RIGHT -> writer.append("shiftright(").piece(x).append(", CAST(").piece(y).append(" AS INT))");
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
    }

    // ==================================================================
    // Aggregates and windows
    // ==================================================================

    /** A sort key of a collected list: the key, its direction, NULLs first or last. */
    private record SortKey(SqlWriter.Piece key, boolean ascending, boolean nullsFirst) {
    }

    /** The values collected into a list in the keys' order, NULL values kept (collect_list drops them): each row's
     *  value and keys as a struct, sorted by a comparator over the keys, then the value read back. */
    private SqlWriter.Piece collected(SqlWriter.Piece value, List<SortKey> keys) {
        return w -> {
            w.append("transform(");
            if (!keys.isEmpty()) {
                w.append("array_sort(");
            }
            w.append("collect_list(named_struct('v', ").piece(value);
            for (int i = 0; i < keys.size(); i++) {
                w.append(", 'k" + i + "', ").piece(keys.get(i).key());
            }
            w.append("))");
            if (!keys.isEmpty()) {
                w.append(", (`__l`, `__r`) -> ").append(comparator(keys, 0)).append(")");
            }
            w.append(", `__s` -> `__s`.`v`)");
        };
    }

    /** An array_sort comparator over the structs' keys from the {@code i}-th: -1, 0 or 1, NULLs where they sort. */
    private static String comparator(List<SortKey> keys, int i) {
        if (i == keys.size()) {
            return "0";
        }
        SortKey k = keys.get(i);
        String l = "`__l`.`k" + i + "`";
        String r = "`__r`.`k" + i + "`";
        String rest = comparator(keys, i + 1);
        return "CASE WHEN " + l + " IS NULL AND " + r + " IS NULL THEN " + rest
                + " WHEN " + l + " IS NULL THEN " + (k.nullsFirst() ? "-1" : "1")
                + " WHEN " + r + " IS NULL THEN " + (k.nullsFirst() ? "1" : "-1")
                + " WHEN " + l + " < " + r + " THEN " + (k.ascending() ? "-1" : "1")
                + " WHEN " + l + " > " + r + " THEN " + (k.ascending() ? "1" : "-1")
                + " ELSE " + rest + " END";
    }

    private SortKey sortKeyOf(SqlSelect.SortKey k) {
        boolean nullsFirst = k.nullOrder() != null
                ? k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                : !k.ascending();
        return new SortKey(w -> w.expr(k.expr(), 0), k.ascending(), nullsFirst);
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
            case MEDIAN -> writer.append("median(").expr(r.args().get(0), 0).append(")");
            case MODE -> writer.append("mode(").expr(r.args().get(0), 0).append(")");
            case STRING_AGG -> listagg(writer, r);
            case LIST -> arrayAgg(writer, r);
            case QUANTILE_CONT -> percentile(writer, "percentile_cont", r, false);
            // percentile_disc answers a DOUBLE whatever its input: cast to the value's own type
            case QUANTILE_DISC -> percentile(writer, "percentile_disc", r, true);
            case CORR -> aggregate(unordered(writer, r), "CORR", r);
            case COVAR_SAMP -> aggregate(unordered(writer, r), "COVAR_SAMP", r);
            case COVAR_POP -> aggregate(unordered(writer, r), "COVAR_POP", r);
            // the value at the key's greatest (least) row, rows of a NULL key ignored
            case ARG_MAX -> byExtreme(writer, "max_by", r);
            case ARG_MIN -> byExtreme(writer, "min_by", r);
            case BOOL_AND -> aggregate(unordered(writer, r), "BOOL_AND", r);
            case BOOL_OR -> aggregate(unordered(writer, r), "BOOL_OR", r);
            case VARIANCE -> aggregate(unordered(writer, r), "VARIANCE", r);
            case STDDEV -> aggregate(unordered(writer, r), "STDDEV", r);
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // a window function is a ranking or value function, never an aggregate (windowCall writes it)
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

    /** {@code writer}, for an aggregate Databricks cannot order: an ordered one is refused. */
    private static SqlWriter unordered(SqlWriter writer, SqlAgg.Reducer r) {
        if (!r.orderBy().isEmpty()) {
            throw new DialectCapability("an ordered " + r.fn() + " reached Databricks");
        }
        return writer;
    }

    /** The {@code STRING_AGG} arm of {@link #reducer}: {@code listagg(...)[ WITHIN GROUP (ORDER BY ...)]}. */
    private SqlWriter listagg(SqlWriter writer, SqlAgg.Reducer r) {
        writer.append("listagg(").append(r.distinct() ? "DISTINCT " : "").list(r.args()).append(")");
        if (!r.orderBy().isEmpty()) {
            writer.append(" WITHIN GROUP (ORDER BY ").join(r.orderBy(), ", ", this::aggregateOrderKey)
                    .append(")");
        }
        return writer;
    }

    /** {@code name(level) WITHIN GROUP (ORDER BY value[ DESC])}, cast to the value's own type when {@code cast}. */
    private SqlWriter percentile(SqlWriter writer, String name, SqlAgg.Reducer r, boolean cast) {
        if (r.args().size() != 2 || r.distinct() || r.orderBy().size() > 1) {
            throw new DialectCapability(r.fn() + " of this shape reached Databricks");
        }
        boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
        SqlType type = r.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
        String castTo = cast && type != null ? castTypeName(type) : null;
        if (castTo != null) {
            writer.append("CAST(");
        }
        writer.append(name).append("(")
                .expr(r.args().get(1), 0).append(") WITHIN GROUP (ORDER BY ").expr(r.args().get(0), 0)
                .append(desc ? " DESC" : "").append(")");
        return castTo != null ? writer.append(" AS ").append(castTo).append(")") : writer;
    }

    /** The {@code LIST} arm of {@link #reducer}. */
    private SqlWriter arrayAgg(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.distinct() || r.args().size() != 1) {
            throw new DialectCapability("a distinct LIST aggregate reached Databricks");
        }
        return writer.piece(collected(w -> w.expr(r.args().get(0), 0),
                r.orderBy().stream().map(this::sortKeyOf).toList()));
    }

    /** {@code max_by(value, key)} / {@code min_by}: the value at the key's greatest (least) row. */
    private static SqlWriter byExtreme(SqlWriter writer, String name, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct() || !r.orderBy().isEmpty()) {
            throw new DialectCapability(r.fn() + " of this shape reached Databricks");
        }
        return writer.append(name).append("(").list(r.args()).append(")");
    }

    /** A windowed average is a DOUBLE (the pass sees only grouped ones); a value or ranking function's offset is an
     *  INT literal, never a BIGINT one; a RANGE frame's offset has its ORDER BY key's type (Spark refuses another). */
    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        boolean cast = w.fn() instanceof SqlAgg.Reducer r && r.fn() == SqlAgg.Fn.AVG;
        if (cast) {
            writer.append("CAST(");
        }
        switch (w.fn()) {
            case SqlAgg.Reducer r -> reducer(writer, r);
            case SqlAgg.RankingFn r -> writer.append(aggregateName(r.fn())).append("(")
                    .join(r.args(), ", ", this::intArg).append(")");
            case SqlAgg.ValueFn v -> writer.append(aggregateName(v.fn())).append("(")
                    .join(v.args(), ", ", this::intArg).append(")");
        }
        writer.append(" OVER (");
        if (!w.partitionBy().isEmpty()) {
            writer.append("PARTITION BY ").list(w.partitionBy());
        }
        if (!w.orderBy().isEmpty()) {
            writer.append(w.partitionBy().isEmpty() ? "" : " ").append("ORDER BY ").join(w.orderBy(), ", ", this::sortKey);
        } else if (w.fn() instanceof SqlAgg.RankingFn) {
            // Spark refuses a ranking window with no order ("[WINDOW_FUNCTION_FRAME_NOT_ORDERED] Window function
            // row_number requires the window to be ordered"); a constant order is accepted and numbers the rows as
            // they come (measured on the warehouse 2026-10-19: OVER (ORDER BY 1) gives 1, 2). The judges' verdict
            // statement numbers a canon-ordered side this way (VerdictSql); channel B on Databricks refused every
            // such case before (step 9).
            writer.append(w.partitionBy().isEmpty() ? "" : " ").append("ORDER BY 1");
        }
        SqlExpr.WindowCall.Frame f = w.frame();
        if (f != null) {
            SqlType key = f.kind() == SqlExpr.WindowCall.Frame.Kind.RANGE && w.orderBy().size() == 1
                    ? typeOf(w.orderBy().get(0).expr()) : null;
            writer.append(" " + f.kind() + " BETWEEN " + frameBound(f.from(), key) + " AND " + frameBound(f.to(), key));
        }
        writer.append(")");
        return cast ? writer.append(" AS DOUBLE)") : writer;
    }

    /** A frame bound: a ROWS offset is an INT; a RANGE offset is its key's type; an interval offset singular. */
    private String frameBound(SqlExpr.WindowCall.Frame.Bound b, @com.legend.base.Nullable SqlType key) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> "UNBOUNDED PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> "CURRENT ROW";
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> "UNBOUNDED FOLLOWING";
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> offset(p.n(), key) + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.Following fo -> offset(fo.n(), key) + " FOLLOWING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> interval(p.n(), p.unit()) + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing fo -> interval(fo.n(), fo.unit()) + " FOLLOWING";
        };
    }

    /** An interval frame offset as a DAY TO SECOND or YEAR TO MONTH literal: Spark wants both bounds of a frame of
     *  ONE interval type, and {@code INTERVAL 1 SECOND} and {@code INTERVAL 1 DAY} are two (measured). */
    private static String interval(long n, String unit) {
        String u = unit.toUpperCase(java.util.Locale.ROOT);
        long seconds = switch (u) {
            case "WEEKS", "WEEK" -> 7 * 86400L;
            case "DAYS", "DAY" -> 86400L;
            case "HOURS", "HOUR" -> 3600L;
            case "MINUTES", "MINUTE" -> 60L;
            case "SECONDS", "SECOND" -> 1L;
            default -> -1L;
        };
        if (seconds > 0) {
            long total = Math.multiplyExact(n, seconds);
            return "INTERVAL '" + total / 86400 + " " + String.format(java.util.Locale.ROOT, "%02d:%02d:%02d",
                    total % 86400 / 3600, total % 3600 / 60, total % 60) + "' DAY TO SECOND";
        }
        long months = switch (u) {
            case "YEARS", "YEAR" -> 12L;
            case "MONTHS", "MONTH" -> 1L;
            default -> throw new DialectCapability("a " + unit + " window frame offset has no Databricks interval");
        };
        long total = Math.multiplyExact(n, months);
        return "INTERVAL '" + total / 12 + "-" + total % 12 + "' YEAR TO MONTH";
    }

    private String offset(Number n, @com.legend.base.Nullable SqlType key) {
        boolean whole = n instanceof Long || n instanceof Integer;
        if (key == null || key == SqlType.Scalar.INTEGER && whole) {
            return String.valueOf(n);
        }
        return key == SqlType.Scalar.BIGINT && whole ? n + "L" : "CAST(" + n + " AS " + castTypeName(key) + ")";
    }

    private void intArg(SqlWriter w, SqlExpr e) {
        if (e instanceof SqlExpr.IntLit i) {
            w.append(String.valueOf(i.value()));
        } else {
            w.expr(e, 0);
        }
    }

    // ==================================================================
    // JSON constructors: VARIANT objects and arrays (to_variant_object converts a struct or a list recursively)
    // ==================================================================

    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        return writer.append("to_variant_object(named_struct(").list(j.kv()).append("))");
    }

    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("to_variant_object(array(").join(j.elements(), ", ", (w, e) -> w.piece(toVariant(e)))
                .append("))");
    }

    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        List<SortKey> keys = j.orderKeys().stream()
                .map(k -> new SortKey(w -> w.expr(k.expr(), 0), !k.desc(), false)).toList();
        return writer.append("to_variant_object(").piece(collected(w -> w.piece(toVariant(j.value())), keys))
                .append(")");
    }

    // ==================================================================
    // Date format codes (Java's DateTimeFormatter letters, date_format)
    // ==================================================================

    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        for (com.legend.sql.DateFmt p : fl.parts()) {
            out.append(switch (p) {
                case com.legend.sql.DateFmt.Text t -> t.s().isEmpty() ? "" : "'" + t.s().replace("'", "''") + "'";
                case com.legend.sql.DateFmt.Part part -> switch (part) {
                    case YEAR4 -> "yyyy";
                    case MONTH2 -> "MM";
                    case DAY2 -> "dd";
                    case HOUR2 -> "HH";
                    case MIN2 -> "mm";
                    case SEC2 -> "ss";
                    case SUBSEC_MICRO -> "SSSSSS";
                    case SUBSEC_NANO -> "SSSSSS'000'";
                    // DuckDB's %g: milliseconds, three digits
                    case SUBSEC_MIN -> "SSS";
                    case MONTH_ABBREV -> "MMM";
                    case MONTH_NAME -> "MMMM";
                    case WEEKDAY_NAME -> "EEEE";
                    case HOUR12 -> "hh";
                    case HOUR12_NOPAD -> "h";
                    case AMPM -> "a";
                };
            });
        }
        return out.toString();
    }

    // ==================================================================
    // Values: what the driver hands back as JSON text
    // ==================================================================

    /** A struct or a map arrives as JSON text (measured): decoded to the platform's map of its fields. Lists are
     *  decoded by the executor's JSON carrier. */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            @com.legend.base.Nullable SqlType type) {
        if (jdbcValue instanceof String s && (type instanceof SqlType.Struct || type instanceof SqlType.Map)
                && s.startsWith("{")) {
            return com.legend.sql.Json.parse(s);
        }
        return jdbcValue;
    }

    /** {@inheritDoc} Its JSON_TYPE answers DuckDB's names (its emulation). */
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

    /** {@inheritDoc} A DATE is {@code date} (Spark's typeof); no other type's name is compared here. */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        if (type == com.legend.sql.SqlType.Scalar.DATE) {
            return "date";
        }
        throw new DialectCapability("a TYPEOF compared with " + type + "'s name reached Databricks");
    }
}
