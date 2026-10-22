// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The Trino EXECUTION dialect (Trino 483, its memory connector; 2026-10-12, docs/CLOUD_BACKENDS_PCT_2026_10_11.md
 * §18). A sibling of {@link Postgres} and {@link Databricks} over {@link AnsiSqlRenderer}: it changes nothing the other
 * dialects render. Every spelling here was measured on the server before it was trusted.
 *
 * <p>Trino is ANSI where Postgres is (double-quoted identifiers, {@code ''} escapes, no QUALIFY, no
 * {@code SELECT * EXCEPT}, no PIVOT) and has Databricks' lists: typed {@code ARRAY(T)} with lambdas ({@code transform},
 * {@code filter}, {@code reduce}, {@code any_match}, {@code all_match}), {@code ROW}, {@code MAP} and {@code JSON}. What
 * differs, measured:
 * <ul>
 *   <li>a bare integer literal is a 32-bit INTEGER whose arithmetic RAISES at 2^31: every integer literal is a
 *       BIGINT ({@code BIGINT '1'}); the few functions that take an INTEGER (round's and truncate's digits, a shift's
 *       count) are given one;</li>
 *   <li>{@code OFFSET} comes before {@code LIMIT};</li>
 *   <li>a lambda takes no element index ({@code zip_with} over the positions binds one); a list's moments, median
 *       and mode are its rows reduced
 *       ({@code (SELECT agg(e) FROM UNNEST(xs) AS u(e))}, a correlated subquery Trino runs);</li>
 *   <li>{@code UNNEST(xs, ys)} pads the shorter list with NULL, as DuckDB's parallel unnests do: an explosion is one
 *       {@code UNNEST} in FROM, lateral to its row;</li>
 *   <li>no exact percentile (only {@code approx_percentile}) and no mode: both from the group's values, sorted
 *       ({@code array_agg}, {@code histogram});</li>
 *   <li>{@code round} rounds half away from zero (Pure rounds half to even); a double's text is always in scientific
 *       notation ({@code 4.1E1}), which the platform's Float spelling unfolds.</li>
 * </ul>
 *
 * <p>Every {@link SqlFn} and {@link SqlAgg.Fn} is decided HERE by an exhaustive switch: its spelling, a shared
 * helper of the base named in its arm, or a {@link DialectCapability} wall.
 */
public final class Trino extends AnsiSqlRenderer {

    /** Trino (2026-10-12, docs/CLOUD_BACKENDS_PCT_2026_10_11.md §18): double quotes; Trino's documented reserved
     *  keywords. The {@code Trino} dialect quotes every identifier, so the list decides no spelling there. */
    public static final Lexicon LEXICON = new Lexicon('"', Set.of(
            "alter", "and", "as", "auto", "between", "by", "case", "cast", "constraint", "create", "cross", "cube",
            "current_catalog", "current_date", "current_path", "current_role", "current_schema", "current_time",
            "current_timestamp", "current_user", "deallocate", "delete", "describe", "distinct", "drop", "else", "end",
            "escape", "except", "execute", "exists", "extract", "false", "for", "from", "full", "group", "grouping",
            "having", "in", "inner", "insert", "intersect", "into", "is", "join", "json_array", "json_exists",
            "json_object", "json_query", "json_table", "json_value", "left", "like", "listagg", "localtime",
            "localtimestamp", "natural", "normalize", "not", "null", "on", "or", "order", "outer", "prepare",
            "recursive", "right", "rollup", "select", "skip", "table", "then", "trim", "true", "uescape", "union",
            "unnest", "using", "values", "when", "where", "with"));

    /** Trino (2026-10-12): typed native arrays with lambdas -- the strategies stand aside, as on Databricks; its
     *  dialect writes the static pivot and the as-of join itself (Trino has neither); FULL OUTER is native. */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(true, true, true, true);

    public Trino() {
        super(LEXICON);
    }

    // ==================================================================
    // Session and structure
    // ==================================================================

    /** The engine's H2 text is refused here: Trino has no translation of it (decision D2: raw text runs on the
     *  database its connection declares; only the engine's H2 text is translated, for H2, DuckDB and Postgres). */
    @Override
    public String fromEngineText(String statement) {
        throw new DialectCapability("the engine's H2 text reached Trino, which has no translation of it");
    }

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "BOOLEAN";
            case INTEGER -> "INTEGER";
            case BIGINT -> "BIGINT";
            case HUGEINT -> "DECIMAL(38,0)";
            case DOUBLE -> "DOUBLE";
            case VARCHAR, LITERAL, TEMPORAL_TEXT -> "VARCHAR";
            case DATE -> "DATE";
            case TIMESTAMP -> "TIMESTAMP(6)";
            case TIMESTAMPTZ -> "TIMESTAMP(6) WITH TIME ZONE";
            case JSON -> "JSON";
            case TIME -> "TIME(6)";
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** The platform's naive-UTC temporal contract. */
    @Override
    public List<String> sessionSetup() {
        return List.of("SET TIME ZONE 'UTC'");
    }

    /** No native PIVOT: a static pivot is a grouped CASE aggregation, its values discovered first. */
    // the probe plan's facts (2026-10-20): Trino: no transaction across a session's statements for its connectors; no column limit documented
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

    /** Native lists (the carrier strategies stand aside); a static pivot as a grouped CASE aggregation and an as-of
     *  join as a window pick (Trino has neither, and refuses the portable form's correlated MAX in a join condition:
     *  "Reference to column from outer scope not allowed in this context", measured); the star exclusion expanded;
     *  every list explosion an UNNEST in FROM; the substring start clamp (a start of 0 is the empty string here);
     *  averages delivered as DOUBLE (an average of a decimal is a decimal here); QUALIFY as a wrapping subselect; a
     *  constant GROUP BY or ORDER BY key as a typed expression (Trino reads an integer one as a position); the root's
     *  DECIMAL(p,s) and HUGEINT columns cast to their declared types. */
    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS), new PivotAsGroupBy(), new AsOfAsPick(),
                new StarExceptToColumns(), new ExplosionMarkers.LateralUnnestAsMarker(),
                new ExplosionMarkers.SelectListUnnestAsLateral(), new ExplosionMarkers.StarOverExplosionUnwrapped(),
                new CheckedDefectsToLists(), new SubstringClamp(),
                new AggregatesDeliverDouble(java.util.Set.of(SqlAgg.Fn.AVG)), new QualifyToSubselect(true),
                new ConstantKeysAsExpressions(), new RootNumericTypes());
    }

    /** {@code OFFSET n} before {@code LIMIT m}: Trino's order (the other is a syntax error, measured). A bare
     *  explosion MARKER (a FROM-less select of UNNESTs, the lists a query answers) reads its elements from one
     *  {@code UNNEST ... WITH ORDINALITY}, in their lists' order. */
    @Override
    protected SqlWriter select(SqlWriter writer, SqlSelect s, int depth) {
        if (ExplosionMarkers.isMarker(s)) {
            writer.append("SELECT ").join(s.projections(), ", ", (w, p) -> {
                String column = aliasIdent(ExplosionMarkers.markerColumn(p));
                // a ROW element as its JSON text (rowsAsJson); a struct carried as text (a metamodel element by its
                // name) is text already
                SqlExpr xs = ((SqlExpr.Call) p.expr()).args().get(0);
                if (depth == 0 && p.out() != null && p.out().type() instanceof SqlType.Struct st
                        && (listElement(xs) == null || listElement(xs) instanceof SqlType.Struct)) {
                    w.append("CAST(CAST(" + column + " AS " + castTypeName(st) + ") AS JSON) AS " + column);
                } else {
                    w.append(column);
                }
            }).append(" FROM ").piece(unnest(s, "__unnest", true));
            return nl(writer, depth).append("ORDER BY ").append(aliasIdent("__ord"));
        }
        if (depth == 0) {
            s = rowsAsJson(s);
        }
        if (s.offset() == null) {
            return super.select(writer, s, depth);
        }
        super.select(writer, s.withLimit(null).withOffset(null), depth);
        nl(writer, depth).append("OFFSET ").append(s.offset());
        if (s.limit() != null) {
            nl(writer, depth).append("LIMIT ").append(s.limit());
        }
        return writer;
    }

    /** The statement's own ROW columns as their JSON text: the driver hands a ROW as its own class (io.trino.jdbc.Row),
     *  which the platform reads through no interface (production code reflects on nothing), and JSON text it decodes
     *  ({@link #normalize}). Each is cast to its declared ROW first, whose named fields are an object's keys (an
     *  anonymous row's, a zip's pair, are empty, measured). */
    private static SqlSelect rowsAsJson(SqlSelect s) {
        java.util.Map<String, SqlType> labels = new java.util.HashMap<>();
        s.outputs().forEach(o -> labels.put(o.name(), o.type()));
        List<SqlSelect.Projection> ps = new ArrayList<>();
        boolean changed = false;
        for (SqlSelect.Projection p : s.projections()) {
            String name = p.alias() != null ? p.alias() : p.outputName();
            SqlType label = name != null && labels.get(name) != null ? labels.get(name) : typeOf(p.expr());
            // a ROW itself (a struct carried as text, a metamodel element by its name, is already text)
            if (label instanceof SqlType.Struct st && typeOf(p.expr()) instanceof SqlType.Struct
                    && !(p.expr() instanceof SqlExpr.StringLit)) {
                ps.add(new SqlSelect.Projection(new SqlExpr.Cast(new SqlExpr.Cast(p.expr(), st, false),
                        SqlType.Scalar.JSON, false), name, p.out()));
                changed = true;
            } else {
                ps.add(p);
            }
        }
        return changed ? new SqlSelect(ps, s.distinct(), s.from(), s.where(), s.groupBy(), s.having(), s.qualify(),
                s.orderBy(), s.limit(), s.offset(), s.outputs()) : s;
    }

    /** An explosion MARKER (ExplosionMarkers) is one {@code UNNEST} of its lists, which Trino pads to the longest
     *  with NULL as DuckDB does; a lateral one joins its row ({@code CROSS JOIN UNNEST}, or {@code LEFT JOIN UNNEST ...
     *  ON TRUE} keeping an empty list's row). */
    @Override
    protected SqlWriter source(SqlWriter writer, SqlSource src, int depth) {
        if (src instanceof SqlSource.Join j && j.right() instanceof SqlSource.Subselect sub
                && ExplosionMarkers.isMarker(sub.inner())
                && (j.kind() == SqlSource.Join.Kind.CROSS_LATERAL || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)) {
            if (j.on() != null && !(j.on() instanceof SqlExpr.BoolLit b && b.value())) {
                throw new DialectCapability("a lateral explosion with a join condition reached Trino");
            }
            source(writer, j.left(), depth);
            nl(writer, depth).append(j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? "LEFT JOIN " : "CROSS JOIN ")
                    .piece(unnest((SqlSelect) sub.inner(), sub.alias(), false));
            return j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? writer.append(" ON TRUE") : writer;
        }
        if (src instanceof SqlSource.Subselect sub && ExplosionMarkers.isMarker(sub.inner())) {
            return writer.piece(unnest((SqlSelect) sub.inner(), sub.alias(), false));
        }
        if (src instanceof SqlSource.Join j && j.kind() == SqlSource.Join.Kind.LEFT_LATERAL && j.on() == null) {
            source(writer, j.left(), depth);
            nl(writer, depth).append("LEFT JOIN LATERAL ");
            source(writer, j.right(), depth);
            return writer.append(" ON TRUE");
        }
        return super.source(writer, src, depth);
    }

    /** {@code UNNEST(xs, ys) AS alias(a, b)}: a NULL list as an empty one of its element's type (no row either way),
     *  a list of rows each wrapped in a row of one field; {@code ordinal}: {@code WITH ORDINALITY}, the position a
     *  column {@code __ord}. */
    private SqlWriter.Piece unnest(SqlSelect marker, String alias, boolean ordinal) {
        return w -> {
            w.append("UNNEST(").join(marker.projections(), ", ", (v, p) -> {
                SqlExpr xs = ((SqlExpr.Call) p.expr()).args().get(0);
                SqlType declared = p.out() != null ? p.out().type() : typeOf(p.expr());
                SqlType element = ExplosionMarkers.nullList(xs) || listElement(xs) == null ? declared : listElement(xs);
                SqlWriter.Piece array;
                if (ExplosionMarkers.nullList(xs)) {
                    array = a -> a.append(declared == null ? "ARRAY[]"
                            : "CAST(ARRAY[] AS " + arrayTypeName(castTypeName(declared)) + ")");
                } else if (listElement(xs) == null && declared != null) {
                    // a list of no element type (Pure's []) as the explosion's declared one: its rows' fields read
                    array = a -> a.append("CAST(").expr(xs, 0).append(" AS " + arrayTypeName(castTypeName(declared))
                            + ")");
                } else {
                    // a list of no type at all is Trino's own ARRAY[], an array(unknown): no rows
                    array = a -> a.expr(xs, 0);
                }
                if (element instanceof SqlType.Struct) {
                    // UNNEST spreads a ROW element over one column per field: each wrapped in a one-field row
                    // stays one column, the row itself (measured: "Column alias list has 1 entries but ... has 2")
                    v.append("transform(").piece(array).append(", \"__e\" -> ROW(\"__e\"))");
                } else {
                    v.piece(array);
                }
            }).append(ordinal ? ") WITH ORDINALITY AS " : ") AS ").append(aliasIdent(alias)).append("(")
                    .append(String.join(", ", marker.projections().stream()
                            .map(p -> aliasIdent(ExplosionMarkers.markerColumn(p))).toList()))
                    .append(ordinal ? ", " + aliasIdent("__ord") + ")" : ")");
        };
    }

    @Override
    protected String starExceptKeyword() {
        throw new DialectCapability("SELECT * EXCEPT reached Trino, which has no star exclusion; the column list is"
                + " expanded upstream (StarExceptToColumns)");
    }

    @Override
    protected String rowOrderColumn() {
        throw new DialectCapability("row order reached Trino, which has no row-order column");
    }

    @Override
    protected String asOfJoinClause() {
        throw new IllegalStateException("an as-of join reached the join clause: Trino writes it as a pick (AsOfAsPick)");
    }

    @Override
    protected SqlWriter pivotSource(SqlWriter writer, SqlSource.Pivot p, int depth) {
        throw new IllegalStateException("a pivot reached Trino's writer: PivotAsGroupBy writes it as a grouped"
                + " aggregation (a dynamic pivot is discovered first)");
    }

    // ==================================================================
    // Lexical: identifiers, literals, types
    // ==================================================================

    /** Every identifier quoted; a double-quoted declaration ({@code "date"}) is already its own spelling. */
    @Override
    protected String ident(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")
                && !name.substring(1, name.length() - 1).replace("\"\"", "").contains("\"")) {
            return name;
        }
        return delimited(name);
    }

    @Override
    protected String arrayTypeName(String element) {
        return "ARRAY(" + element + ")";
    }

    @Override
    protected String mapTypeName(String key, String value) {
        return "MAP(" + key + ", " + value + ")";
    }

    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        List<java.util.Map.Entry<String, String>> fields = structFields(st);
        StringBuilder sb = new StringBuilder("ROW(");
        for (int i = 0; i < fields.size(); i++) {
            sb.append(i > 0 ? ", " : "").append(fields.get(i).getKey()).append(" ").append(fields.get(i).getValue());
        }
        return sb.append(")").toString();
    }

    /** A timestamp literal keeps its digits (TIMESTAMP(p) by the literal's own precision, measured); its date and
     *  time are apart by a space (a {@code T} is refused, measured). */
    @Override
    protected String timestampLit(String iso) {
        return "TIMESTAMP '" + iso.replaceFirst("T", " ") + "'";
    }

    /** Alias-less projections label EXPLICITLY from the declared output: Trino labels an expression {@code _colN}. */
    @Override
    protected @com.legend.base.Nullable String implicitLabel(SqlSelect.Projection p) {
        return p.out() != null ? aliasIdent(p.out().name()) : super.implicitLabel(p);
    }

    /** A value that can only be NULL (a NULL literal, a raise) is spelled in its declared slot's type: Trino types a bare
     *  NULL {@code unknown}, which a reader of the column cannot always use. */
    @Override
    protected SqlWriter projection(SqlWriter writer, SqlSelect.Projection p) {
        if (p.out() != null && (p.expr().type() instanceof com.legend.sql.TypeFact.Bottom || isRaise(p.expr())
                || p.expr() instanceof SqlExpr.NullLit)) {
            writer.append("CAST(").expr(p.expr(), 0).append(" AS ").append(castTypeName(p.out().type())).append(")");
            String label = p.alias() != null ? aliasIdent(p.alias()) : implicitLabel(p);
            return label == null ? writer : writer.append(" AS ").append(label);
        }
        return super.projection(writer, p);
    }

    private static boolean isRaise(SqlExpr e) {
        return e instanceof SqlExpr.Call c && c.fn() == SqlFn.ERROR;
    }

    /** A branch that IS the raise is cast to the CASE's own type: {@code fail} is {@code unknown}. */
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

    /** A Float literal is its digits, a DECIMAL (the numeric charter's Rule 1); past a DECIMAL's 38 digits Trino
     *  refuses it ("not a valid DECIMAL literal", measured: a double's exact expansion, 48 digits; DuckDB reads such a
     *  literal as a DOUBLE): it is the double's shortest text instead (a DOUBLE where that is in exponent form). */
    @Override
    protected String floatLiteral(double v) {
        String plain = super.floatLiteral(v);
        return plain.replace("-", "").replace(".", "").replaceFirst("^0+", "").length() > 38 ? Double.toString(v)
                : plain;
    }

    /** An integer past a signed BIGINT: a bare one is refused ("Invalid numeric literal", measured); a DECIMAL's
     *  holds 38 digits. */
    @Override
    protected String hugeIntLiteral(java.math.BigDecimal v) {
        return "DECIMAL '" + v.toPlainString() + "'";
    }

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        return switch (e) {
            // a BIGINT: a bare literal is a 32-bit INTEGER, whose arithmetic raises at 2^31
            case SqlExpr.IntLit i -> writer.append("BIGINT '" + i.value() + "'");
            // past a DECIMAL's 38 digits a decimal literal is refused ("not a valid DECIMAL literal", measured: a
            // double's exact expansion, 48 digits); DuckDB reads one as a DOUBLE, and so does this
            case SqlExpr.DecimalLit d when d.value().precision() > 38 ->
                    writer.append("DOUBLE '" + d.value().toPlainString() + "'");
            case SqlExpr.OrderedListAgg ola -> writer.append("array_agg(").expr(ola.value(), 0).append(" ORDER BY ")
                    .expr(ola.orderBy(), 0).append(" NULLS LAST)");
            default -> super.expr(writer, e, parentPrec);
        };
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

    /** A list argument: the empty collection (a bare NULL, untyped) as an empty array, whose {@code unknown} elements
     *  coerce to any type (Trino refuses a bare NULL where an array is required). */
    private SqlWriter.Piece list(SqlExpr xs) {
        if (xs instanceof SqlExpr.NullLit) {
            return w -> w.append("CAST(NULL AS ARRAY(UNKNOWN))");
        }
        return w -> w.expr(xs, 0);
    }

    /** {@code xs} less its NULL elements. */
    private SqlWriter.Piece present(SqlExpr xs) {
        return w -> w.append("filter(").piece(list(xs)).append(", \"__x\" -> \"__x\" IS NOT NULL)");
    }

    /** A list of rows types each field by the first row whose field is typed: a missing property is a NULL of no
     *  type ({@code ^Person(lastName='Peter')}), and a ROW's fields are typed. */
    @Override
    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        if (!elements.isEmpty() && elements.stream().allMatch(e -> e instanceof SqlExpr.StructLit)) {
            java.util.Map<String, SqlType> types = new java.util.HashMap<>();
            for (SqlExpr e : elements) {
                for (SqlExpr.StructLit.Field f : ((SqlExpr.StructLit) e).fields()) {
                    SqlType t = typeOf(f.value());
                    if (t != null) {
                        types.putIfAbsent(f.name(), t);
                    }
                }
            }
            return writer.append("ARRAY[").join(elements, ", ", (w, e) -> structLit(w, (SqlExpr.StructLit) e, types))
                    .append("]");
        }
        return writer.append("ARRAY[").list(elements).append("]");
    }

    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit st) {
        return structLit(writer, st, java.util.Map.of());
    }

    private SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit st, java.util.Map<String, SqlType> siblings) {
        writer.append("CAST(ROW(").join(st.fields(), ", ", (w, f) -> w.expr(f.value(), 0)).append(") AS ROW(");
        return writer.join(st.fields(), ", ", (w, f) -> {
            SqlType t = typeOf(f.value());
            if (t == null) {
                // an absent optional property: the slot its builder declared (as ClickHouse and DuckDB read it;
                // 2026-10-20, the parity phase: five Grammar and Essential tests constructing an instance)
                t = f.declared();
            }
            if (t == null) {
                t = siblings.get(f.name());
            }
            if (t == null) {
                throw new DialectCapability("a struct field of unknown type reached Trino: a ROW's fields are typed");
            }
            w.append(ident(f.name())).append(" ").append(castTypeName(t));
        }).append("))");
    }

    /** Whether both operands are ROWs (structs). */
    private static boolean rows(List<SqlExpr> a) {
        return a.size() == 2 && typeOf(a.get(0)) instanceof SqlType.Struct && typeOf(a.get(1)) instanceof SqlType.Struct;
    }

    /** A field by name; a field NUMBER (a zip's pair, read by position) a row subscript ({@code r[1]}). */
    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        if (g.field().matches("[0-9]+")) {
            return writer.append("(").expr(g.source(), 8).append(")[" + g.field() + "]");
        }
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

    /** {@code xs}'s 1-based positions, {@code [1, ..., n]} ({@code []} for an empty list, NULL for a NULL one). */
    private SqlWriter.Piece positions(SqlExpr xs) {
        return w -> w.append("slice(sequence(1, cardinality(").piece(list(xs)).append(") + 1), 1, cardinality(")
                .piece(list(xs)).append("))");
    }

    /** A lambda over the element alone: Trino's take no index. */
    private static SqlExpr.Lambda elementOnly(SqlExpr.Lambda l, SqlFn fn) {
        if (l.params().size() > 1) {
            throw new DialectCapability(fn + " with an element index reached Trino: its lambdas take the element"
                    + " alone");
        }
        return l;
    }

    /** {@code (SELECT agg FROM UNNEST(xs) AS u(e))}: a list reduced as rows (a correlated scalar subquery, measured);
     *  an empty or NULL list reduces over no rows. */
    private SqlWriter.Piece overRows(SqlExpr xs, SqlAgg.Fn fn, List<SqlExpr> extras) {
        SqlType element = listElement(xs);
        SqlExpr e = element == null ? SqlExpr.Column.derived(null, "__e") : SqlExpr.Column.param("__e", xs);
        List<SqlExpr> args = new ArrayList<>();
        args.add(e);
        args.addAll(extras);
        SqlAgg.Reducer r = new SqlAgg.Reducer(fn, args, false, List.of());
        return w -> {
            w.append("(SELECT ");
            reducer(w, r);
            w.append(" FROM UNNEST(").piece(list(xs)).append(") AS \"__v\"(").append(param("__e")).append("))");
        };
    }

    @Override
    protected SqlWriter listCall(SqlWriter writer, SqlFn fnName, List<SqlExpr> args) {
        SqlExpr xs = args.get(0);
        return switch (fnName) {
            // a lambda over the element and its 1-based index (DuckDB's): zip_with binds both, over the positions
            case LIST_FILTER -> ((SqlExpr.Lambda) args.get(1)).params().size() > 1
                    ? writer.append("transform(filter(zip(").piece(list(xs)).append(", zip_with(").piece(list(xs))
                            .append(", ").piece(positions(xs)).append(", ").expr(args.get(1), 0)
                            .append(")), \"__z\" -> \"__z\"[2]), \"__z\" -> \"__z\"[1])")
                    : writer.append("filter(").piece(list(xs)).append(", ").expr(args.get(1), 0).append(")");
            case LIST_TRANSFORM -> ((SqlExpr.Lambda) args.get(1)).params().size() > 1
                    ? writer.append("zip_with(").piece(list(xs)).append(", ").piece(positions(xs)).append(", ")
                            .expr(args.get(1), 0).append(")")
                    : writer.append("transform(").piece(list(xs)).append(", ").expr(args.get(1), 0).append(")");
            // NULL || xs is xs, as DuckDB's list_concat
            // a NULL or empty literal list adds nothing, and an untyped one leaves concat no candidate ("Could not
            // choose a best candidate operator", measured): only the others are concatenated
            case LIST_CONCAT -> {
                List<SqlExpr> parts = args.stream().filter(a -> !(a instanceof SqlExpr.NullLit)
                        && !(a instanceof SqlExpr.ArrayLit al && al.elements().isEmpty())).toList();
                if (parts.isEmpty()) {
                    yield writer.append("ARRAY[]");
                }
                if (parts.size() == 1) {
                    yield writer.append("coalesce(").piece(list(parts.get(0))).append(", ARRAY[])");
                }
                yield writer.append("concat(").join(parts, ", ", (w, a) -> w.append("coalesce(").piece(list(a))
                        .append(", ARRAY[])")).append(")");
            }
            // 1-based, negative from the end, out of range NULL (DuckDB's list_extract); 0 is out of range
            case LIST_GET -> writer.append("(CASE WHEN ").expr(args.get(1), 0).append(" = 0 THEN NULL ELSE element_at(")
                    .piece(list(xs)).append(", ").expr(args.get(1), 0).append(") END)");
            // DuckDB's list_position: NULL when absent (array_position answers 0); a CASE, not nullif, which inside a
            // lambda fails in Trino's own planner ("io.trino.sql.ir.Bind cannot be cast to io.trino.sql.ir.Lambda",
            // measured on 483)
            case LIST_POSITION -> writer.append("(CASE WHEN array_position(").piece(list(xs)).append(", ")
                    .expr(args.get(1), 0).append(") = 0 THEN NULL ELSE array_position(").piece(list(xs)).append(", ")
                    .expr(args.get(1), 0).append(") END)");
            // DuckDB's list_distinct drops NULLs; array_distinct keeps the first occurrence's order
            case LIST_DISTINCT -> writer.append("array_distinct(").piece(present(xs)).append(")");
            case LIST_APPEND -> writer.append("concat(").piece(list(xs)).append(", ARRAY[").expr(args.get(1), 0)
                    .append("])");
            case LIST_SUM -> writer.piece(overRows(xs, SqlAgg.Fn.SUM, List.of()));
            // array_min and array_max are NULL when an element is: the present elements
            case LIST_MIN -> writer.append("array_min(").piece(present(xs)).append(")");
            case LIST_MAX -> writer.append("array_max(").piece(present(xs)).append(")");
            case LIST_AVG -> writer.append("CAST(").piece(overRows(xs, SqlAgg.Fn.AVG, List.of())).append(" AS DOUBLE)");
            case LIST_MEDIAN -> writer.piece(overRows(xs, SqlAgg.Fn.MEDIAN, List.of()));
            case LIST_MODE -> writer.piece(overRows(xs, SqlAgg.Fn.MODE, List.of()));
            case LIST_BOOL_AND -> writer.piece(overRows(xs, SqlAgg.Fn.BOOL_AND, List.of()));
            case LIST_BOOL_OR -> writer.piece(overRows(xs, SqlAgg.Fn.BOOL_OR, List.of()));
            case LIST_PRODUCT -> writer.append("reduce(").piece(list(xs)).append(", CAST(1 AS DOUBLE), ")
                    .append("(\"__a\", \"__x\") -> \"__a\" * coalesce(\"__x\", 1), \"__a\" -> \"__a\")");
            // array_sort: ascending, NULLs last (DuckDB's default); descending, NULLs last
            case LIST_SORT -> writer.append("array_sort(").piece(list(xs)).append(")");
            case LIST_SORT_DESC -> writer.append("concat(reverse(array_sort(").piece(present(xs))
                    .append(")), filter(").piece(list(xs)).append(", \"__x\" -> \"__x\" IS NULL))");
            case LIST_REVERSE -> writer.append("reverse(").piece(list(xs)).append(")");
            case LIST_TAIL -> writer.append("slice(").piece(list(xs)).append(", 2, greatest(cardinality(")
                    .piece(list(xs)).append(") - 1, 0))");
            case LIST_INIT -> writer.append("slice(").piece(list(xs)).append(", 1, greatest(cardinality(")
                    .piece(list(xs)).append(") - 1, 0))");
            // DuckDB's list_slice: 1-based, both ends inclusive
            case LIST_SLICE -> writer.append("slice(").piece(list(xs)).append(", greatest(").expr(args.get(1), 0)
                    .append(", 1), greatest(").expr(args.get(2), 6).append(" - greatest(").expr(args.get(1), 0)
                    .append(", 1) + 1, 0))");
            // DuckDB's list_zip: pairs by position (a struct read by field number, structGet); truncated to the
            // shorter list when asked, else the shorter padded with NULL, as Trino's zip does
            case LIST_ZIP -> {
                boolean truncate = args.size() > 2 && args.get(args.size() - 1) instanceof SqlExpr.BoolLit b
                        && b.value();
                List<SqlExpr> lists = args.size() > 2 && args.get(args.size() - 1) instanceof SqlExpr.BoolLit
                        ? args.subList(0, args.size() - 1) : args;
                if (!truncate) {
                    yield writer.append("zip(").join(lists, ", ", (w, l) -> w.piece(list(l))).append(")");
                }
                SqlWriter.Piece n = w -> w.append("least(").join(lists, ", ", (v, l) -> v.append("coalesce(cardinality(")
                        .piece(list(l)).append("), 0)")).append(")");
                yield writer.append("zip(").join(lists, ", ", (w, l) -> w.append("slice(").piece(list(l))
                        .append(", 1, ").piece(n).append(")")).append(")");
            }
            // DuckDB's list_reduce: the first element is the accumulator's start, the lambda (acc, x) over the rest
            case LIST_REDUCE -> writer.append("reduce(slice(").piece(list(xs)).append(", 2, greatest(cardinality(")
                    .piece(list(xs)).append(") - 1, 0)), element_at(").piece(list(xs)).append(", 1), ")
                    .expr(args.get(1), 0).append(", \"__s\" -> \"__s\")");
            // Trino's own name of the value's type, compared only with typeofName's (the judges' grid sides read a
            // declared timestamp's DATE cells by it since the plan path program's step 6, 2026-10-20)
            case TYPEOF -> writer.function("typeof", args);
            case RANGE_FN, REPEAT_VALUE -> throw new IllegalStateException(fnName + " is rendered by call");
            default -> throw new IllegalStateException("not a list call: " + fnName);
        };
    }

    @Override
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("coalesce(any_match(").piece(list(args.get(0))).append(", ")
                .expr(elementOnly((SqlExpr.Lambda) args.get(1), SqlFn.LIST_EXISTS), 0).append("), FALSE)");
    }

    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("coalesce(all_match(").piece(list(args.get(0))).append(", ")
                .expr(elementOnly((SqlExpr.Lambda) args.get(1), SqlFn.LIST_FOR_ALL), 0).append("), TRUE)");
    }

    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("coalesce(cardinality(array_distinct(").piece(list(args.get(0))).append(")) = cardinality(")
                .piece(list(args.get(0))).append("), TRUE)");
    }

    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        return writer.append("contains(").piece(list(m.collection())).append(", ").expr(m.needle(), 0).append(")");
    }

    /** A list reduced: by an array function where one says it exactly, else as rows ({@link #overRows}). NULL
     *  elements are skipped, as an aggregate skips them. */
    @Override
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        SqlWriter.Piece present = present(rc.collection());
        return switch (rc.reducer()) {
            // NULL over no present element, as string_agg over no row
            case STRING_AGG -> rc.extras().size() == 1
                    ? writer.append("(CASE WHEN cardinality(").piece(present).append(") = 0 THEN NULL ELSE array_join(")
                            .append("transform(").piece(present).append(", \"__x\" -> CAST(\"__x\" AS VARCHAR)), ")
                            .expr(rc.extras().get(0), 0).append(") END)")
                    : writer.piece(overRows(rc.collection(), rc.reducer(), rc.extras()));
            case MIN -> writer.append("array_min(").piece(present).append(")");
            case MAX -> writer.append("array_max(").piece(present).append(")");
            case COUNT -> writer.append("cardinality(").piece(present).append(")");
            default -> writer.piece(overRows(rc.collection(), rc.reducer(), rc.extras()));
        };
    }

    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("an UNNEST in a select list reached Trino: it explodes only in FROM"
                + " (ExplosionMarkers places it there; a grouped select's is refused)");
    }

    /** Pure's fold, {@code (element, acc)}, as Trino's {@code reduce(xs, init, (acc, x) -> ..., acc -> acc)}: its
     *  accumulator may be any type, a list included; an empty or NULL list folds to the init. */
    @Override
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        if (!(f.type() instanceof com.legend.sql.TypeFact.Typed t)) {
            throw new DialectCapability("a fold of unknown type reached Trino");
        }
        String type = castTypeName(t.type());
        String elem = param(f.lambda().params().get(0));
        String acc = param(f.lambda().params().get(1));
        return writer.append("reduce(coalesce(").piece(list(f.source())).append(", ARRAY[]), CAST(").expr(f.init(), 0)
                .append(" AS " + type + "), (" + acc + ", " + elem + ") -> CAST(").expr(f.lambda().body(), 0)
                .append(" AS " + type + "), " + acc + " -> " + acc + ")");
    }

    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        // Pure's splitPart: the non-empty tokens, 1-based, NULL past the end (split takes the delimiter as text)
        return writer.append("element_at(filter(split(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                .append("), \"__t\" -> \"__t\" <> ''), ").expr(a.get(2), 0).append(")");
    }

    @Override
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> args) {
        SqlType st = typeOf(args.get(0));
        if (!(st instanceof SqlType.Struct struct) || !(args.get(1) instanceof SqlExpr.StringLit name)) {
            throw new DialectCapability("struct_insert over an untyped struct reached Trino");
        }
        SqlType added = typeOf(args.get(2));
        if (added == null) {
            throw new DialectCapability("struct_insert of a value of unknown type reached Trino");
        }
        List<SqlType.Struct.Field> kept = struct.fields().stream().filter(f -> !f.name().equals(name.value())).toList();
        writer.append("CAST(ROW(");
        for (SqlType.Struct.Field f : kept) {
            writer.append("(").expr(args.get(0), 8).append(").").append(ident(f.name())).append(", ");
        }
        writer.expr(args.get(2), 0).append(") AS ROW(");
        for (SqlType.Struct.Field f : kept) {
            writer.append(ident(f.name())).append(" ").append(castTypeName(f.type())).append(", ");
        }
        return writer.append(ident(name.value())).append(" ").append(castTypeName(added)).append("))");
    }

    // ==================================================================
    // Variant (Trino's JSON)
    // ==================================================================

    private static boolean isJson(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.JSON;
    }

    private static boolean isText(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.VARCHAR;
    }

    /** A value as JSON: text is a JSON string (DuckDB's to_json); a list, map or struct converts (a ROW of named
     *  fields is an object, measured); a scalar casts. */
    private SqlWriter.Piece toVariant(SqlExpr x) {
        if (isJson(x)) {
            return w -> w.expr(x, 0);
        }
        return w -> w.append("CAST(").expr(x, 0).append(" AS JSON)");
    }

    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() != 1) {
            throw new DialectCapability("toVariant of " + a.size() + " arguments reached Trino");
        }
        return writer.piece(toVariant(a.get(0)));
    }

    /** A key (a member, or NULL) or a 0-based index (-1 the last, past the end NULL) of a JSON value: through a cast
     *  to a map or an array of JSON, NULL when it is not one. */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        SqlExpr key = args.get(1);
        if (key instanceof SqlExpr.StringLit root && "$".equals(root.value())) {
            return writer.expr(args.get(0), 0);
        }
        if (isText(key) || key instanceof SqlExpr.StringLit) {
            return writer.append("element_at(TRY_CAST(").expr(args.get(0), 0).append(" AS MAP(VARCHAR, JSON)), ")
                    .expr(key, 0).append(")");
        }
        return writer.append("element_at(TRY_CAST(").expr(args.get(0), 0).append(" AS ARRAY(JSON)), CASE WHEN ")
                .expr(key, 0).append(" >= 0 THEN ").expr(key, 6).append(" + 1 ELSE ").expr(key, 0).append(" END)");
    }

    @Override
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        if (listElement(args.get(0)) != null) {
            return writer.expr(args.get(0), 0);
        }
        return writer.append("TRY_CAST(").expr(args.get(0), 0).append(" AS ARRAY(JSON))");
    }

    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        if (c.target() == SqlType.Scalar.TEMPORAL_TEXT || c.target() == SqlType.Scalar.DECIMAL_TEXT) {
            return super.variantAwareCast(writer, c);
        }
        // a zoned timestamp from text: ISO 8601's T, Z, +hhmm offsets and one-digit months, which a cast refuses,
        // as does the nanosecond parser the offset and the month (measured); this parser keeps milliseconds
        if (c.target() == SqlType.Scalar.TIMESTAMPTZ && isText(c.value())) {
            return writer.append("from_iso8601_timestamp(").expr(c.value(), 0).append(")");
        }
        // a timestamp from text: DuckDB's cast takes ISO 8601's T between the date and the time (Pure's DateTime text,
        // LiteralSpelling.dateTimeText), Trino's only a space ('Value cannot be cast to timestamp:
        // 2026-01-07T00:00:01.000000000', the database judge's tolerance cells, measured); as MariaDB's cast
        if (c.target() == SqlType.Scalar.TIMESTAMP && isText(c.value())) {
            return writer.append("CAST(replace(").expr(c.value(), 0).append(", 'T', ' ') AS ")
                    .append(castTypeName(c.target())).append(")");
        }
        if (c.target() == SqlType.Scalar.JSON) {
            return isText(c.value()) ? writer.append("json_parse(").expr(c.value(), 0).append(")")
                    : writer.piece(toVariant(c.value()));
        }
        if (isJson(c.value())) {
            // a member's value as text is its text (a JSON string casts unquoted); the whole value as text is its
            // JSON, as DuckDB's cast (a JSON object or array refuses a cast to VARCHAR, measured)
            boolean member = c.value() instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET;
            if (c.target() == SqlType.Scalar.VARCHAR) {
                if (!member) {
                    return writer.append("json_format(").expr(c.value(), 0).append(")");
                }
                return writer.append("(CASE WHEN is_json_scalar(").expr(c.value(), 0).append(") THEN CAST(")
                        .expr(c.value(), 0).append(" AS VARCHAR) ELSE json_format(").expr(c.value(), 0)
                        .append(") END)");
            }
            if (c.target() instanceof SqlType.Array || c.target() instanceof SqlType.Map) {
                return writer.append("TRY_CAST(").expr(c.value(), 0).append(" AS ").append(castTypeName(c.target()))
                        .append(")");
            }
        }
        // a double's text: Trino's cast is always scientific (1.5E0, measured); a double's JSON is Java's shortest
        // text (1.5, 2.0, 1.3421E-8; NULL is NULL, the value read once: format's %s writes NULL as 'null', and reading
        // it twice multiplied the Float spellings over it past what the server answers, measured), its exponent marked
        // 'e' as DuckDB's, which every Float spelling unfolds (NaN and Infinity are quoted JSON strings). A
        // Float literal is a DECIMAL by its digits (the numeric charter's Rule 1) but one of an extreme magnitude is
        // written in exponent form, a DOUBLE here: its text so too
        if (c.target() == SqlType.Scalar.VARCHAR
                && (typeOf(c.value()) == SqlType.Scalar.DOUBLE || c.value() instanceof SqlExpr.FloatLit)) {
            return writer.append("replace(json_format(CAST(").expr(c.value(), 0).append(" AS JSON)), 'E', 'e')");
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
        // over the empty collection the answer is NULL: spelled in its declared type, never Trino's unknown
        if (NULL_OVER_EMPTY.contains(c.fn()) && !a.isEmpty() && a.get(0) instanceof SqlExpr.NullLit
                && c.type() instanceof com.legend.sql.TypeFact.Typed t) {
            return writer.append("CAST(NULL AS ").append(castTypeName(t.type())).append(")");
        }
        return switch (c.fn()) {
            // ---- each function as this dialect spells it, by name (the dialect program's Phase 1, D1, 2026-10-13:
            // an exhaustive switch; a spelling several databases share is a named helper of the base)
            // the ANSI operators
            // two ROWs compare field by field, and a NULL field makes Trino's `=` NULL (measured): an instance's
            // absent property is a NULL field, and Pure's equality of two instances holds it equal (2026-10-20, the
            // parity phase: testEqualNonPrimitive and the identity tests), so ROWs compare null-safe
            case EQUAL -> rows(a) ? notDistinctFrom(writer, a) : infix(writer, c, parentPrec);
            case NOT_EQUAL -> rows(a) ? distinctFrom(writer, a) : infix(writer, c, parentPrec);
            case AND, OR, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES ->
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
            case PI -> writer.append("pi()");
            case CEILING -> bigintOf(writer, "ceil", c.args());
            case FLOOR -> bigintOf(writer, "floor", c.args());
            case SIGN -> bigintOf(writer, "sign", c.args());
            case LPAD -> padWithSpace(writer, "lpad", c.args());
            case RPAD -> padWithSpace(writer, "rpad", c.args());
            case UC_FIRST -> firstCharCased(writer, "upper", c.args(), parentPrec);
            case LC_FIRST -> firstCharCased(writer, "lower", c.args(), parentPrec);
            case TODAY -> writer.append("current_date");
            // Pure's divide is a Float division: both operands DOUBLE
            case DIVIDE -> divideAsDoubles(writer, c.args());
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
            case ASIN -> writer.function("asin", c.args());
            case ATAN -> writer.function("atan", c.args());
            case ATAN2 -> writer.function("atan2", c.args());
            case CBRT -> writer.function("cbrt", c.args());
            case CHR -> writer.function("chr", c.args());
            // DuckDB's coalesce casts a text operand beside a JSON one to JSON (an instance canon's empty key '[]'
            // beside a nested instance's canon object); Trino unifies no JSON with a varchar ('All COALESCE operands
            // must be the same type or coercible to a common type ... json and varchar(2)', channel B's zips of Pairs
            // of Pairs, measured 2026-10-20)
            case COALESCE -> a.stream().anyMatch(Trino::isJson) && a.stream().anyMatch(Trino::isText)
                    ? writer.append("coalesce(").join(a, ", ", (w, e) -> {
                        if (isText(e)) {
                            w.append("json_parse(").expr(e, 0).append(")");
                        } else {
                            w.expr(e, 0);
                        }
                    }).append(")")
                    : writer.function("coalesce", c.args());
            case COS -> writer.function("cos", c.args());
            case COSH -> writer.function("cosh", c.args());
            case DEGREES -> writer.function("degrees", c.args());
            case EXP -> writer.function("exp", c.args());
            case GREATEST -> writer.function("greatest", c.args());
            case LEAST -> writer.function("least", c.args());
            case LENGTH -> writer.function("length", c.args());
            case LEVENSHTEIN -> writer.function("levenshtein_distance", c.args());
            case LIST_FLATTEN -> writer.function("flatten", c.args());
            case LN -> writer.function("ln", c.args());
            case LOG10 -> writer.function("log10", c.args());
            case LOWER -> writer.function("lower", c.args());
            case MAP_KEYS -> writer.function("map_keys", c.args());
            case MAP_VALUES -> writer.function("map_values", c.args());
            case POW -> writer.function("power", c.args());
            case RADIANS -> writer.function("radians", c.args());
            case REPLACE -> writer.function("replace", c.args());
            case TRANSLATE -> writer.function("translate", c.args());
            case REVERSE_STRING, LIST_REVERSE -> writer.function("reverse", c.args());
            case SIN -> writer.function("sin", c.args());
            case SINH -> writer.function("sinh", c.args());
            case SQRT -> writer.function("sqrt", c.args());
            case SUBSTRING -> writer.function("substr", c.args());
            case TAN -> writer.function("tan", c.args());
            case TANH -> writer.function("tanh", c.args());
            case UPPER -> writer.function("upper", c.args());
            case STARTS_WITH -> writer.function("starts_with", c.args());
            case STRPOS -> writer.function("strpos", c.args());
            case MAP_FROM_ENTRIES -> writer.function("map_from_entries", c.args());

            // fail(message): the raise, its message the platform's envelope
            case ERROR -> error(writer, c);
            // NULL-ignoring, as DuckDB's concat (Trino's concat is NULL if any argument is)
            case CONCAT -> writer.append("concat_ws('', ").join(flattenConcat(a), ", ", this::asText).append(")");
            case CONCAT_JOIN -> writer.append("concat_ws('', ").join(a, ", ", this::asText).append(")");
            // integer division truncates toward zero, as DuckDB's //
            // a zero divisor is NULL, as DuckDB's: Trino folds a constant division by zero to its own error even in a
            // CASE's untaken arm (the platform's own raise, measured), and nullif breaks inside a lambda
            case INT_DIVIDE -> writer.append("(").expr(a.get(0), 6).append(" / ").piece(divisor(a.get(1))).append(")");
            case REM -> writer.append("MOD(").expr(a.get(0), 0).append(", ").piece(divisor(a.get(1))).append(")");
            case MOD -> writer.append("MOD(MOD(").expr(a.get(0), 0).append(", ").piece(divisor(a.get(1))).append(") + ")
                    .expr(a.get(1), 6).append(", ").piece(divisor(a.get(1))).append(")");
            case BOOL_TO_TEXT -> writer.append("CAST(").expr(a.get(0), 0).append(" AS VARCHAR)");
            case BIT_NOT -> writer.append("bitwise_not(CAST(").expr(a.get(0), 0).append(" AS BIGINT))");
            // floor without the BIGINT conversion
            case FLOOR_RAW -> writer.append("floor(").expr(a.get(0), 0).append(")");
            // the first character's code point (codepoint takes a VARCHAR(1), and refuses the empty one: Pure's is 0)
            case ASCII_CODE -> writer.append("(CASE WHEN length(").expr(a.get(0), 0).append(") = 0 THEN 0 ELSE")
                    .append(" codepoint(CAST(substr(").expr(a.get(0), 0).append(", 1, 1) AS VARCHAR(1))) END)");
            case COT -> writer.append("(1 / tan(").expr(a.get(0), 0).append("))");
            case LEFT -> writer.append("substr(").expr(a.get(0), 0).append(", 1, greatest(").expr(a.get(1), 0)
                    .append(", 0))");
            case RIGHT -> writer.append("(CASE WHEN ").expr(a.get(1), 0).append(" <= 0 THEN '' ELSE substr(")
                    .expr(a.get(0), 0).append(", greatest(length(").expr(a.get(0), 0).append(") - ").expr(a.get(1), 6)
                    .append(" + 1, 1)) END)");
            case ENDS_WITH -> writer.append("(substr(").expr(a.get(0), 0).append(", greatest(length(").expr(a.get(0), 0)
                    .append(") - length(").expr(a.get(1), 0).append(") + 1, 1)) = ").expr(a.get(1), 0)
                    .append(" AND length(").expr(a.get(0), 0).append(") >= length(").expr(a.get(1), 0).append("))");
            // array_join skips a NULL element: no string repeated is NULL, not ''
            case REPEAT_STR -> writer.append("(CASE WHEN ").expr(a.get(0), 0).append(" IS NULL THEN NULL ELSE")
                    .append(" array_join(repeat(").expr(a.get(0), 0).append(", greatest(").expr(a.get(1), 0)
                    .append(", 0)), '') END)");
            // DuckDB's split_part: the empty string past the end (Trino's is NULL)
            case SPLIT_PART -> writer.append("coalesce(split_part(").list(a).append("), '')");
            case ROUND_HALF_UP -> roundHalfUp(writer, c);

            // ---- strings
            case MATCHES -> writer.append("regexp_like(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
            case REGEXP_FULL_MATCH -> writer.append("regexp_like(").expr(a.get(0), 0).append(", ")
                    .piece(anchored(a.get(1))).append(")");
            case REGEXP_EXTRACT -> writer.append("regexp_extract(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                    .append(", ").piece(group(a)).append(")");
            case REGEXP_EXTRACT_ALL -> writer.append("regexp_extract_all(").expr(a.get(0), 0).append(", ")
                    .expr(a.get(1), 0).append(", ").piece(group(a)).append(")");
            // DuckDB replaces the first match unless 'g'; Trino replaces every one: the first alone is the match of
            // ^(.*?)(?:p), replaced by that prefix and r, r's group references shifted by one
            case REGEXP_REPLACE -> regexpReplace(writer, c);
            // split takes the delimiter as text
            case SPLIT -> writer.append("split(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
            case TRIM, LTRIM, RTRIM -> trim(writer, c);
            case ENCODE_BASE64 -> writer.append("to_base64(to_utf8(").expr(a.get(0), 0).append("))");
            case DECODE_BASE64 -> writer.append("from_utf8(from_base64(").expr(a.get(0), 0).append("))");
            case MD5 -> writer.append("lower(to_hex(md5(to_utf8(").expr(a.get(0), 0).append("))))");
            case SHA1 -> writer.append("lower(to_hex(sha1(to_utf8(").expr(a.get(0), 0).append("))))");
            case SHA256 -> writer.append("lower(to_hex(sha256(to_utf8(").expr(a.get(0), 0).append("))))");
            case GUID -> writer.append("CAST(uuid() AS VARCHAR)");
            case RANDOM -> writer.append("random()");
            case FORMAT -> writer.append("format(").list(a).append(")");
            case JARO_WINKLER -> writer.piece(jaroWinkler(a.get(0), a.get(1)));
            case CURRENT_USER_FN -> writer.append("current_user");

            // ---- temporal
            case NOW -> writer.append("CAST(current_timestamp AS TIMESTAMP(6))");
            case DATE_TRUNC_DAY -> writer.append("CAST(").expr(a.get(0), 0).append(" AS DATE)");
            case DATE_TRUNC -> dateTrunc(writer, c);
            case PARSE_DATE -> writer.append("CAST(").expr(a.get(0), 0).append(" AS TIMESTAMP(6))");
            case EXTRACT -> writer.piece(extract(a));
            case MAKE_DATE -> writer.append("date_add('day', ").expr(a.get(2), 0).append(" - 1, date_add('month', ")
                    .expr(a.get(1), 0).append(" - 1, date_add('year', ").expr(a.get(0), 0)
                    .append(" - 1970, DATE '1970-01-01')))");
            case MAKE_TIMESTAMP -> makeTimestamp(writer, c);
            // date_add has no microsecond unit: microseconds through the epoch's
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> addInterval(writer, c);
            case DATE_DIFF -> writer.piece(dateDiff(a));
            case TIME_BUCKET -> writer.piece(timeBucket(a));
            // the zone's wall time of the instant t (the platform's timestamps are UTC instants; a zoned one, a
            // parsed offset's, is its own instant: with_timezone takes only a naive one)
            case TIMEZONE -> timezone(writer, c);
            // Trino adds no date and time: the date-time is read from their texts (designed, not measured here)
            case DATE_AT_TIME -> writer.append("CAST(CAST(").expr(a.get(0), 0).append(" AS VARCHAR) || ' ' || CAST(")
                    .expr(a.get(1), 0).append(" AS VARCHAR) AS TIMESTAMP(6))");
            // DuckDB's epoch(ts) is DOUBLE seconds; epoch_ms truncates toward zero
            case EPOCH_SECONDS -> writer.append("to_unixtime(with_timezone(").expr(a.get(0), 0).append(", 'UTC'))");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability("a nanosecond epoch: Trino keeps"
                    + " microseconds (the Cypher island's nanosecond leg is DuckDB's)");
            case EPOCH_MS -> writer.append("CAST(truncate(to_unixtime(with_timezone(").expr(a.get(0), 0)
                    .append(", 'UTC')) * 1000) AS BIGINT)");
            case FROM_EPOCH_SECONDS -> writer.append("CAST(from_unixtime(").expr(a.get(0), 0)
                    .append(", 'UTC') AS TIMESTAMP(6))");
            case FROM_EPOCH_MS -> writer.append("CAST(from_unixtime(CAST(").expr(a.get(0), 0)
                    .append(" AS DOUBLE) / 1000, 'UTC') AS TIMESTAMP(6))");
            case DAYNAME -> writer.append("format_datetime(").expr(a.get(0), 0).append(", 'EEEE')");
            case MONTHNAME -> writer.append("format_datetime(").expr(a.get(0), 0).append(", 'MMMM')");
            case STRFTIME -> strftime(writer, c);
            case STRPTIME -> strptime(writer, c);

            // ---- lists whose first argument is no list
            case LIST_LENGTH -> writer.append("cardinality(").piece(list(a.get(0))).append(")");
            // DuckDB's range: [start, stop) by step (start 0, step 1 by default); Trino's sequence is inclusive and
            // refuses a step away from the stop
            case RANGE_FN -> rangeOf(writer, c);
            case REPEAT_VALUE -> writer.append("repeat(").expr(a.get(0), 0).append(", greatest(").expr(a.get(1), 0)
                    .append(", 0))");

            // ---- maps
            case MAP_EMPTY -> writer.append("MAP()");
            case MAP_FROM_LISTS -> writer.append("map(").list(a).append(")");
            // DuckDB's map_extract: a list of the key's value, empty when absent
            case MAP_EXTRACT -> writer.append("(CASE WHEN contains(map_keys(").expr(a.get(0), 0).append("), ")
                    .expr(a.get(1), 0).append(") THEN ARRAY[element_at(").expr(a.get(0), 0).append(", ")
                    .expr(a.get(1), 0).append(")] ELSE ARRAY[] END)");
            // a duplicate key keeps its LAST value, as DuckDB's (measured)
            case MAP_CONCAT -> writer.append("map_concat(").list(a).append(")");

            // ---- variant
            // in DuckDB's json_type vocabulary, which the lowering compares against
            case JSON_TYPE -> jsonType(writer, c);
            case JSON_ARRAY_LENGTH -> writer.append("json_array_length(").expr(a.get(0), 0).append(")");
            case JSON_PRETTY -> throw wall(c.fn(), "Trino has no JSON pretty-printer");
            case JSON_MERGE_PATCH -> throw wall(c.fn(), "Trino has no JSON merge patch");
        };
    }

    /** The {@code ERROR} arm of {@link #call}. */
    private SqlWriter error(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        writer.append("fail(chr(31) || ");
        if (a.size() > 1) {
            writer.expr(a.get(1), 0).append(" || chr(30) || ");
        }
        return writer.append("(").expr(a.get(0), 0).append(") || chr(31))");
    }

    /** The {@code ROUND_HALF_UP} arm of {@link #call}. */
    private SqlWriter roundHalfUp(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        writer.append("round(").expr(a.get(0), 0);
        if (a.size() > 1) {
            writer.append(", CAST(").expr(a.get(1), 0).append(" AS INTEGER)");
        }
        return writer.append(")");
    }

    /** The {@code TRIM}, {@code LTRIM}, {@code RTRIM} arm of {@link #call}. */
    private SqlWriter trim(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String fn = c.fn() == SqlFn.TRIM ? "trim" : c.fn() == SqlFn.LTRIM ? "ltrim" : "rtrim";
        if (a.size() == 1) {
            return writer.append(fn + "(").expr(a.get(0), 0).append(")");
        }
        return writer.append(fn + "(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
    }

    /** The {@code DATE_TRUNC} arm of {@link #call}. */
    private SqlWriter dateTrunc(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        com.legend.sql.DatePart part = part(a.get(0));
        boolean toDate = switch (part) {
            case MONTH, YEAR, WEEK, QUARTER -> true;
            case DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> false;
            case NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_trunc to " + part
                    + " has no Trino spelling");
        };
        writer.append(toDate ? "CAST(date_trunc(" : "date_trunc(").append(stringLit(intervalUnit(part))).append(", ")
                .expr(a.get(1), 0).append(")");
        return toDate ? writer.append(" AS DATE)") : writer;
    }

    /** The {@code MAKE_TIMESTAMP} arm of {@link #call}. */
    private SqlWriter makeTimestamp(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() != 6) {
            throw wall(c.fn(), "make_timestamp of " + a.size() + " arguments");
        }
        // its text, cast (date_add has no microsecond unit): the seconds to six digits
        return writer.append("CAST(format('%04d-%02d-%02d %02d:%02d:%09.6f', ").list(a.subList(0, 5))
                .append(", CAST(").expr(a.get(5), 0).append(" AS DOUBLE)) AS TIMESTAMP(6))");
    }

    /** The {@code ADD_INTERVAL}, {@code ADD_INTERVAL_TEMPORAL} arm of {@link #call}. */
    private SqlWriter addInterval(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        com.legend.sql.DatePart part = part(a.get(0));
        String unit = intervalUnit(part);
        if (part == com.legend.sql.DatePart.MICROSECOND) {
            return writer.piece(fromMicros(w -> w.piece(micros(v -> v.expr(a.get(2), 0))).append(" + ")
                    .expr(a.get(1), 6)));
        }
        return writer.append("date_add(").append(stringLit(unit)).append(", ").expr(a.get(1), 0).append(", ")
                .expr(a.get(2), 0).append(")");
    }

    /** The {@code TIMEZONE} arm of {@link #call}. */
    private SqlWriter timezone(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        writer.append("CAST(at_timezone(");
        if (typeOf(a.get(1)) == SqlType.Scalar.TIMESTAMPTZ) {
            writer.expr(a.get(1), 0);
        } else {
            writer.append("with_timezone(").expr(a.get(1), 0).append(", 'UTC')");
        }
        return writer.append(", ").piece(zone(a.get(0))).append(") AS TIMESTAMP(6))");
    }

    /** The {@code STRFTIME} arm of {@link #call}: {@code format_datetime}, whose Joda letters carry no digit finer
     *  than a millisecond (measured 2026-10-19, Trino 483: {@code 'SSSSSS'} of {@code 13:11:11.338001} is
     *  {@code 338000}), so a microsecond or nanosecond part is the value's own six digits, the tail of its
     *  {@code TIMESTAMP(6)} text (a nanosecond part padded with {@code 000}, as the Joda form was), the parts around
     *  it {@code format_datetime}'s. */
    private SqlWriter strftime(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit fl)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        List<SqlWriter.Piece> pieces = new ArrayList<>();
        List<com.legend.sql.DateFmt> run = new ArrayList<>();
        for (com.legend.sql.DateFmt p : fl.parts()) {
            if (p == com.legend.sql.DateFmt.Part.SUBSEC_MICRO || p == com.legend.sql.DateFmt.Part.SUBSEC_NANO) {
                if (!run.isEmpty()) {
                    pieces.add(formatDatetime(a.get(0), new SqlExpr.FormatLit(run)));
                    run = new ArrayList<>();
                }
                pieces.add(w -> w.append("substr(CAST(CAST(").expr(a.get(0), 0)
                        .append(" AS TIMESTAMP(6)) AS VARCHAR), -6)"));
                if (p == com.legend.sql.DateFmt.Part.SUBSEC_NANO) {
                    pieces.add(w -> w.append("'000'"));
                }
            } else {
                run.add(p);
            }
        }
        if (!run.isEmpty() || pieces.isEmpty()) {
            pieces.add(formatDatetime(a.get(0), new SqlExpr.FormatLit(run)));
        }
        if (pieces.size() == 1) {
            return writer.piece(pieces.get(0));
        }
        return writer.append("concat(").join(pieces, ", ", SqlWriter::piece).append(")");
    }

    /** {@code format_datetime(x, format)}. */
    private static SqlWriter.Piece formatDatetime(SqlExpr x, SqlExpr.FormatLit format) {
        return w -> w.append("format_datetime(").expr(x, 0).append(", ").expr(format, 0).append(")");
    }

    /** The {@code STRPTIME} arm of {@link #call}. */
    private SqlWriter strptime(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        return writer.append("CAST(parse_datetime(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                .append(") AS TIMESTAMP(6))");
    }

    /** The {@code JSON_TYPE} arm of {@link #call}. */
    private SqlWriter jsonType(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlWriter.Piece s = w -> w.append("json_format(").expr(a.get(0), 0).append(")");
        return writer.append("(CASE WHEN ").expr(a.get(0), 0).append(" IS NULL THEN NULL WHEN ").piece(s)
                .append(" = 'null' THEN 'NULL' WHEN ").piece(s).append(" IN ('true', 'false') THEN 'BOOLEAN'")
                .append(" WHEN substr(").piece(s).append(", 1, 1) = '\"' THEN 'VARCHAR' WHEN substr(").piece(s)
                .append(", 1, 1) = '[' THEN 'ARRAY' WHEN substr(").piece(s)
                .append(", 1, 1) = '{' THEN 'OBJECT' WHEN regexp_like(").piece(s)
                .append(", '^-?[0-9]+$') THEN 'BIGINT' ELSE 'DOUBLE' END)");
    }

    /** DuckDB's range: [start, stop) by step, as a filtered sequence. */
    private SqlWriter rangeOf(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlWriter.Piece start = a.size() == 1 ? w -> w.append("BIGINT '0'") : w -> w.append("CAST(")
                .expr(a.get(0), 0).append(" AS BIGINT)");
        SqlWriter.Piece stop = w -> w.append("CAST(").expr(a.get(a.size() == 1 ? 0 : 1), 0).append(" AS BIGINT)");
        SqlWriter.Piece step = a.size() > 2 ? w -> w.append("CAST(").expr(a.get(2), 0).append(" AS BIGINT)")
                : w -> w.append("BIGINT '1'");
        return writer.append("(CASE WHEN (").piece(stop).append(" - ").piece(start).append(") * ").piece(step)
                .append(" > 0 THEN filter(sequence(").piece(start).append(", ").piece(stop).append(", ")
                .piece(step).append("), \"__g\" -> \"__g\" <> ").piece(stop)
                .append(") ELSE CAST(ARRAY[] AS ARRAY(BIGINT)) END)");
    }

    /** DuckDB's regexp_replace on Trino: the first match alone unless 'g' (the call arm's comment says how). */
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

    /** A CONCAT argument as text: concat_ws takes strings only. */
    private void asText(SqlWriter w, SqlExpr e) {
        if (isText(e) || e instanceof SqlExpr.StringLit) {
            w.expr(e, 0);
        } else {
            w.append("CAST(").expr(e, 0).append(" AS VARCHAR)");
        }
    }

    /**
     * Jaro-Winkler similarity, exactly DuckDB's: Trino has no Jaro-Winkler function. Databricks' construction
     * (Databricks.jaroWinkler, measured equal to DuckDB's on 1,585 pairs) in Trino's spelling: {@code reduce} for
     * Spark's {@code aggregate}, a typed ROW for a named struct, {@code rpad} for {@code repeat}, a flag set by
     * concatenation for {@code overlay}. The arguments are bound once by a one-element transform; an aggregate over
     * {@code t}'s positions matches each character in its window ({@code f} flags {@code p}'s matched characters,
     * {@code tm} is {@code t}'s matched characters in order); its finish counts the transpositions along {@code p}
     * ({@code n} matched so far, {@code x} unequal to {@code tm}'s character in the same place) and scores, in DOUBLE
     * in DuckDB's order of operations.
     */
    private static SqlWriter.Piece jaroWinkler(SqlExpr p, SqlExpr t) {
        String o = "\"__jo\"";
        String s = "\"__js\"";
        String u = "\"__ju\"";
        String v = "\"__jv\"";
        String j = "\"__jj\"";
        String i = "\"__ji\"";
        String ftm = "ROW(f VARCHAR, tm VARCHAR)";
        String ftmk = "ROW(f VARCHAR, tm VARCHAR, k BOOLEAN)";
        String nx = "ROW(n BIGINT, x BIGINT)";
        String lp = "length(" + o + ".p)";
        String lt = "length(" + o + ".t)";
        String window = "greatest(0, greatest(" + lp + ", " + lt + ") / 2 - 1)";
        String m = "CAST(" + v + ".n AS DOUBLE)";
        String jaro = "((" + m + " / CAST(" + lp + " AS DOUBLE) + " + m + " / CAST(" + lt + " AS DOUBLE) + (" + m
                + " - CAST(" + v + ".x / 2 AS DOUBLE)) / " + m + ") / CAST(3 AS DOUBLE))";
        List<String> prefix = new ArrayList<>();
        for (int k = 1; k <= 4; k++) {
            prefix.add("CASE WHEN " + k + " <= least(" + lp + ", " + lt + ") AND substr(" + o + ".p, 1, " + k
                    + ") = substr(" + o + ".t, 1, " + k + ") THEN 1 ELSE 0 END");
        }
        String score = "CASE WHEN " + v + ".n = 0 THEN CAST(0 AS DOUBLE) WHEN " + jaro + " > CAST(0.7 AS DOUBLE) THEN "
                + jaro + " + CAST(" + String.join(" + ", prefix) + " AS DOUBLE) * CAST(0.1 AS DOUBLE) * (CAST(1 AS"
                + " DOUBLE) - " + jaro + ") ELSE " + jaro + " END";
        String take = "reduce(sequence(greatest(1, " + j + " - " + window + "), " + j + " + " + window + "), CAST(ROW("
                + s + ".f, " + s + ".tm, false) AS " + ftmk + "), (" + u + ", " + i + ") -> CASE WHEN NOT " + u
                + ".k AND substr(" + u + ".f, " + i + ", 1) = '0' AND substr(" + o + ".p, " + i + ", 1) = substr(" + o
                + ".t, " + j + ", 1) THEN CAST(ROW(concat(substr(" + u + ".f, 1, " + i + " - 1), '1', substr(" + u
                + ".f, " + i + " + 1)), concat(" + u + ".tm, substr(" + o + ".t, " + j + ", 1)), true) AS " + ftmk
                + ") ELSE " + u + " END, " + u + " -> CAST(ROW(" + u + ".f, " + u + ".tm) AS " + ftm + "))";
        String counted = "reduce(sequence(1, " + lp + "), CAST(ROW(0, 0) AS " + nx + "), (" + v + ", " + i
                + ") -> CASE WHEN substr(" + s + ".f, " + i + ", 1) = '1' THEN CAST(ROW(" + v + ".n + 1, " + v
                + ".x + CASE WHEN substr(" + o + ".p, " + i + ", 1) = substr(" + s + ".tm, " + v + ".n + 1, 1) THEN 0"
                + " ELSE 1 END) AS " + nx + ") ELSE " + v + " END, " + v + " -> " + score + ")";
        String body = "CASE WHEN " + o + ".p IS NULL OR " + o + ".t IS NULL THEN CAST(NULL AS DOUBLE) WHEN " + lp
                + " = 0 OR " + lt + " = 0 THEN CAST(0 AS DOUBLE) ELSE reduce(sequence(1, " + lt + "), CAST(ROW(rpad('',"
                + " " + lp + ", '0'), '') AS " + ftm + "), (" + s + ", " + j + ") -> " + take + ", " + s + " -> "
                + counted + ") END";
        return w -> w.append("element_at(transform(ARRAY[CAST(ROW(").expr(p, 0).append(", ").expr(t, 0)
                .append(") AS ROW(p VARCHAR, t VARCHAR))], " + o + " -> " + body + "), 1)");
    }

    /** A zone: an abbreviation Trino does not know as its tz database's fixed-offset zone (EST is -05:00 all year,
     *  as DuckDB's and Java's; Trino refuses it: "'EST' is not a valid time zone", measured). */
    private static SqlWriter.Piece zone(SqlExpr z) {
        if (z instanceof SqlExpr.StringLit s && FIXED_ZONES.containsKey(s.value())) {
            return w -> w.append("'" + FIXED_ZONES.get(s.value()) + "'");
        }
        return w -> w.expr(z, 0);
    }

    private static final java.util.Map<String, String> FIXED_ZONES = java.util.Map.of(
            "EST", "-05:00", "MST", "-07:00", "HST", "-10:00");

    /** {@code d}, NULL where it is zero. */
    private static SqlWriter.Piece divisor(SqlExpr d) {
        return w -> w.append("CASE WHEN ").expr(d, 0).append(" = 0 THEN NULL ELSE ").expr(d, 0).append(" END");
    }

    private static DialectCapability wall(SqlFn fn, String why) {
        return new DialectCapability(fn + " reached Trino: " + why);
    }

    /** A regexp extraction's group: the named one, else 0. */
    private static SqlWriter.Piece group(List<SqlExpr> a) {
        return a.size() > 2 ? w -> w.expr(a.get(2), 0) : w -> w.append("0");
    }

    /** The pattern anchored at both ends. */
    private SqlWriter.Piece anchored(SqlExpr p) {
        if (p instanceof SqlExpr.StringLit lit) {
            String s = stringLit("^(?:" + lit.value() + ")$");
            return w -> w.append(s);
        }
        return w -> w.append("concat('^(?:', ").expr(p, 0).append(", ')$')");
    }

    /** DuckDB's replacement text ({@code \1} a group, {@code $} itself) as Trino's ({@code $1}, {@code \$}), each
     *  group number raised by {@code shift}. */
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

    /** A date part as Trino's function of it; a Pure integer (seconds truncated). Trino's day_of_week is Monday 1 ..
     *  Sunday 7 (ISO). */
    private SqlWriter.Piece extract(List<SqlExpr> a) {
        com.legend.sql.DatePart part = part(a.get(0));
        String fn = switch (part) {
            case YEAR -> "year";
            case QUARTER -> "quarter";
            case MONTH -> "month";
            case WEEK -> "week";
            case DAY -> "day";
            case HOUR -> "hour";
            case MINUTE -> "minute";
            case SECOND -> "second";
            case DAY_OF_YEAR -> "day_of_year";
            case ISO_DAY_OF_WEEK -> "day_of_week";
            case MILLISECOND, MICROSECOND, NANOSECOND -> throw new DialectCapability("date part " + part
                    + " has no probed Trino spelling");
        };
        return w -> w.append("CAST(" + fn + "(").expr(a.get(1), 0).append(") AS BIGINT)");
    }

    /** A unit as Trino's date_add and date_trunc name it (date_add has no microsecond: measured "'microsecond' is not
     *  a valid TIMESTAMP field", so that one is added through the epoch's microseconds). */
    private static String intervalUnit(com.legend.sql.DatePart unit) {
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
            case NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("interval unit " + unit
                    + " has no Trino spelling");
        };
    }

    /** DuckDB's date_diff counts BOUNDARIES crossed (Trino's date_diff counts whole units): year, quarter and month
     *  by the calendar, day by dates, the sub-day parts by the floored epoch in their unit. */
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
            case WEEK -> w -> w.append("floor(CAST(date_diff('day', DATE '1969-12-29', CAST(").piece(to)
                    .append(" AS DATE)) AS DOUBLE) / 7) - floor(CAST(date_diff('day', DATE '1969-12-29', CAST(")
                    .piece(from).append(" AS DATE)) AS DOUBLE) / 7)");
            case DAY -> w -> w.append("date_diff('day', CAST(").piece(from).append(" AS DATE), CAST(").piece(to)
                    .append(" AS DATE))");
            case HOUR -> epochBoundaries(from, to, 3600000000L);
            case MINUTE -> epochBoundaries(from, to, 60000000L);
            case SECOND -> epochBoundaries(from, to, 1000000L);
            case MILLISECOND -> epochBoundaries(from, to, 1000L);
            case MICROSECOND -> w -> w.append("(").piece(micros(to)).append(" - ").piece(micros(from)).append(")");
            case NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_diff part " + part
                    + " has no probed Trino spelling");
        };
        return w -> w.append("CAST(").piece(body).append(" AS BIGINT)");
    }

    /** {@code t}'s microseconds since 1970 (BIGINT): Trino's date_diff stops at milliseconds, so its whole seconds
     *  (floored) and its six fraction digits, read from its text (measured: -500000 for 1969-12-31 23:59:59.5). */
    private static SqlWriter.Piece micros(SqlWriter.Piece t) {
        return w -> w.append("(date_diff('second', TIMESTAMP '1970-01-01 00:00:00', date_trunc('second', CAST(").piece(t)
                .append(" AS TIMESTAMP(6)))) * 1000000 + CAST(substr(CAST(CAST(").piece(t)
                .append(" AS TIMESTAMP(6)) AS VARCHAR), 21, 6) AS BIGINT))");
    }

    /** The naive UTC timestamp {@code x} microseconds after 1970 (through from_unixtime_nanos: date_add has no
     *  microsecond unit). */
    private static SqlWriter.Piece fromMicros(SqlWriter.Piece x) {
        return w -> w.append("CAST(at_timezone(from_unixtime_nanos((").piece(x).append(") * 1000), 'UTC') AS TIMESTAMP(6))");
    }

    /** The floored epoch (in microseconds per unit) of {@code to} less {@code from}'s: floor, before 1970 too. */
    private static SqlWriter.Piece epochBoundaries(SqlWriter.Piece from, SqlWriter.Piece to, long perUnit) {
        return w -> w.append("(floor(CAST(").piece(micros(to)).append(" AS DOUBLE) / " + perUnit
                + ") - floor(CAST(").piece(micros(from)).append(" AS DOUBLE) / " + perUnit + "))");
    }

    /** DuckDB's time_bucket: fixed units binned from 1970-01-01 (weeks from the Monday 1969-12-29) in microseconds,
     *  floored; months and years by the month index since 1970-01, floored to the bucket's size in months. */
    private SqlWriter.Piece timeBucket(List<SqlExpr> a) {
        com.legend.sql.DatePart unit = part(a.get(0));
        Long micros = bucketMicros(unit);
        SqlWriter.Piece t = w -> w.append("CAST(").expr(a.get(2), 0).append(" AS TIMESTAMP(6))");
        SqlWriter.Piece n = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS BIGINT)");
        if (micros != null) {
            long origin = unit == com.legend.sql.DatePart.WEEK ? -3L * 86400000000L : 0L;
            return w -> w.piece(fromMicros(v -> v.append("CAST(floor(CAST(").piece(micros(t)).append(" - (" + origin
                    + ") AS DOUBLE) / (").piece(n).append(" * " + micros + ")) AS BIGINT) * ").piece(n)
                    .append(" * " + micros + " + (" + origin + ")")));
        }
        Integer months = unit == com.legend.sql.DatePart.MONTH ? Integer.valueOf(1)
                : unit == com.legend.sql.DatePart.YEAR ? Integer.valueOf(12) : null;
        if (months == null) {
            throw new DialectCapability("a " + unit + " time bucket has no Trino spelling");
        }
        SqlWriter.Piece size = w -> w.append("(").piece(n).append(" * " + months + ")");
        return w -> w.append("CAST(date_add('month', CAST(floor(CAST((year(").piece(t)
                .append(") - 1970) * 12 + month(").piece(t).append(") - 1 AS DOUBLE) / ").piece(size).append(") * ")
                .piece(size).append(" AS BIGINT), DATE '1970-01-01') AS TIMESTAMP(6))");
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

    /** Pure's hashCode is a signed 64-bit integer: xxhash64 of the value's text, read as one. A list's or a row's
     *  text is its JSON (no VARCHAR cast takes one); no value (an empty collection's NULL) hashes the empty text, a
     *  hash still (Pure's hashCode([]) is one). */
    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        writer.append("from_big_endian_64(xxhash64(to_utf8(coalesce(");
        if (a.size() == 1) {
            hashText(writer, a.get(0));
        } else {
            writer.append("concat_ws(chr(31), ").join(a, ", ", this::hashText).append(")");
        }
        return writer.append(", ''))))");
    }

    private void hashText(SqlWriter w, SqlExpr e) {
        SqlType t = typeOf(e);
        if (t instanceof SqlType.Array || t instanceof SqlType.Struct || t instanceof SqlType.Map) {
            w.append("json_format(CAST(").expr(e, 0).append(" AS JSON))");
        } else {
            asText(w, e);
        }
    }

    /** Pure round is half-EVEN; Trino's round is half away from zero (doubles included): an exact .5 of the scaled
     *  value steps to its even neighbour. */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece scale = a.size() > 1 ? w -> w.append("CAST(").expr(a.get(1), 0).append(" AS INTEGER)")
                : w -> w.append("0");
        boolean exact = !(typeOf(a.get(0)) == SqlType.Scalar.DOUBLE || a.get(0) instanceof SqlExpr.FloatLit);
        String ten = exact ? "CAST(10 AS DECIMAL(38, 0))" : "CAST(10 AS DOUBLE)";
        SqlWriter.Piece p = w -> w.append("power(" + ten + ", ").piece(scale).append(")");
        SqlWriter.Piece v = w -> w.append("(").expr(a.get(0), 7).append(" * ").piece(p).append(")");
        if (a.size() == 1) {
            return writer.append("(CASE WHEN ").expr(a.get(0), 6).append(" - floor(").expr(a.get(0), 0)
                    .append(") = 0.5 THEN (CASE WHEN mod(floor(").expr(a.get(0), 0).append("), 2) = 0 THEN floor(")
                    .expr(a.get(0), 0).append(") ELSE floor(").expr(a.get(0), 0).append(") + 1 END) ELSE round(")
                    .expr(a.get(0), 0).append(") END)");
        }
        return writer.append("(CASE WHEN ").piece(v).append(" - floor(").piece(v)
                .append(") = 0.5 THEN (CASE WHEN mod(floor(").piece(v).append("), 2) = 0 THEN floor(").piece(v)
                .append(") ELSE floor(").piece(v).append(") + 1 END) ELSE round(").piece(v).append(") END / ").piece(p)
                .append(")");
    }

    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        SqlWriter.Piece x = w -> w.append("CAST(").expr(a.get(0), 0).append(" AS BIGINT)");
        SqlWriter.Piece y = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS BIGINT)");
        SqlWriter.Piece shift = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS INTEGER)");
        return switch (fnName) {
            case BIT_AND -> writer.append("bitwise_and(").piece(x).append(", ").piece(y).append(")");
            case BIT_OR -> writer.append("bitwise_or(").piece(x).append(", ").piece(y).append(")");
            case BIT_XOR -> writer.append("bitwise_xor(").piece(x).append(", ").piece(y).append(")");
            case BIT_SHIFT_LEFT -> writer.append("bitwise_left_shift(").piece(x).append(", ").piece(shift).append(")");
            case BIT_SHIFT_RIGHT -> writer.append("bitwise_right_shift_arithmetic(").piece(x).append(", ").piece(shift)
                    .append(")");
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
    }

    // ==================================================================
    // Aggregates and windows
    // ==================================================================

    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM -> aggregate(unordered(writer, r), "SUM", r);
            case COUNT -> aggregate(unordered(writer, r), "COUNT", r);
            case AVG -> aggregate(unordered(writer, r), "AVG", r);
            case MIN -> aggregate(unordered(writer, r), "MIN", r);
            case MAX -> aggregate(unordered(writer, r), "MAX", r);
            case ANY_VALUE -> writer.append("arbitrary(").list(r.args()).append(")");
            case STDDEV_SAMP -> aggregate(unordered(writer, r), "STDDEV_SAMP", r);
            case STDDEV_POP -> aggregate(unordered(writer, r), "STDDEV_POP", r);
            case VAR_SAMP -> aggregate(unordered(writer, r), "VAR_SAMP", r);
            case VAR_POP -> aggregate(unordered(writer, r), "VAR_POP", r);
            case MEDIAN, QUANTILE_CONT, QUANTILE_DISC -> writer.piece(quantile(r, collected(r.args().get(0), null)));
            case MODE -> writer.piece(mode(counted(r.args().get(0), null)));
            case STRING_AGG -> listagg(writer, r);
            case LIST -> arrayAgg(writer, r);
            case CORR, COVAR_SAMP, COVAR_POP -> writer.piece(covariance(r, null));
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

    /** {@code writer}, for an aggregate Trino cannot order: an ordered one is refused. */
    private static SqlWriter unordered(SqlWriter writer, SqlAgg.Reducer r) {
        if (!r.orderBy().isEmpty()) {
            throw new DialectCapability("an ordered " + r.fn() + " reached Trino");
        }
        return writer;
    }

    /** The {@code STRING_AGG} arm of {@link #reducer}: {@code listagg(...) WITHIN GROUP (ORDER BY ...)}; one of no order
     *  {@code array_join(array_agg(...), sep)}, the values in the order they come, as DuckDB's {@code string_agg} and
     *  H2's {@code listagg} read them (step 9, 2026-10-19: listagg needs an order, and the values' own sorted a
     *  collection's TDS text lines -- {@code reverse}, {@code sort} by a key, {@code [10, 2]} as text -- where the rows
     *  come in the collection's order). Both skip a null value and answer null over no rows. */
    private SqlWriter listagg(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 2) {
            throw new DialectCapability("a STRING_AGG of " + r.args().size() + " arguments reached Trino");
        }
        if (r.orderBy().isEmpty()) {
            return writer.append("array_join(array_agg(").append(r.distinct() ? "DISTINCT " : "")
                    .expr(r.args().get(0), 0).append("), ").expr(r.args().get(1), 0).append(")");
        }
        return writer.append("listagg(").append(r.distinct() ? "DISTINCT " : "").list(r.args())
                .append(") WITHIN GROUP (ORDER BY ").join(r.orderBy(), ", ", this::aggregateOrderKey).append(")");
    }

    /** The {@code LIST} arm of {@link #reducer}. */
    private SqlWriter arrayAgg(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.distinct() || r.args().size() != 1) {
            throw new DialectCapability("a distinct LIST aggregate reached Trino");
        }
        writer.append("array_agg(").expr(r.args().get(0), 0);
        if (!r.orderBy().isEmpty()) {
            writer.append(" ORDER BY ").join(r.orderBy(), ", ", this::listOrderKey);
        }
        return writer.append(")");
    }

    /** {@code max_by(value, key)} / {@code min_by}: the value at the key's greatest (least) row. */
    private static SqlWriter byExtreme(SqlWriter writer, String name, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct() || !r.orderBy().isEmpty()) {
            throw new DialectCapability(r.fn() + " of this shape reached Trino");
        }
        return writer.append(name).append("(").list(r.args()).append(")");
    }

    /**
     * A covariance, or a correlation: Trino's corr and covar_* answer a REAL over anything but DOUBLE (decimals and
     * integers coerce to REAL, measured), so their arguments are DOUBLE; and Trino's corr of [1, 2] and [10, 20] is
     * 0.9999999999999999 where DuckDB's is 1.0 (measured): DuckDB's correlation is the population covariance over the
     * product of the population deviations, spelled so. Over {@code window}'s frame when it is one, each aggregate.
     */
    private SqlWriter.Piece covariance(SqlAgg.Reducer r, @com.legend.base.Nullable SqlExpr.WindowCall window) {
        if (!r.orderBy().isEmpty() || r.distinct() || r.args().size() != 2) {
            throw new DialectCapability("an ordered, distinct or other-shaped " + r.fn() + " reached Trino");
        }
        SqlWriter.Piece x = w -> w.append("CAST(").expr(r.args().get(0), 0).append(" AS DOUBLE)");
        SqlWriter.Piece y = w -> w.append("CAST(").expr(r.args().get(1), 0).append(" AS DOUBLE)");
        java.util.function.BiFunction<String, List<SqlWriter.Piece>, SqlWriter.Piece> agg = (fn, args) -> w -> {
            w.append(fn + "(").join(args, ", ", SqlWriter::piece).append(")");
            if (window != null) {
                over(w, window);
            }
        };
        return switch (r.fn()) {
            case COVAR_SAMP -> agg.apply("covar_samp", List.of(x, y));
            case COVAR_POP -> agg.apply("covar_pop", List.of(x, y));
            default -> w -> w.append("(").piece(agg.apply("covar_pop", List.of(x, y))).append(" / (")
                    .piece(agg.apply("stddev_pop", List.of(x))).append(" * ")
                    .piece(agg.apply("stddev_pop", List.of(y))).append("))");
        };
    }

    /** A key of a collected list's order: NULLs where the key says, else where they sort (largest). */
    private void listOrderKey(SqlWriter w, SqlSelect.SortKey k) {
        w.expr(k.expr(), 0).append(k.ascending() ? " ASC" : " DESC");
        boolean nullsFirst = k.nullOrder() != null ? k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                : !k.ascending();
        w.append(nullsFirst ? " NULLS FIRST" : " NULLS LAST");
    }

    /**
     * A quantile of the group's values (DuckDB's quantile_cont and quantile_disc, the median's 0.5): Trino has only
     * an approximate percentile. The group's present values, sorted ({@code array_sort(array_agg(x))}, NULLs last), are
     * bound once by a one-element transform, and read: the continuous one interpolates between the values at
     * {@code floor((n-1)q)} and {@code ceil((n-1)q)} (0-based), the discrete one is the value at
     * {@code max(ceil(nq) - 1, 0)}; NULL over no value. {@code q} is a constant ({@code fixed}) or an expression.
     */
    private SqlWriter.Piece quantile(SqlAgg.Reducer r, SqlWriter.Piece values) {
        if (r.fn() == SqlAgg.Fn.MEDIAN) {
            return quantile(values, null, 0.5, true, r);
        }
        if (r.args().size() != 2 || r.distinct() || r.orderBy().size() > 1) {
            throw new DialectCapability(r.fn() + " of this shape reached Trino");
        }
        boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
        SqlExpr q = desc ? SqlExpr.Call.of(SqlFn.MINUS, new SqlExpr.FloatLit(1.0), r.args().get(1)) : r.args().get(1);
        return quantile(values, q, 0, r.fn() == SqlAgg.Fn.QUANTILE_CONT, r);
    }

    /** The values a quantile reads: {@code array_agg(x)}, over {@code window}'s frame when it is one. */
    private SqlWriter.Piece collected(SqlExpr x, @com.legend.base.Nullable SqlExpr.WindowCall window) {
        return w -> {
            w.append("array_agg(").expr(x, 0).append(")");
            if (window != null) {
                over(w, window);
            }
        };
    }

    /** The value counts a mode reads: {@code histogram(x)}, over {@code window}'s frame when it is one. */
    private SqlWriter.Piece counted(SqlExpr x, @com.legend.base.Nullable SqlExpr.WindowCall window) {
        return w -> {
            w.append("histogram(").expr(x, 0).append(")");
            if (window != null) {
                over(w, window);
            }
        };
    }

    private SqlWriter.Piece quantile(SqlWriter.Piece values, @com.legend.base.Nullable SqlExpr q, double fixed,
            boolean cont, SqlAgg.Reducer r) {
        SqlWriter.Piece qp = q == null ? w -> w.append("CAST(" + fixed + " AS DOUBLE)")
                : w -> w.append("CAST(").expr(q, 0).append(" AS DOUBLE)");
        String s = "\"__qs\"";
        String n = "cardinality(" + s + ")";
        SqlType type = r.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
        // the 0-based position (n - 1) q, parenthesized whole
        SqlWriter.Piece pos = w -> w.append("((" + n + " - 1) * ").piece(qp).append(")");
        return w -> {
            w.append("element_at(transform(ARRAY[filter(array_sort(").piece(values)
                    .append("), \"__x\" -> \"__x\" IS NOT NULL)], " + s + " -> CASE WHEN " + n + " = 0 THEN NULL ");
            if (cont) {
                SqlWriter.Piece lo = v -> v.append("CAST(element_at(" + s + ", CAST(floor(").piece(pos)
                        .append(") AS BIGINT) + 1) AS DOUBLE)");
                SqlWriter.Piece hi = v -> v.append("CAST(element_at(" + s + ", CAST(ceiling(").piece(pos)
                        .append(") AS BIGINT) + 1) AS DOUBLE)");
                w.append("ELSE ").piece(lo).append(" + (").piece(pos).append(" - floor(").piece(pos).append(")) * (")
                        .piece(hi).append(" - ").piece(lo).append(") END), 1)");
            } else {
                w.append("ELSE ");
                if (type != null) {
                    w.append("CAST(");
                }
                w.append("element_at(" + s + ", CAST(greatest(ceiling(" + n + " * ").piece(qp)
                        .append(") - 1, 0) AS BIGINT) + 1)");
                if (type != null) {
                    w.append(" AS " + castTypeName(type) + ")");
                }
                w.append(" END), 1)");
            }
        };
    }

    /** The group's most frequent value (DuckDB's mode): the value counts ({@code histogram}), the greatest count
     *  first, a tie by the smaller value; NULL over no value. */
    private SqlWriter.Piece mode(SqlWriter.Piece counts) {
        return w -> w.append("element_at(array_sort(map_entries(").piece(counts)
                .append("), (\"__l\", \"__r\") -> CASE WHEN \"__l\"[2] > \"__r\"[2] THEN -1 WHEN \"__l\"[2] < \"__r\"[2]")
                .append(" THEN 1 WHEN \"__l\"[1] < \"__r\"[1] THEN -1 WHEN \"__l\"[1] > \"__r\"[1] THEN 1 ELSE 0 END), 1)[1]");
    }

    /** A windowed median, quantile or mode reads the frame's values as its grouped form reads the group's; an average
     *  is a DOUBLE; a value or ranking function's offset is an integer literal. */
    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        if (w.fn() instanceof SqlAgg.Reducer r && (r.fn() == SqlAgg.Fn.MEDIAN || r.fn() == SqlAgg.Fn.QUANTILE_CONT
                || r.fn() == SqlAgg.Fn.QUANTILE_DISC || r.fn() == SqlAgg.Fn.MODE)) {
            return writer.piece(r.fn() == SqlAgg.Fn.MODE ? mode(counted(r.args().get(0), w))
                    : quantile(r, collected(r.args().get(0), w)));
        }
        if (w.fn() instanceof SqlAgg.Reducer r && (r.fn() == SqlAgg.Fn.CORR || r.fn() == SqlAgg.Fn.COVAR_SAMP
                || r.fn() == SqlAgg.Fn.COVAR_POP)) {
            return writer.piece(covariance(r, w));
        }
        boolean cast = w.fn() instanceof SqlAgg.Reducer r && r.fn() == SqlAgg.Fn.AVG;
        if (cast) {
            writer.append("CAST(");
        }
        switch (w.fn()) {
            // a windowed listagg of no order of its own concatenates in the window's (DuckDB's string_agg reads the
            // frame in order; Trino's listagg needs a WITHIN GROUP order, which would else be the values')
            case SqlAgg.Reducer r when r.fn() == SqlAgg.Fn.STRING_AGG && r.orderBy().isEmpty()
                    && !w.orderBy().isEmpty() ->
                    reducer(writer, new SqlAgg.Reducer(r.fn(), r.args(), r.distinct(), w.orderBy(), r.type()));
            // a windowed listagg of no order at all: the values' own (listagg needs one, and an array_join cannot
            // wrap a window function); the platform's comparison is unordered
            case SqlAgg.Reducer r when r.fn() == SqlAgg.Fn.STRING_AGG && r.orderBy().isEmpty() -> {
                if (r.args().size() != 2) {
                    throw new DialectCapability("a STRING_AGG of " + r.args().size() + " arguments reached Trino");
                }
                writer.append("listagg(").append(r.distinct() ? "DISTINCT " : "").list(r.args())
                        .append(") WITHIN GROUP (ORDER BY ").expr(r.args().get(0), 0).append(")");
            }
            case SqlAgg.Reducer r -> reducer(writer, r);
            case SqlAgg.RankingFn r -> writer.append(aggregateName(r.fn())).append("(").list(r.args()).append(")");
            case SqlAgg.ValueFn v -> writer.append(aggregateName(v.fn())).append("(").list(v.args()).append(")");
        }
        over(writer, w);
        return cast ? writer.append(" AS DOUBLE)") : writer;
    }

    /** A frame bound: an interval offset a Trino interval literal ({@code INTERVAL '1' DAY}). */
    @Override
    protected String bound(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p -> interval(p.n(), p.unit()) + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f -> interval(f.n(), f.unit()) + " FOLLOWING";
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.Following f -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> super.bound(b);
        };
    }

    private static String interval(long n, String unit) {
        String u = unit.toUpperCase(java.util.Locale.ROOT);
        String single = u.endsWith("S") ? u.substring(0, u.length() - 1) : u;
        return switch (single) {
            case "WEEK" -> "INTERVAL '" + Math.multiplyExact(n, 7L) + "' DAY";
            case "DAY", "HOUR", "MINUTE", "SECOND", "YEAR", "MONTH" -> "INTERVAL '" + n + "' " + single;
            default -> throw new DialectCapability("a " + unit + " window frame offset has no Trino interval");
        };
    }

    // ==================================================================
    // JSON constructors
    // ==================================================================

    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        List<SqlExpr> kv = j.kv();
        writer.append("CAST(MAP(ARRAY[");
        for (int i = 0; i < kv.size(); i += 2) {
            writer.append(i > 0 ? ", " : "").expr(kv.get(i), 0);
        }
        writer.append("], ARRAY[");
        for (int i = 1; i < kv.size(); i += 2) {
            writer.append(i > 1 ? ", " : "").piece(toVariant(kv.get(i)));
        }
        return writer.append("]) AS JSON)");
    }

    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("CAST(ARRAY[").join(j.elements(), ", ", (w, e) -> w.piece(toVariant(e))).append("] AS JSON)");
    }

    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        writer.append("coalesce(CAST(array_agg(").piece(toVariant(j.value()));
        if (!j.orderKeys().isEmpty()) {
            writer.append(" ORDER BY ").join(j.orderKeys(), ", ", (w, k) -> w.expr(k.expr(), 0)
                    .append(k.desc() ? " DESC" : " ASC").append(" NULLS LAST"));
        }
        return writer.append(") AS JSON), json_parse('[]'))");
    }

    // ==================================================================
    // Date format codes (Joda's letters, format_datetime and parse_datetime)
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
    // Session tables and DDL
    // ==================================================================

    /** Trino has no temporary table: a plan's metamodel tables are its query's own rows. */
    @Override
    public boolean temporaryTables() {
        return false;
    }

    /** Trino has no temporary table, so no table only one session sees: the server engine's system database is
     *  refused by name here. */
    @Override
    public List<String> sessionTable(com.legend.sql.SqlDdl.CreateTable table,
            com.legend.sql.SqlDml.@com.legend.base.Nullable InsertValues rows) {
        throw new DialectCapability("a session's own table reached Trino, which has no temporary tables: "
                + table.table());
    }

    /** Trino DDL: a nullable column says nothing (an explicit NULL is a syntax error). */
    @Override
    protected String ddlNullability(boolean notNull) {
        return notNull ? " NOT NULL" : "";
    }

    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT -> "BIGINT";
                case SMALLINT -> "SMALLINT";
                case TINYINT -> "TINYINT";
                case INTEGER -> "INTEGER";
                case FLOAT, DOUBLE -> "DOUBLE";
                case REAL -> "REAL";
                case BIT -> "BOOLEAN";
                case TIMESTAMP -> "TIMESTAMP(6)";
                case DATE -> "DATE";
                case TIME -> "TIME(6)";
                case TIMESTAMP_NS -> "TIMESTAMP(9)";
                case JSON -> "JSON";
                case VARCHAR -> "VARCHAR";
                case OTHER, DISTINCT, ARRAY, OBJECT -> throw DdlSpelling.unspelled(p.kind());
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> DdlSpelling.sized(z);
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> DdlSpelling.scaled(s);
        };
    }

    // ==================================================================
    // Values: what the driver hands back
    // ==================================================================

    /** A statement's ROW column arrives as its JSON text ({@link #rowsAsJson}), decoded to the platform's map of its
     *  fields; a MAP as a java.util.Map. Lists are java.sql.Array, read by the executor. */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            @com.legend.base.Nullable SqlType type) {
        if (jdbcValue instanceof String s && type instanceof SqlType.Struct && s.startsWith("{")) {
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

    /** {@inheritDoc} A DATE is {@code date} (Trino's typeof); no other type's name is compared here. */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        if (type == com.legend.sql.SqlType.Scalar.DATE) {
            return "date";
        }
        throw new DialectCapability("a TYPEOF compared with " + type + "'s name reached Trino");
    }

    /** {@inheritDoc} A statement's ROW column is delivered as its JSON text (the driver's own Row class is read
     *  through no interface; {@link #rowsAsJson}), decoded to the struct's map (2026-10-12). */
    @Override
    public boolean deliversOwn(SqlType label, String wire) {
        return label instanceof SqlType.Struct && wire.equals("JSON");
    }

    /** {@inheritDoc} Measured 2026-10-12: a date 12,345,678,912 days on opens with no columns, its error on the first
     *  read; a statement that succeeds always reports its columns. */
    @Override
    public boolean opensFailedResultsWithoutColumns() {
        return true;
    }
}
