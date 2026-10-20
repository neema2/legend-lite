// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.DatePart;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

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
import java.util.Map;
import java.util.Set;

/**
 * The Snowflake EXECUTION dialect (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md). A sibling of {@link Databricks}
 * and {@link Postgres} over {@link AnsiSqlRenderer}: it changes nothing the other dialects render. Every spelling here
 * was measured on the account (Snowflake 10.36.101) before it was trusted; the probe statements and their answers are
 * the draft's NOTES.md.
 *
 * <p>What differs from DuckDB, measured:
 * <ul>
 *   <li>an unquoted identifier folds to UPPER case: EVERY identifier is quoted ({@code "x"});</li>
 *   <li>a string literal reads backslash escapes ({@code 'q\'x'} is {@code q'x}): a backslash is written {@code \\},
 *       a quote doubled;</li>
 *   <li>integers are NUMBER(38,0): no 64-bit overflow, and arithmetic over NUMBER keeps a fixed scale (AVG and the
 *       variances answer 6 digits, MEDIAN 3) -- the Float aggregates compute over DOUBLE arguments;</li>
 *   <li>lists are semi-structured {@code ARRAY}s of VARIANT elements: an element is cast to the list's element type
 *       wherever it is read (a GET, a lambda's typed parameter), and an untyped VARIANT in arithmetic would be a
 *       FLOAT. A SQL NULL element is JSON "undefined": the session's {@code STRICT_JSON_OUTPUT} prints it as
 *       {@code null}, and {@code JSON_INDENT = 0} prints a list compact, so a list cell reaches the platform's JSON
 *       reader ({@code Executor.unwrap}) as standard JSON;</li>
 *   <li>a lambda takes ONE parameter (two for REDUCE), has no index parameter (DuckDB's is 1-based: emulated over
 *       {@code ARRAY_GENERATE_RANGE}), and CANNOT read an enclosing lambda's parameter (measured: invalid
 *       identifier) -- a FILTER or TRANSFORM that does is a REDUCE carrying those values ({@link #carried}); any
 *       other such nesting is refused by name;</li>
 *   <li>a correlated subquery over FLATTEN is refused ("Unsupported subquery type"), so a list is reduced with
 *       REDUCE / ARRAY_* functions, and exploded only by {@code CROSS JOIN LATERAL FLATTEN} (never with ON);</li>
 *   <li>VALUES takes constants only (no ARRAY_CONSTRUCT, PARSE_JSON): such rows are a UNION ALL of selects;</li>
 *   <li>OFFSET needs a LIMIT ({@code LIMIT NULL OFFSET n});</li>
 *   <li>no raise function: error() is a cast of the sentinel-wrapped message that cannot parse (the full message
 *       comes back in the driver's error, measured with 300 characters).</li>
 * </ul>
 *
 * <p>Every {@link SqlFn} and {@link SqlAgg.Fn} is decided HERE by an exhaustive switch: its spelling, a shared
 * helper of the base named in its arm, or a {@link DialectCapability} wall. A wall is honest; a guessed spelling is a
 * silent wrong answer.
 */
public final class Snowflake extends AnsiSqlRenderer {

    /** The engine's H2 text is refused here: Snowflake has no translation of it (decision D2: raw text runs on the
     *  database its connection declares; only the engine's H2 text is translated, for H2, DuckDB and Postgres). */
    @Override
    public String fromEngineText(String statement) {
        throw new DialectCapability("the engine's H2 text reached Snowflake, which has no translation of it");
    }

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "BOOLEAN";
            case INTEGER -> "INTEGER";
            case BIGINT -> "BIGINT";
            case HUGEINT -> "NUMBER(38,0)";
            case DOUBLE -> "DOUBLE";
            case VARCHAR, LITERAL, TEMPORAL_TEXT -> "VARCHAR";
            case DATE -> "DATE";
            case TIMESTAMP -> "TIMESTAMP_NTZ";
            case TIMESTAMPTZ -> "TIMESTAMP_LTZ";
            case JSON -> "VARIANT";
            case TIME -> "TIME";
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** No java.time re-fetch (measured 2026-10-11): its driver refuses getObject(int, LocalDateTime.class),
     *  "Type passed to getObject(int, Class) is unsupported". A timestamp cell keeps the driver's Timestamp;
     *  a BC date-time is designed, not built (docs/CLOUD_BACKENDS_PCT_2026_10_11.md §15). */
    @Override
    public boolean refetchesTimestampAsJavaTime() {
        return false;
    }

    /** Snowflake (2026-10-11, docs/CLOUD_BACKENDS_PCT_2026_10_11.md): double quotes; Snowflake's documented reserved keywords. The dialect quotes EVERY
     *  identifier, so the list never decides a spelling here. */
    public static final Lexicon LEXICON = new Lexicon('"', Set.of(
            "account", "all", "alter", "and", "any", "as", "between", "by", "case", "cast", "check", "column",
            "connect", "connection", "constraint", "create", "cross", "current", "current_date", "current_time",
            "current_timestamp", "current_user", "database", "delete", "distinct", "drop", "else", "exists", "false",
            "following", "for", "from", "full", "grant", "group", "gscluster", "having", "ilike", "in", "increment",
            "inner", "insert", "intersect", "into", "is", "issue", "join", "lateral", "left", "like", "localtime",
            "localtimestamp", "minus", "natural", "not", "null", "of", "on", "or", "order", "organization", "qualify",
            "regexp", "revoke", "right", "rlike", "row", "rows", "sample", "schema", "select", "set", "some", "start",
            "table", "tablesample", "then", "to", "trigger", "true", "try_cast", "union", "unique", "update", "using",
            "values", "view", "when", "whenever", "where", "window", "with"));

    /** Snowflake (2026-10-11): semi-structured arrays with higher-order functions -- the strategies stand aside,
     *  as on DuckDB and Databricks; FULL OUTER is native. */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(true, true, true, true);

    public Snowflake() {
        super(LEXICON);
    }

    // ==================================================================
    // Session and structure
    // ==================================================================

    /**
     * One statement: the platform's naive-UTC contract (TIMEZONE); ISO weeks starting Monday (WEEK_START: DATE_TRUNC
     * 'week' is DuckDB's Monday); a bare TIMESTAMP is TIMESTAMP_NTZ; quoted identifiers keep their case; a script runs
     * in one execute (MULTI_STATEMENT_COUNT); and list, object and variant cells print as compact, STANDARD JSON
     * ({@code JSON_INDENT = 0}; {@code STRICT_JSON_OUTPUT} prints a SQL NULL element as {@code null}, never
     * {@code undefined}, which the platform's JSON reader cannot read). All measured.
     */
    /** The labels of the statement being rendered, as planned (its root's outputs before this dialect's passes):
     *  the contract the executor reads, and the census checks, the root's columns against. */
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

    /** The labels of the statement being rendered (none outside a render). */
    private List<com.legend.sql.OutputCol> rootLabels() {
        List<com.legend.sql.OutputCol> labels = roots.peek();
        return labels == null ? List.of() : labels;
    }

    @Override
    public List<String> sessionSetup() {
        return List.of("ALTER SESSION SET TIMEZONE = 'UTC', WEEK_START = 1, WEEK_OF_YEAR_POLICY = 1,"
                + " TIMESTAMP_TYPE_MAPPING = 'TIMESTAMP_NTZ', QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE,"
                + " MULTI_STATEMENT_COUNT = 0, JSON_INDENT = 0, STRICT_JSON_OUTPUT = TRUE");
    }

    /** A dynamic pivot is discovered first: the static form is emulated (PivotAsGroupBy). */
    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    /**
     * Native lists (the carrier strategies stand aside); the pivot as filtered aggregates (Snowflake's PIVOT takes one
     * aggregate and names its columns {@code 'a'}, quotes included); the checked-defect envelopes as list lambdas
     * (a to-many hoist nests lambdas and walls here); the substring start clamp (a negative start counts from the
     * end here); a select-list UNNEST as a lateral FLATTEN; a constant GROUP BY or ORDER BY key as a typed
     * expression (Snowflake reads an integer one as a position); and the root's DECIMAL(p,s) and HUGEINT columns
     * cast to their declared types. QUALIFY and star exclusion are native.
     */
    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS), new PivotAsGroupBy(), new CheckedDefectsToLists(),
                new SubstringClamp(), new ExplosionMarkers.LateralUnnestAsMarker(),
                new ExplosionMarkers.SelectListUnnestAsLateral(), new ExplosionMarkers.StarOverExplosionUnwrapped(),
                new ConstantKeysAsExpressions(), new RootNumericTypes());
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
    protected String rowOrderColumn() {
        throw new DialectCapability("row order reached Snowflake, which has no row-order column");
    }

    /** The as-of join is written whole by {@link #source}: Snowflake's ASOF JOIN takes its inequality apart from
     *  ON, as a MATCH_CONDITION. */
    @Override
    protected String asOfJoinClause() {
        throw new IllegalStateException("an as-of join reached the base join writer: Snowflake's source() writes it");
    }

    @Override
    protected SqlWriter pivotSource(SqlWriter writer, SqlSource.Pivot p, int depth) {
        throw new DialectCapability("a pivot reached the Snowflake writer: PivotAsGroupBy emulates a static one"
                + (p.in().isEmpty() ? " (a dynamic pivot is discovered first: needsStaticPivot)" : "")
                + "; this one has " + p.on().size() + " pivot columns");
    }

    // ==================================================================
    // Lexical: identifiers, literals, types
    // ==================================================================

    /** Every identifier quoted (an unquoted one folds to upper case); a quote-bearing declaration ({@code "date"}) is
     *  already its own spelling. */
    @Override
    protected String ident(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")
                && !name.substring(1, name.length() - 1).replace("\"\"", "").contains("\"")) {
            return name;
        }
        return delimited(name);
    }

    /** Alias-less projections label EXPLICITLY from the declared output: Snowflake labels an expression by its
     *  upper-cased text. */
    @Override
    protected @com.legend.base.Nullable String implicitLabel(SqlSelect.Projection p) {
        return p.out() != null ? aliasIdent(p.out().name()) : super.implicitLabel(p);
    }

    /** A quote doubled, a backslash escaped (measured: {@code 'back\\slash'} is {@code back\slash}); NUL spliced as
     *  {@code chr(0)}, as the base does. */
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
        return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'";
    }

    /** {@code TIMESTAMP '...'} is TIMESTAMP_NTZ under the session's mapping ({@code TIMESTAMP_NTZ '...'} is a syntax
     *  error, measured). Snowflake keeps nanoseconds; DuckDB and the platform keep microseconds: finer digits are cut. */
    @Override
    protected String timestampLit(String iso) {
        return super.timestampLit(microseconds(iso));
    }

    private static String microseconds(String iso) {
        int dot = iso.lastIndexOf('.');
        if (dot < 0) {
            return iso;
        }
        int end = dot + 1;
        while (end < iso.length() && Character.isDigit(iso.charAt(end))) {
            end++;
        }
        return end - dot - 1 > 6 ? iso.substring(0, dot + 7) + iso.substring(end) : iso;
    }

    /** A Float literal's plain spelling; NaN and the infinities as FLOAT text (a bare {@code NaN} is no literal). */
    @Override
    protected String floatLiteral(double v) {
        if (Double.isNaN(v)) {
            return "CAST('NaN' AS DOUBLE)";
        }
        if (Double.isInfinite(v)) {
            return v > 0 ? "CAST('inf' AS DOUBLE)" : "CAST('-inf' AS DOUBLE)";
        }
        // the literal is the DECIMAL of its digits (SqlTyping.decimalLitType, DuckDB's reading): Snowflake drops a
        // trailing zero's scale (17.0 is a NUMBER(2,0), its text '17', measured), so such a literal spells its type
        String plain = super.floatLiteral(v);
        java.math.BigDecimal digits = com.legend.sql.SqlTyping.floatDecimal(v);
        if (digits != null && digits.scale() > 0 && digits.stripTrailingZeros().scale() < digits.scale()) {
            return "CAST(" + plain + " AS NUMBER(" + Math.max(digits.precision(), digits.scale()) + "," + digits.scale()
                    + "))";
        }
        return plain;
    }

    /** Lists are semi-structured ARRAYs whatever their element (the elements are VARIANTs, cast where read). */
    @Override
    protected String arrayTypeName(String element) {
        return "ARRAY";
    }

    @Override
    protected String mapTypeName(String key, String value) {
        return "OBJECT";
    }

    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        List<java.util.Map.Entry<String, String>> fields = structFields(st);
        return "OBJECT";
    }

    /** A date and a date-time placeholder take the NTZ timestamp; finer than a microsecond is cut to six digits, as
     *  its literal is. */
    @Override
    protected RenderedStatement.TypeSpelling holeType(com.legend.sql.ValueKind kind) {
        return switch (kind) {
            case DATE -> new RenderedStatement.TypeSpelling("DATE", RenderedStatement.Digits.NONE);
            case DATE_TIME -> new RenderedStatement.TypeSpelling("TIMESTAMP_NTZ", RenderedStatement.Digits.NONE);
            case DATE_TIME_NANOS -> new RenderedStatement.TypeSpelling("TIMESTAMP_NTZ", RenderedStatement.Digits.NONE, 6);
            case INTEGER, DECIMAL, FLOATING -> super.holeType(kind);
        };
    }

    /** Snowflake DDL: BOOLEAN for BIT, VARIANT for JSON, TIMESTAMP_NTZ for TIMESTAMP, ARRAY and OBJECT as such. */
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
                case TIMESTAMP -> "TIMESTAMP_NTZ";
                case DATE -> "DATE";
                case TIME -> "TIME";
                case TIMESTAMP_NS -> "TIMESTAMP_NTZ(9)";
                case JSON -> "VARIANT";
                case VARCHAR -> "VARCHAR";
                case OTHER, DISTINCT -> throw DdlSpelling.unspelled(p.kind());
                case ARRAY -> "ARRAY";
                case OBJECT -> "OBJECT";
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> DdlSpelling.sized(z);
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> DdlSpelling.scaled(s);
        };
    }

    // ==================================================================
    // Expressions: lambda scopes, element reads, lateral reads
    // ==================================================================

    /** A lambda written in SQL: its declared parameters, and the IR parameters it writes as pieces (an emulated
     *  index lambda reads its element and index off its one range parameter). */
    private record Scope(Set<String> params, Map<String, SqlWriter.Piece> subs) {
    }

    /** The lambdas being written, innermost first. Snowflake's lambda reads no enclosing lambda's parameter. */
    private final Deque<Scope> scopes = new ArrayDeque<>();

    /** The lateral FLATTENs in scope of the select being written: alias, then each exploded column's list. */
    private final Deque<Map<String, Map<String, SqlExpr>>> laterals = new ArrayDeque<>();

    private int fresh;

    private String freshName(String stem) {
        return "__" + stem + (fresh++);
    }

    /** What a name read inside lambdas resolves to. */
    private enum Binding { NONE, PARAM, SUB }

    private record Resolved(Binding kind, SqlWriter.@com.legend.base.Nullable Piece sub) {
    }

    private Resolved resolve(String name) {
        int depth = 0;
        for (Scope s : scopes) {
            boolean sub = s.subs().containsKey(name);
            if (sub || s.params().contains(name)) {
                if (depth > 0) {
                    throw new DialectCapability("a lambda reading the parameter '" + name + "' of an enclosing"
                            + " lambda reached Snowflake, whose lambdas cannot (measured: invalid identifier)");
                }
                return sub ? new Resolved(Binding.SUB, s.subs().get(name)) : new Resolved(Binding.PARAM, null);
            }
            depth++;
        }
        return new Resolved(Binding.NONE, null);
    }

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        return switch (e) {
            case SqlExpr.Column c -> column(writer, c);
            case SqlExpr.OrderedListAgg ola -> writer.append("ARRAY_AGG(").piece(keepNull(ola.value()))
                    .append(") WITHIN GROUP (ORDER BY ").expr(ola.orderBy(), 0).append(")");
            case SqlExpr.Membership m when m.collection() instanceof SqlExpr.PlanParam p -> throw new DialectCapability(
                    "list parameter '" + p.name() + "' reached Snowflake, which binds no array (= ANY(?))");
            // a decimal literal is the DECIMAL of its digits (SqlTyping.decimalLitType): Snowflake drops a trailing
            // zero's scale (1.0 is a NUMBER(2,0), its text '1', measured), so such a literal spells its type
            case SqlExpr.DecimalLit d when d.value().scale() > 0
                    && d.value().stripTrailingZeros().scale() < d.value().scale() -> writer.append("CAST("
                    + d.value().toPlainString() + " AS " + (d.type() instanceof com.legend.sql.TypeFact.Typed t
                            && t.type() instanceof SqlType.Decimal dd ? castTypeName(dd)
                            : "NUMBER(" + Math.max(d.value().precision(), d.value().scale()) + ","
                                    + d.value().scale() + ")") + ")");
            default -> super.expr(writer, e, parentPrec);
        };
    }

    /** A column: a lambda parameter (or its emulated piece), a field of a struct parameter ({@code x.f}, DuckDB's
     *  spelling), a lateral FLATTEN's exploded element, or a plain reference. */
    private SqlWriter column(SqlWriter writer, SqlExpr.Column c) {
        String table = c.table();
        if (table == null) {
            Resolved r = resolve(c.name());
            if (r.kind() == Binding.SUB) {
                return writer.piece(java.util.Objects.requireNonNull(r.sub(), "sub"));
            }
            // an unqualified read of an exploded column (corr's SELECT CORR(a, b) FROM (SELECT UNNEST(xs) AS a, ...)):
            // the FLATTEN that carries it, when exactly one of the innermost select's does
            Map<String, Map<String, SqlExpr>> innermost = laterals.peek();
            if (r.kind() == Binding.NONE && innermost != null) {
                List<Map.Entry<String, Map<String, SqlExpr>>> owners = innermost.entrySet().stream()
                        .filter(e -> e.getValue().containsKey(c.name())).toList();
                if (owners.size() == 1) {
                    return explodedColumn(writer, owners.get(0).getKey(), c, owners.get(0).getValue());
                }
            }
            return writer.append(columnRef(c));
        }
        Resolved r = scopes.isEmpty() ? new Resolved(Binding.NONE, null) : resolve(table);
        if (r.kind() != Binding.NONE) {
            SqlWriter.Piece param = r.kind() == Binding.SUB ? java.util.Objects.requireNonNull(r.sub(), "sub")
                    : w -> w.append(columnRef(SqlExpr.Column.derived(null, table)));
            // an untyped field read keeps SQL NULL for a NULL field (stored as JSON null: OBJECT_CONSTRUCT_KEEP_NULL;
            // a JSON null is no NULL to COALESCE, measured); a typed read's CAST already does
            SqlWriter.Piece field = w -> w.append("GET(").piece(param).append(", ").append(stringLit(c.name()))
                    .append(")");
            SqlType t = typeOf(c);
            return writer.piece(t == null ? w -> w.append("STRIP_NULL_VALUE(").piece(field).append(")") : typed(field, t));
        }
        for (Map<String, Map<String, SqlExpr>> scope : laterals) {
            Map<String, SqlExpr> cols = scope.get(table);
            if (cols != null && cols.containsKey(c.name())) {
                return explodedColumn(writer, table, c, cols);
            }
        }
        return writer.append(columnRef(c));
    }

    /** {@code c}, an exploded column of the FLATTEN {@code table}: its list's element at the FLATTEN's position. */
    private SqlWriter explodedColumn(SqlWriter writer, String table, SqlExpr.Column c, Map<String, SqlExpr> cols) {
        SqlExpr list = java.util.Objects.requireNonNull(cols.get(c.name()), "exploded column");
        SqlType t = typeOf(c) != null ? typeOf(c) : listElement(list);
        return writer.piece(typed(w -> w.append("GET(").expr(list, 0).append(", ").append(aliasIdent(table))
                .append(".\"INDEX\")"), t));
    }

    /** A VARIANT read as type {@code t}: a scalar cast to it, a list to ARRAY, a struct or map to OBJECT; a variant
     *  (or an unknown type) as it is. */
    private SqlWriter.Piece typed(SqlWriter.Piece v, @com.legend.base.Nullable SqlType t) {
        if (t == null || t == SqlType.Scalar.JSON) {
            return v;
        }
        String name = castTypeName(t);
        return w -> w.append("CAST(").piece(v).append(" AS " + name + ")");
    }

    /** The SQL type a lambda parameter over elements of {@code t} is declared as, or null to leave it a VARIANT. */
    private @com.legend.base.Nullable String paramType(@com.legend.base.Nullable SqlType t) {
        return t == null ? null : castTypeName(t);
    }

    /** {@code v} as a list element keeping a SQL NULL as JSON null (ARRAY_AGG drops a NULL, OBJECT_INSERT drops its
     *  key, FLATTEN skips an undefined element -- measured). */
    private static SqlWriter.Piece keepNull(SqlExpr v) {
        return w -> w.append("COALESCE(TO_VARIANT(").expr(v, 0).append("), PARSE_JSON('null'))");
    }

    private static @com.legend.base.Nullable SqlType listElement(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof SqlType.Array a
                ? a.element() : null;
    }

    private static @com.legend.base.Nullable SqlType typeOf(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
    }

    private static boolean isJson(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.JSON;
    }

    private static boolean isText(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.VARCHAR || e instanceof SqlExpr.StringLit;
    }

    private static boolean isDouble(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.DOUBLE;
    }

    private static boolean isInteger(@com.legend.base.Nullable SqlType t) {
        return t == SqlType.Scalar.INTEGER || t == SqlType.Scalar.BIGINT || t == SqlType.Scalar.HUGEINT;
    }

    private static boolean isTimestamp(SqlExpr e) {
        return typeOf(e) == SqlType.Scalar.TIMESTAMP;
    }

    // ---- lambdas

    /** A lambda parameter: its name and its declared type (null: a VARIANT). */
    private record Param(String name, @com.legend.base.Nullable String type) {
    }

    /** {@code "p" T -> body} (or {@code ("a" T, "b" U) -> body}), the body written inside the lambda's scope. */
    private SqlWriter.Piece lambdaOf(List<Param> params, Map<String, SqlWriter.Piece> subs, SqlWriter.Piece body) {
        return w -> {
            List<String> decl = new ArrayList<>();
            for (Param p : params) {
                decl.add(delimited(p.name()) + (p.type() == null ? "" : " " + p.type()));
            }
            w.append(decl.size() == 1 ? decl.get(0) : "(" + String.join(", ", decl) + ")").append(" -> ");
            Set<String> names = new java.util.LinkedHashSet<>();
            params.forEach(p -> names.add(p.name()));
            scopes.push(new Scope(names, subs));
            try {
                w.piece(body);
            } finally {
                scopes.pop();
            }
        };
    }

    /** An IR lambda written as it is (a function the dialect passes through): its parameters as VARIANTs. */
    @Override
    protected SqlWriter lambda(SqlWriter writer, SqlExpr.Lambda l) {
        return writer.piece(lambdaOf(l.params().stream().map(p -> new Param(p, null)).toList(), Map.of(),
                w -> w.expr(l.body(), 0)));
    }

    /** An element lambda over {@code xs}: one IR parameter is a typed SQL parameter; with an index parameter (DuckDB's,
     *  1-based) it ranges over the positions instead, reading the element and the index off the position. Returns
     *  the lambda and the array it ranges over. */
    private record Ranged(SqlWriter.Piece over, SqlWriter.Piece lambda, boolean positions) {
    }

    private Ranged ranged(SqlExpr xs, SqlExpr.Lambda l, java.util.function.Function<SqlWriter.Piece, SqlWriter.Piece> body) {
        SqlType element = listElement(xs);
        if (l.params().size() == 1) {
            return new Ranged(w -> w.expr(xs, 0), lambdaOf(List.of(new Param(l.params().get(0), paramType(element))),
                    Map.of(), body.apply(w -> w.expr(l.body(), 0))), false);
        }
        if (l.params().size() != 2) {
            throw new DialectCapability("a lambda of " + l.params().size() + " parameters over a list reached Snowflake");
        }
        String i = freshName("i");
        String iRef = delimited(i);
        Map<String, SqlWriter.Piece> subs = new LinkedHashMap<>();
        subs.put(l.params().get(0), typed(w -> w.append("GET(").expr(xs, 0).append(", " + iRef + ")"), element));
        subs.put(l.params().get(1), w -> w.append("(" + iRef + " + 1)"));
        return new Ranged(positions(xs), lambdaOf(List.of(new Param(i, "INTEGER")), subs,
                body.apply(w -> w.expr(l.body(), 0))), true);
    }

    /** The names {@code l}'s body reads that an enclosing lambda binds (its parameters, or the names it writes as
     *  pieces): Snowflake's lambda cannot read them (measured: invalid identifier), so they are carried in
     *  ({@link #carried}). */
    private Set<String> captured(SqlExpr.Lambda l) {
        Set<String> out = new java.util.LinkedHashSet<>();
        if (!scopes.isEmpty()) {
            collectCaptured(l.body(), new java.util.HashSet<>(l.params()), out);
        }
        return out;
    }

    private void collectCaptured(SqlExpr e, Set<String> bound, Set<String> out) {
        if (e instanceof SqlExpr.Lambda inner) {
            Set<String> b = new java.util.HashSet<>(bound);
            b.addAll(inner.params());
            collectCaptured(inner.body(), b, out);
            return;
        }
        if (e instanceof SqlExpr.Column c) {
            String name = c.table() != null ? c.table() : c.name();
            if (!bound.contains(name)
                    && scopes.stream().anyMatch(s -> s.params().contains(name) || s.subs().containsKey(name))) {
                out.add(name);
            }
        }
        for (SqlExpr k : e.children()) {
            collectCaptured(k, bound, out);
        }
    }

    /** The type a read of {@code name} in {@code e} declares (null: none does, a VARIANT). */
    private static @com.legend.base.Nullable SqlType capturedType(SqlExpr e, String name) {
        if (e instanceof SqlExpr.Column c && c.table() == null && c.name().equals(name) && typeOf(c) != null) {
            return typeOf(c);
        }
        for (SqlExpr k : e.children()) {
            SqlType t = capturedType(k, name);
            if (t != null) {
                return t;
            }
        }
        return null;
    }

    /**
     * {@code FILTER(xs, p -> body)} or {@code TRANSFORM(xs, p -> body)} whose body reads an enclosing lambda's names
     * (removeDuplicates' comparator reads the candidate its fold holds): a REDUCE whose accumulator carries those
     * values -- {@code {'r': the result so far, 'c0': name0, ...}}, its start written where the names are in scope --
     * and whose lambda reads them off it. Measured: an enclosing lambda's parameter is an invalid identifier inside a
     * lambda, but not in REDUCE's start; a NULL list reduces to NULL, an empty one to the start.
     */
    private SqlWriter.Piece carried(SqlExpr xs, SqlExpr.Lambda l, Set<String> names, boolean filter) {
        List<String> cs = new ArrayList<>(names);
        String state = delimited(freshName("s"));
        Map<String, SqlWriter.Piece> subs = new LinkedHashMap<>();
        for (int k = 0; k < cs.size(); k++) {
            SqlType t = capturedType(l.body(), cs.get(k));
            String key = "'c" + k + "'";
            SqlWriter.Piece read = w -> w.append("GET(" + state + ", " + key + ")");
            subs.put(cs.get(k), t == null ? read : typed(read, t));
        }
        String element = l.params().get(0);
        String declared = paramType(listElement(xs));
        SqlWriter.Piece kept = w -> w.append("CAST(GET(" + state + ", 'r') AS ARRAY)");
        SqlWriter.Piece next = filter
                ? w -> w.append("IFF(").expr(l.body(), 0).append(", ARRAY_APPEND(").piece(kept).append(", "
                        + delimited(element) + "), ").piece(kept).append(")")
                : w -> w.append("ARRAY_APPEND(").piece(kept).append(", ").expr(l.body(), 0).append(")");
        SqlWriter.Piece step = w -> {
            w.append("OBJECT_CONSTRUCT_KEEP_NULL('r', ").piece(next);
            for (int k = 0; k < cs.size(); k++) {
                w.append(", 'c" + k + "', GET(" + state + ", 'c" + k + "')");
            }
            w.append(")");
        };
        SqlWriter.Piece start = w -> {
            w.append("OBJECT_CONSTRUCT_KEEP_NULL('r', ARRAY_CONSTRUCT()");
            for (int k = 0; k < cs.size(); k++) {
                w.append(", 'c" + k + "', ").expr(SqlExpr.Column.derived(null, cs.get(k)), 0);
            }
            w.append(")");
        };
        return w -> w.append("CAST(GET(REDUCE(").expr(xs, 0).append(", ").piece(start).append(", ")
                .piece(lambdaOf(List.of(new Param(state.replace("\"", ""), "OBJECT"),
                        new Param(element, declared != null ? declared : "VARIANT")), subs, step))
                .append("), 'r') AS ARRAY)");
    }

    /**
     * Jaro-Winkler similarity, exactly DuckDB's (rapidfuzz's): the match window is {@code max(|p|, |t|) / 2 - 1}, at
     * least 0; each character of {@code t} in turn takes the first unmatched equal character of {@code p} in its
     * window; half the matches out of order, rounded down, are the transpositions; a Jaro score above 0.7 gains a tenth
     * of its distance to 1 for each character of a common prefix of up to four; an empty string scores 0. The score is
     * computed in DOUBLE in DuckDB's order of operations, so the bits agree (measured: 1,585 pairs, every one equal).
     * Snowflake's own JAROWINKLER_SIMILARITY is another function: an integer percentage, case-insensitive (96 for
     * MARTHA/MARHTA, 100 for abc/ABC, 0 for two empty strings; measured).
     *
     * <p>Three REDUCEs over OBJECT states: the matching (a REDUCE over {@code t}'s positions, each a REDUCE over its
     * window), the transpositions (along {@code p}), then the score. A lambda here reads no enclosing lambda's
     * parameter (as {@link #carried}), so each state carries what the next lambda reads, written into a start where it
     * is in scope; the arguments are written once, into a one-element TRANSFORM that binds them.
     */
    private SqlWriter.Piece jaroWinkler(SqlExpr p, SqlExpr t) {
        String o = delimited(freshName("o"));
        String s = delimited(freshName("s"));
        String u = delimited(freshName("u"));
        String v = delimited(freshName("v"));
        String q = delimited(freshName("q"));
        String j = delimited(freshName("j"));
        String i = delimited(freshName("i"));
        String op = fieldOf(o, "p", "STRING");
        String ot = fieldOf(o, "t", "STRING");
        // t's j-th character takes the first unmatched equal character of p in its window: 'f' flags p's matched
        // characters, 'tm' is t's matched characters in order, 'k' says whether this one has matched
        String b = fieldOf(s, "b", "INTEGER");
        String take = withField(withField(withField(u, "k", "1"), "f", "INSERT(" + fieldOf(u, "f", "STRING") + ", " + i + ", 1, '1')"),
                "tm", fieldOf(u, "tm", "STRING") + " || " + fieldOf(u, "c", "STRING"));
        String window = "REDUCE(ARRAY_GENERATE_RANGE(GREATEST(1, " + j + " - " + b + "), " + j + " + " + b + " + 1), "
                + withField(withField(s, "c", "SUBSTR(" + fieldOf(s, "t", "STRING") + ", " + j + ", 1)"), "k", "0") + ", (" + u
                + " OBJECT, " + i + " INTEGER) -> IFF(" + fieldOf(u, "k", "INTEGER") + " = 0 AND SUBSTR("
                + fieldOf(u, "f", "STRING") + ", " + i + ", 1) = '0' AND SUBSTR(" + fieldOf(u, "p", "STRING") + ", " + i
                + ", 1) = " + fieldOf(u, "c", "STRING") + ", " + take + ", " + u + "))";
        String matched = "REDUCE(ARRAY_GENERATE_RANGE(1, LENGTH(" + ot + ") + 1), OBJECT_CONSTRUCT_KEEP_NULL('p', " + op
                + ", 't', " + ot + ", 'b', GREATEST(0, FLOOR(GREATEST(LENGTH(" + op + "), LENGTH(" + ot
                + ")) / 2) - 1), 'f', REPEAT('0', LENGTH(" + op + ")), 'tm', ''), (" + s + " OBJECT, " + j
                + " INTEGER) -> " + window + ")";
        // along p: 'n' matched characters so far, 'x' of them unequal to t's matched character in the same place
        String n = fieldOf(v, "n", "INTEGER");
        String counted = "REDUCE(ARRAY_GENERATE_RANGE(1, LENGTH(" + op + ") + 1), " + withField(withField(matched, "n", "0"), "x", "0")
                + ", (" + v + " OBJECT, " + i + " INTEGER) -> IFF(SUBSTR(" + fieldOf(v, "f", "STRING") + ", " + i
                + ", 1) = '1', " + withField(withField(v, "n", n + " + 1"), "x", fieldOf(v, "x", "INTEGER") + " + IFF(SUBSTR("
                + fieldOf(v, "p", "STRING") + ", " + i + ", 1) = SUBSTR(" + fieldOf(v, "tm", "STRING") + ", " + n
                + " + 1, 1), 0, 1)") + ", " + v + "))";
        String qp = fieldOf(q, "p", "STRING");
        String qt = fieldOf(q, "t", "STRING");
        String m = "CAST(" + fieldOf(q, "n", "INTEGER") + " AS DOUBLE)";
        String jaro = "((" + m + " / CAST(LENGTH(" + qp + ") AS DOUBLE) + " + m + " / CAST(LENGTH(" + qt
                + ") AS DOUBLE) + (" + m + " - CAST(FLOOR(" + fieldOf(q, "x", "INTEGER") + " / 2) AS DOUBLE)) / " + m
                + ") / CAST(3 AS DOUBLE))";
        List<String> prefix = new ArrayList<>();
        for (int k = 1; k <= 4; k++) {
            prefix.add("IFF(" + k + " <= LEAST(LENGTH(" + qp + "), LENGTH(" + qt + ")) AND LEFT(" + qp + ", " + k
                    + ") = LEFT(" + qt + ", " + k + "), 1, 0)");
        }
        String score = "IFF(" + qp + " IS NULL OR " + qt + " IS NULL, NULL, IFF(" + fieldOf(q, "n", "INTEGER")
                + " = 0, CAST(0 AS DOUBLE), IFF(" + jaro + " > CAST(0.7 AS DOUBLE), " + jaro + " + CAST("
                + String.join(" + ", prefix) + " AS DOUBLE) * CAST(0.1 AS DOUBLE) * (CAST(1 AS DOUBLE) - " + jaro
                + "), " + jaro + ")))";
        String body = "GET(TRANSFORM(ARRAY_CONSTRUCT(" + counted + "), " + q + " OBJECT -> " + score + "), 0)";
        return w -> w.append("CAST(GET(TRANSFORM(ARRAY_CONSTRUCT(OBJECT_CONSTRUCT_KEEP_NULL('p', ").expr(p, 0)
                .append(", 't', ").expr(t, 0).append(")), " + o + " OBJECT -> " + body + "), 0) AS DOUBLE)");
    }

    /** {@code key} of the OBJECT {@code state}, cast to {@code type}. */
    private static String fieldOf(String state, String key, String type) {
        return "CAST(GET(" + state + ", '" + key + "') AS " + type + ")";
    }

    /** The OBJECT {@code state} with {@code key} set to {@code value}. */
    private static String withField(String state, String key, String value) {
        return "OBJECT_INSERT(" + state + ", '" + key + "', " + value + ", TRUE)";
    }

    /** The positions of {@code xs}, 0-based ({@code NULL} for a NULL list). */
    private static SqlWriter.Piece positions(SqlExpr xs) {
        return w -> w.append("ARRAY_GENERATE_RANGE(0, ARRAY_SIZE(").expr(xs, 0).append("))");
    }

    // ==================================================================
    // Lists: semi-structured ARRAYs, elements cast where read
    // ==================================================================

    @Override
    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        return writer.append("ARRAY_CONSTRUCT(").list(elements).append(")");
    }

    /** A pure collection holds no empties: SQL NULL and JSON null elements both drop (ARRAY_COMPACT, measured). */
    @Override
    protected SqlWriter compactList(SqlWriter writer, SqlExpr.CompactList cl, int parentPrec) {
        return writer.append("ARRAY_COMPACT(").expr(cl.list(), 0).append(")");
    }

    @Override
    protected SqlWriter listCall(SqlWriter writer, SqlFn fn, List<SqlExpr> args) {
        return list(writer, fn, args, null);
    }

    /** Every list function whose first argument is a list; {@code result} is the call's own type when known. */
    private SqlWriter list(SqlWriter writer, SqlFn fn, List<SqlExpr> a, @com.legend.base.Nullable SqlType result) {
        SqlExpr xs = a.get(0);
        SqlType element = listElement(xs);
        return switch (fn) {
            case LIST_FILTER -> {
                SqlExpr.Lambda fl = (SqlExpr.Lambda) a.get(1);
                Set<String> carry = captured(fl);
                if (!carry.isEmpty() && fl.params().size() == 1) {
                    yield writer.piece(carried(xs, fl, carry, true));
                }
                Ranged r = ranged(xs, fl, b -> b);
                if (!r.positions()) {
                    yield writer.append("FILTER(").piece(r.over()).append(", ").piece(r.lambda()).append(")");
                }
                // the kept positions, then their elements (as the list holds them)
                String j = delimited(freshName("j"));
                yield writer.append("TRANSFORM(FILTER(").piece(r.over()).append(", ").piece(r.lambda()).append("), ")
                        .append(j + " INTEGER -> GET(").expr(xs, 0).append(", " + j + "))");
            }
            case LIST_TRANSFORM -> {
                SqlExpr.Lambda tl = (SqlExpr.Lambda) a.get(1);
                Set<String> carry = captured(tl);
                if (!carry.isEmpty() && tl.params().size() == 1) {
                    yield writer.piece(carried(xs, tl, carry, false));
                }
                Ranged r = ranged(xs, tl, b -> b);
                yield writer.append("TRANSFORM(").piece(r.over()).append(", ").piece(r.lambda()).append(")");
            }
            // NULL ++ xs is xs, as DuckDB's list_concat
            case LIST_CONCAT -> {
                SqlWriter.Piece acc = w -> w.append("COALESCE(").expr(a.get(0), 0).append(", ARRAY_CONSTRUCT())");
                for (int k = 1; k < a.size(); k++) {
                    SqlWriter.Piece left = acc;
                    SqlExpr next = a.get(k);
                    acc = w -> w.append("ARRAY_CAT(").piece(left).append(", COALESCE(").expr(next, 0)
                            .append(", ARRAY_CONSTRUCT()))");
                }
                yield writer.piece(acc);
            }
            // 1-based, negative from the end, out of range (0 included) NULL, as DuckDB's list_extract; GET is 0-based
            // and raises on a negative index (measured), so the negative branch guards it
            case LIST_GET -> writer.piece(typed(listGet(xs, a.get(1)), element != null ? element : result));
            // DuckDB's list_position: 1-based, NULL when absent (ARRAY_POSITION is 0-based, NULL when absent)
            case LIST_POSITION -> writer.append("(ARRAY_POSITION(TO_VARIANT(").expr(a.get(1), 0).append("), ")
                    .expr(xs, 0).append(") + 1)");
            // DuckDB's list_distinct drops NULLs; ARRAY_DISTINCT keeps the first occurrence's order (measured)
            case LIST_DISTINCT -> writer.append("ARRAY_DISTINCT(ARRAY_COMPACT(").expr(xs, 0).append("))");
            case LIST_APPEND -> writer.append("ARRAY_APPEND(").expr(xs, 0).append(", ").expr(a.get(1), 0).append(")");
            case LIST_SUM -> writer.piece(nonEmpty(xs, sum(xs, element, result)));
            case LIST_MIN -> writer.piece(typed(w -> w.append("ARRAY_MIN(").expr(xs, 0).append(")"),
                    element != null ? element : result));
            case LIST_MAX -> writer.piece(typed(w -> w.append("ARRAY_MAX(").expr(xs, 0).append(")"),
                    element != null ? element : result));
            case LIST_AVG -> writer.piece(nonEmpty(xs, w -> w.piece(sum(xs, SqlType.Scalar.DOUBLE,
                    SqlType.Scalar.DOUBLE)).append(" / ARRAY_SIZE(ARRAY_COMPACT(").expr(xs, 0).append("))")));
            case LIST_MEDIAN -> writer.piece(median(xs, element, result));
            case LIST_MODE -> writer.piece(mode(xs, element));
            case LIST_BOOL_AND -> writer.piece(nonEmpty(xs, w -> w.append("NOT ARRAY_CONTAINS(TO_VARIANT(FALSE), ")
                    .expr(xs, 0).append(")")));
            case LIST_BOOL_OR -> writer.piece(nonEmpty(xs, w -> w.append("ARRAY_CONTAINS(TO_VARIANT(TRUE), ")
                    .expr(xs, 0).append(")")));
            case LIST_PRODUCT -> writer.piece(nonEmpty(xs, reduce(xs, SqlType.Scalar.DOUBLE, SqlType.Scalar.DOUBLE,
                    "1", "*")));
            // ascending, NULLs last (undefined sorts as SQL NULL; a JSON null as the greatest value, measured)
            case LIST_SORT -> element instanceof SqlType.Struct st ? writer.piece(structSort(xs, st, false))
                    : writer.append("ARRAY_SORT(").expr(xs, 0).append(")");
            case LIST_SORT_DESC -> element instanceof SqlType.Struct st ? writer.piece(structSort(xs, st, true))
                    : writer.append("ARRAY_SORT(").expr(xs, 0).append(", FALSE, FALSE)");
            case LIST_TAIL -> writer.append("ARRAY_SLICE(").expr(xs, 0).append(", 1, ARRAY_SIZE(").expr(xs, 0)
                    .append("))");
            case LIST_INIT -> writer.append("ARRAY_SLICE(").expr(xs, 0).append(", 0, -1)");
            // DuckDB's array_slice: 1-based, both ends inclusive; ARRAY_SLICE is 0-based, its end exclusive
            case LIST_SLICE -> {
                if (negativeLiteral(a.get(1)) || negativeLiteral(a.get(2))) {
                    throw wall(fn, "a slice counted from the end (DuckDB's end is inclusive there, Snowflake's not)");
                }
                yield writer.append("ARRAY_SLICE(").expr(xs, 0).append(", GREATEST(").expr(a.get(1), 6)
                        .append(" - 1, 0), ").expr(a.get(2), 0).append(")");
            }
            case LIST_FLATTEN -> writer.append("ARRAY_FLATTEN(ARRAY_COMPACT(").expr(xs, 0).append("))");
            case LIST_ZIP -> throw wall(fn, "ARRAYS_ZIP names its fields $1, $2 (the IR's are 1, 2)");
            // DuckDB's list_reduce: the first element starts the accumulator, the lambda (acc, x) runs over the rest
            case LIST_REDUCE -> {
                if (!(a.get(1) instanceof SqlExpr.Lambda l) || l.params().size() != 2) {
                    throw wall(fn, "a reduction lambda of another shape");
                }
                String t = paramType(element);
                if (t == null) {
                    throw wall(fn, "a reduction over a list of unknown element type");
                }
                yield writer.append("REDUCE(ARRAY_SLICE(").expr(xs, 0).append(", 1, ARRAY_SIZE(").expr(xs, 0)
                        .append(")), ").piece(typed(w -> w.append("GET(").expr(xs, 0).append(", 0)"), element))
                        .append(", ").piece(lambdaOf(List.of(new Param(l.params().get(0), t),
                                new Param(l.params().get(1), t)), Map.of(),
                                w -> w.append("CAST(").expr(l.body(), 0).append(" AS " + t + ")"))).append(")");
            }
            case TYPEOF -> throw wall(fn, "TYPEOF names Snowflake's variant types, not DuckDB's");
            default -> throw new IllegalStateException("not a list call: " + fn);
        };
    }

    /** A list of structs sorted as DuckDB sorts them, field by field in their declared order: an OBJECT's keys
     *  compare in name order here (measured: {i: 1, k: 10} sorts before {i: 2, k: 9}), so each element sorts as an
     *  ARRAY of its fields (compared element by element, measured) carrying the element last, read back after. */
    private SqlWriter.Piece structSort(SqlExpr xs, SqlType.Struct st, boolean desc) {
        String e = delimited(freshName("e"));
        String p = delimited(freshName("p"));
        StringBuilder keys = new StringBuilder("ARRAY_CONSTRUCT(");
        for (SqlType.Struct.Field f : st.fields()) {
            keys.append("GET(").append(e).append(", ").append(stringLit(f.name())).append("), ");
        }
        keys.append(e).append(")");
        return w -> w.append("TRANSFORM(ARRAY_SORT(TRANSFORM(").expr(xs, 0).append(", " + e + " OBJECT -> " + keys + ")"
                + (desc ? ", FALSE, FALSE" : "") + "), " + p + " ARRAY -> GET(" + p + ", " + st.fields().size() + "))");
    }

    private SqlWriter.Piece listGet(SqlExpr xs, SqlExpr i) {
        if (i instanceof SqlExpr.IntLit lit) {
            long n = lit.value();
            if (n > 0) {
                return w -> w.append("GET(").expr(xs, 0).append(", " + (n - 1) + ")");
            }
            if (n == 0) {
                return w -> w.append("NULL");
            }
            return w -> w.append("(CASE WHEN ARRAY_SIZE(").expr(xs, 0).append(") + (" + n + ") >= 0 THEN GET(")
                    .expr(xs, 0).append(", ARRAY_SIZE(").expr(xs, 0).append(") + (" + n + ")) END)");
        }
        return w -> w.append("(CASE WHEN ").expr(i, 0).append(" > 0 THEN GET(").expr(xs, 0).append(", ").expr(i, 6)
                .append(" - 1) WHEN ").expr(i, 0).append(" < 0 AND ARRAY_SIZE(").expr(xs, 0).append(") + ").expr(i, 6)
                .append(" >= 0 THEN GET(").expr(xs, 0).append(", ARRAY_SIZE(").expr(xs, 0).append(") + ").expr(i, 6)
                .append(") END)");
    }

    private static boolean negativeLiteral(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i && i.value() < 0;
    }

    /** {@code body} when {@code xs} holds an element, else NULL: DuckDB's list aggregates answer NULL over an empty
     *  (or all-NULL, or NULL) list. */
    private static SqlWriter.Piece nonEmpty(SqlExpr xs, SqlWriter.Piece body) {
        return w -> w.append("(CASE WHEN ARRAY_SIZE(ARRAY_COMPACT(").expr(xs, 0).append(")) > 0 THEN ").piece(body)
                .append(" END)");
    }

    /** The sum of {@code xs}'s non-NULL elements in {@code acc} (the call's type; its element's when unknown). */
    private SqlWriter.Piece sum(SqlExpr xs, @com.legend.base.Nullable SqlType element,
            @com.legend.base.Nullable SqlType acc) {
        SqlType t = acc != null ? acc : element;
        if (t == null) {
            throw new DialectCapability("a sum over a list of unknown element type reached Snowflake");
        }
        return reduce(xs, element, t, "0", "+");
    }

    /** {@code REDUCE(ARRAY_COMPACT(xs), init, (acc T, x E) -> acc op x)}. */
    private SqlWriter.Piece reduce(SqlExpr xs, @com.legend.base.Nullable SqlType element, SqlType acc, String init,
            String op) {
        String t = castTypeName(acc);
        String e = paramType(element);
        return w -> w.append("REDUCE(ARRAY_COMPACT(").expr(xs, 0).append("), CAST(" + init + " AS " + t + "), (\"__a\" "
                + t + ", \"__x\" " + (e == null ? t : e) + ") -> \"__a\" " + op + " \"__x\")");
    }

    /** DuckDB's list_median over numbers: the middle of the sorted non-NULL elements, or the mean of the middle two;
     *  an integer's or a Float's in DOUBLE, a decimal's in its own NUMBER, either cast to the call's type. */
    private SqlWriter.Piece median(SqlExpr xs, @com.legend.base.Nullable SqlType element,
            @com.legend.base.Nullable SqlType result) {
        if (!(isInteger(element) || element == SqlType.Scalar.DOUBLE || element instanceof SqlType.Decimal)) {
            throw new DialectCapability("a list median of " + element + " reached Snowflake (numbers only)");
        }
        String num = element instanceof SqlType.Decimal d ? castTypeName(d) : "DOUBLE";
        SqlWriter.Piece sorted = w -> w.append("ARRAY_SORT(ARRAY_COMPACT(").expr(xs, 0).append("))");
        SqlWriter.Piece n = w -> w.append("ARRAY_SIZE(ARRAY_COMPACT(").expr(xs, 0).append("))");
        SqlWriter.Piece at = w -> w.append("CAST(GET(").piece(sorted).append(", TRUNC(").piece(n)
                .append(" / 2)) AS " + num + ")");
        SqlWriter.Piece middle = w -> w.append("(CASE WHEN ").piece(n).append(" = 0 THEN NULL WHEN MOD(").piece(n)
                .append(", 2) = 1 THEN ").piece(at).append(" ELSE (CAST(GET(").piece(sorted).append(", TRUNC(")
                .piece(n).append(" / 2) - 1) AS " + num + ") + ").piece(at).append(") / 2 END)");
        String out = result != null ? castTypeName(result) : num;
        return out.equals(num) ? middle : w -> w.append("CAST(").piece(middle).append(" AS " + out + ")");
    }

    /**
     * DuckDB's list mode: the most frequent non-NULL element, the earliest-seen on a tie (its first_row). A nested
     * lambda cannot count an element's peers here (it reads no enclosing lambda's parameter, measured), so it is a
     * REDUCE over the elements sorted with their positions -- equal values adjacent, each run's first pair holding
     * its earliest position -- into {@code [best, its count, its first, run value, run count, run first]}.
     */
    private SqlWriter.Piece mode(SqlExpr xs, @com.legend.base.Nullable SqlType element) {
        String t = paramType(element);
        if (t == null) {
            throw new DialectCapability("a list mode of unknown element type reached Snowflake");
        }
        String i = delimited(freshName("i"));
        String a = delimited(freshName("a"));
        String p = delimited(freshName("p"));
        String same = "(CAST(GET(" + a + ", 4) AS INTEGER) > 0 AND GET(" + p + ", 0) = GET(" + a + ", 3))";
        String count = "IFF(" + same + ", CAST(GET(" + a + ", 4) AS INTEGER) + 1, 1)";
        String first = "IFF(" + same + ", CAST(GET(" + a + ", 5) AS INTEGER), CAST(GET(" + p + ", 1) AS INTEGER))";
        String better = "(" + count + " > CAST(GET(" + a + ", 1) AS INTEGER) OR (" + count + " = CAST(GET(" + a
                + ", 1) AS INTEGER) AND " + first + " < CAST(GET(" + a + ", 2) AS INTEGER)))";
        String step = "ARRAY_CONSTRUCT(IFF(" + better + ", GET(" + p + ", 0), GET(" + a + ", 0)), IFF(" + better + ", "
                + count + ", CAST(GET(" + a + ", 1) AS INTEGER)), IFF(" + better + ", " + first + ", CAST(GET(" + a
                + ", 2) AS INTEGER)), GET(" + p + ", 0), " + count + ", " + first + ")";
        SqlWriter.Piece compact = w -> w.append("ARRAY_COMPACT(").expr(xs, 0).append(")");
        return w -> w.append("CAST(GET(REDUCE(ARRAY_SORT(TRANSFORM(ARRAY_GENERATE_RANGE(0, ARRAY_SIZE(").piece(compact)
                .append(")), " + i + " INTEGER -> ARRAY_CONSTRUCT(CAST(GET(").piece(compact)
                .append(", " + i + ") AS " + t + "), " + i + "))), ARRAY_CONSTRUCT(NULL, 0, 0, NULL, 0, 0), (" + a
                        + " ARRAY, " + p + " ARRAY) -> " + step + "), 0) AS " + t + ")");
    }

    /** DuckDB's list quantile_cont: over the sorted non-NULL elements, RN = (n - 1) * q in DOUBLE, the value at
     *  floor(RN) plus (RN - floor(RN)) of the step to the value at ceil(RN). */
    private SqlWriter.Piece quantile(SqlExpr xs, @com.legend.base.Nullable SqlType element, SqlExpr q) {
        if (!(isInteger(element) || element == SqlType.Scalar.DOUBLE || element instanceof SqlType.Decimal)) {
            throw new DialectCapability("a list quantile of " + element + " reached Snowflake (numbers only)");
        }
        SqlWriter.Piece sorted = w -> w.append("ARRAY_SORT(ARRAY_COMPACT(").expr(xs, 0).append("))");
        SqlWriter.Piece n = w -> w.append("ARRAY_SIZE(ARRAY_COMPACT(").expr(xs, 0).append("))");
        SqlWriter.Piece rn = w -> w.append("(CAST(").piece(n).append(" - 1 AS DOUBLE) * CAST(").expr(q, 0)
                .append(" AS DOUBLE))");
        SqlWriter.Piece lo = w -> w.append("CAST(GET(").piece(sorted).append(", CAST(FLOOR(").piece(rn)
                .append(") AS INTEGER)) AS DOUBLE)");
        SqlWriter.Piece hi = w -> w.append("CAST(GET(").piece(sorted).append(", CAST(CEIL(").piece(rn)
                .append(") AS INTEGER)) AS DOUBLE)");
        return w -> w.append("(CASE WHEN ").piece(n).append(" = 0 THEN NULL ELSE ").piece(lo).append(" + (")
                .piece(hi).append(" - ").piece(lo).append(") * (").piece(rn).append(" - FLOOR(").piece(rn)
                .append(")) END)");
    }

    /** exists([]) is false: the kept elements are some. */
    @Override
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        Ranged r = ranged(args.get(0), (SqlExpr.Lambda) args.get(1), b -> b);
        return writer.append("COALESCE(ARRAY_SIZE(FILTER(").piece(r.over()).append(", ").piece(r.lambda())
                .append(")) > 0, FALSE)");
    }

    /** forAll([]) is true: no element is false (a NULL predicate is no answer, as DuckDB's list_bool_and). */
    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        Ranged r = ranged(args.get(0), (SqlExpr.Lambda) args.get(1), b -> w -> w.append("NOT (").piece(b).append(")"));
        return writer.append("COALESCE(ARRAY_SIZE(FILTER(").piece(r.over()).append(", ").piece(r.lambda())
                .append(")) = 0, TRUE)");
    }

    /** No duplicates iff the distinct non-NULL count is the count (DuckDB's len(list_distinct(x)) = len(x)). */
    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("COALESCE(ARRAY_SIZE(ARRAY_DISTINCT(ARRAY_COMPACT(").expr(args.get(0), 0)
                .append("))) = ARRAY_SIZE(").expr(args.get(0), 0).append("), TRUE)");
    }

    /** DuckDB's list_contains: NULL for a NULL needle or list; ARRAY_CONTAINS finds an undefined element for a NULL
     *  needle (measured), so the needle is tested first. */
    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        return writer.append("(CASE WHEN ").expr(m.needle(), 4).append(" IS NULL THEN NULL ELSE ARRAY_CONTAINS(TO_VARIANT(")
                .expr(m.needle(), 0).append("), ").expr(m.collection(), 0).append(") END)");
    }

    /** A named aggregate over a list's non-NULL elements, in list order. */
    @Override
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        SqlExpr xs = rc.collection();
        SqlType element = listElement(xs);
        SqlType result = typeOf(rc);
        return switch (rc.reducer()) {
            case SUM -> writer.piece(nonEmpty(xs, sum(xs, element, result)));
            case MIN -> writer.piece(typed(w -> w.append("ARRAY_MIN(").expr(xs, 0).append(")"),
                    element != null ? element : result));
            case MAX -> writer.piece(typed(w -> w.append("ARRAY_MAX(").expr(xs, 0).append(")"),
                    element != null ? element : result));
            case AVG -> writer.piece(nonEmpty(xs, w -> w.piece(sum(xs, SqlType.Scalar.DOUBLE, SqlType.Scalar.DOUBLE))
                    .append(" / ARRAY_SIZE(ARRAY_COMPACT(").expr(xs, 0).append("))")));
            case COUNT -> writer.append("ARRAY_SIZE(ARRAY_COMPACT(").expr(xs, 0).append("))");
            case MEDIAN -> writer.piece(median(xs, element, result));
            case MODE -> writer.piece(mode(xs, element));
            case QUANTILE_CONT -> {
                if (rc.extras().size() != 1) {
                    throw new DialectCapability("a list quantile of " + rc.extras().size()
                            + " extra arguments reached Snowflake");
                }
                yield writer.piece(quantile(xs, element, rc.extras().get(0)));
            }
            case BOOL_AND -> writer.piece(nonEmpty(xs, w -> w.append("NOT ARRAY_CONTAINS(TO_VARIANT(FALSE), ")
                    .expr(xs, 0).append(")")));
            case BOOL_OR -> writer.piece(nonEmpty(xs, w -> w.append("ARRAY_CONTAINS(TO_VARIANT(TRUE), ").expr(xs, 0)
                    .append(")")));
            // joinStrings: the elements' text in list order; NULL over no element, as DuckDB's string_agg
            case STRING_AGG -> {
                if (rc.extras().size() != 1) {
                    throw new DialectCapability("a list string_agg of " + rc.extras().size()
                            + " extra arguments reached Snowflake");
                }
                yield writer.piece(nonEmpty(xs, w -> w.append("ARRAY_TO_STRING(ARRAY_COMPACT(").expr(xs, 0)
                        .append("), ").expr(rc.extras().get(0), 0).append(")")));
            }
            case STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP, VARIANCE, STDDEV -> writer.piece(moments(xs, rc.reducer()));
            case ANY_VALUE, LIST, QUANTILE_DISC, CORR,
                 COVAR_SAMP, COVAR_POP, ARG_MAX, ARG_MIN, ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK,
                 CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE -> throw new DialectCapability(
                    "a list reduction '" + rc.reducer() + "' reached Snowflake, which reduces a list only with"
                    + " REDUCE and the ARRAY_* functions");
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY ->
                    throw new IllegalStateException("lowering marker " + rc.reducer() + " reached the renderer");
        };
    }

    /**
     * A list's variance or standard deviation (a correlated aggregate over its FLATTEN is an "Unsupported subquery
     * type", measured): DuckDB's own Welford update, so the same doubles, as a REDUCE over the non-NULL elements
     * into {@code [n, mean, M2]}. A sample's needs two elements (NULL otherwise); a population's one, 0 alone.
     */
    private SqlWriter.Piece moments(SqlExpr xs, SqlAgg.Fn fn) {
        String n = "CAST(GET(\"__a\", 0) AS DOUBLE)";
        String mean = "CAST(GET(\"__a\", 1) AS DOUBLE)";
        String m2 = "CAST(GET(\"__a\", 2) AS DOUBLE)";
        String count = "(" + n + " + 1)";
        String next = "(" + mean + " + (\"__x\" - " + mean + ") / " + count + ")";
        String step = "ARRAY_CONSTRUCT(" + count + ", " + next + ", " + m2 + " + (\"__x\" - " + next + ") * (\"__x\" - "
                + mean + "))";
        java.util.function.IntFunction<SqlWriter.Piece> read = i -> w -> w.append("CAST(GET(REDUCE(ARRAY_COMPACT(")
                .expr(xs, 0).append("), ARRAY_CONSTRUCT(0, 0, 0), (\"__a\" ARRAY, \"__x\" DOUBLE) -> " + step + "), "
                        + i + ") AS DOUBLE)");
        boolean sample = fn == SqlAgg.Fn.STDDEV_SAMP || fn == SqlAgg.Fn.VAR_SAMP || fn == SqlAgg.Fn.VARIANCE
                || fn == SqlAgg.Fn.STDDEV;
        boolean root = fn == SqlAgg.Fn.STDDEV_SAMP || fn == SqlAgg.Fn.STDDEV_POP || fn == SqlAgg.Fn.STDDEV;
        SqlWriter.Piece variance = sample
                ? w -> w.append("(CASE WHEN ").piece(read.apply(0)).append(" > 1 THEN ").piece(read.apply(2))
                        .append(" / (").piece(read.apply(0)).append(" - 1) END)")
                : w -> w.append("(CASE WHEN ").piece(read.apply(0)).append(" > 1 THEN ").piece(read.apply(2))
                        .append(" / ").piece(read.apply(0)).append(" WHEN ").piece(read.apply(0))
                        .append(" = 1 THEN 0E0 END)");
        return root ? w -> w.append("SQRT(").piece(variance).append(")") : variance;
    }

    /** Pure's fold, {@code (element, acc)}, as {@code REDUCE(xs, init, (acc T, x E) -> body)}: the accumulator typed
     *  as the fold's result (a list one is an ARRAY); an empty or NULL list folds to the init. */
    @Override
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        if (!(f.type() instanceof com.legend.sql.TypeFact.Typed t)) {
            throw new DialectCapability("a fold of unknown type reached Snowflake");
        }
        String type = castTypeName(t.type());
        SqlType element = listElement(f.source());
        SqlExpr.Lambda l = f.lambda();
        List<Param> params = List.of(new Param(l.params().get(1), type), new Param(l.params().get(0),
                paramType(element)));
        return writer.append("REDUCE(COALESCE(").expr(f.source(), 0).append(", ARRAY_CONSTRUCT()), CAST(")
                .expr(f.init(), 0).append(" AS " + type + "), ")
                .piece(lambdaOf(params, Map.of(), w -> w.append("CAST(").expr(l.body(), 0).append(" AS " + type + ")")))
                .append(")");
    }

    /** A select-list UNNEST the passes did not place (inside an expression, or under a GROUP BY). */
    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        throw new DialectCapability("an UNNEST Snowflake cannot place reached the writer: it explodes only as a"
                + " lateral FLATTEN of a select's FROM");
    }

    /** Pure's splitPart: the non-empty tokens, 1-based, NULL past the end. */
    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        String t = delimited(freshName("t"));
        return writer.append("(CASE WHEN ").expr(a.get(2), 0).append(" > 0 THEN CAST(GET(FILTER(").piece(split(a.get(0),
                a.get(1))).append(", " + t + " VARCHAR -> " + t + " <> ''), ").expr(a.get(2), 6)
                .append(" - 1) AS VARCHAR) END)");
    }

    /** DuckDB's string_split: a literal delimiter; the empty one splits into characters (SPLIT keeps the whole). */
    private SqlWriter.Piece split(SqlExpr s, SqlExpr d) {
        if (d instanceof SqlExpr.StringLit lit && lit.value().isEmpty()) {
            return w -> w.append("REGEXP_SUBSTR_ALL(").expr(s, 0).append(", '.', 1, 1, 's')");
        }
        return w -> w.append("SPLIT(").expr(s, 0).append(", ").expr(d, 0).append(")");
    }

    // ==================================================================
    // Structs and maps: OBJECTs
    // ==================================================================

    /** OBJECT_CONSTRUCT_KEEP_NULL: a NULL field is kept as JSON null (OBJECT_CONSTRUCT drops it, measured). */
    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit st) {
        return writer.append("OBJECT_CONSTRUCT_KEEP_NULL(")
                .join(st.fields(), ", ", (w, f) -> w.append(stringLit(f.name())).append(", ").expr(f.value(), 0))
                .append(")");
    }

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
        if (type == null) {
            throw new DialectCapability("a field '" + g.field() + "' of unknown type reached Snowflake");
        }
        return writer.piece(typed(w -> w.append("GET(").expr(g.source(), 0).append(", ").append(stringLit(g.field()))
                .append(")"), type));
    }

    /** The struct with one field set (a NULL kept as JSON null: OBJECT_INSERT drops a NULL value's key, measured). */
    @Override
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("OBJECT_INSERT(").expr(args.get(0), 0).append(", ").expr(args.get(1), 0).append(", ")
                .piece(keepNull(args.get(2))).append(", TRUE)");
    }

    // ==================================================================
    // Variant (VARIANT, Snowflake's own semi-structured type)
    // ==================================================================

    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() != 1) {
            throw new DialectCapability("toVariant of " + a.size() + " arguments reached Snowflake");
        }
        return writer.append("TO_VARIANT(").expr(a.get(0), 0).append(")");
    }

    /** A key (a member, NULL when absent or not an object) or a 0-based index (-1 the last, past the end NULL) of a
     *  variant -- GET raises on a negative index (measured); {@code '$'} is the value, {@code '$.*'} its members. */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        SqlExpr v = args.get(0);
        SqlExpr key = args.get(1);
        if (key instanceof SqlExpr.StringLit root && "$".equals(root.value())) {
            return writer.expr(v, 0);
        }
        if (key instanceof SqlExpr.StringLit all && "$.*".equals(all.value())) {
            String k = delimited(freshName("k"));
            return writer.append("(CASE WHEN IS_OBJECT(TO_VARIANT(").expr(v, 0).append(")) THEN TRANSFORM(OBJECT_KEYS(")
                    .expr(v, 0).append("), " + k + " VARCHAR -> GET(").expr(v, 0).append(", " + k
                            + ")) ELSE AS_ARRAY(TO_VARIANT(").expr(v, 0).append(")) END)");
        }
        if (isText(key)) {
            return writer.append("GET(").expr(v, 0).append(", ").expr(key, 0).append(")");
        }
        // a literal index is checked when the statement compiles, in a branch never taken too (measured: "array
        // index -1 is off limits"): its branch is chosen here
        if (key instanceof SqlExpr.IntLit lit) {
            long n = lit.value();
            return n >= 0 ? writer.append("GET(").expr(v, 0).append(", " + n + ")")
                    : writer.append("(CASE WHEN ARRAY_SIZE(").expr(v, 0).append(") + (" + n + ") >= 0 THEN GET(")
                            .expr(v, 0).append(", ARRAY_SIZE(").expr(v, 0).append(") + (" + n + ")) END)");
        }
        return writer.append("(CASE WHEN ").expr(key, 0).append(" >= 0 THEN GET(").expr(v, 0).append(", ").expr(key, 0)
                .append(") WHEN ARRAY_SIZE(").expr(v, 0).append(") + ").expr(key, 6).append(" >= 0 THEN GET(")
                .expr(v, 0).append(", ARRAY_SIZE(").expr(v, 0).append(") + ").expr(key, 6).append(") END)");
    }

    /** A variant array's elements (NULL when it is no array); a list already is one. */
    @Override
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        if (listElement(args.get(0)) != null) {
            return writer.expr(args.get(0), 0);
        }
        return writer.append("AS_ARRAY(TO_VARIANT(").expr(args.get(0), 0).append("))");
    }

    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        SqlType target = c.target();
        SqlExpr v = c.value();
        if (target == SqlType.Scalar.TEMPORAL_TEXT || target == SqlType.Scalar.DECIMAL_TEXT) {
            return super.variantAwareCast(writer, c);
        }
        // text parses into a VARIANT (DuckDB's CAST(text AS JSON)); any other value converts
        if (target == SqlType.Scalar.JSON) {
            return isText(v) ? writer.append("PARSE_JSON(").expr(v, 0).append(")")
                    : writer.append("TO_VARIANT(").expr(v, 0).append(")");
        }
        if (isJson(v)) {
            boolean member = v instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET;
            // the whole value as text is its JSON (DuckDB's cast); a member's text is its own (->>): a string unquoted
            if (target == SqlType.Scalar.VARCHAR && !member) {
                return writer.append("TO_JSON(").expr(v, 0).append(")");
            }
            if (target instanceof SqlType.Array) {
                return writer.append("AS_ARRAY(").expr(v, 0).append(")");
            }
            if (target instanceof SqlType.Map || target instanceof SqlType.Struct) {
                return writer.append("AS_OBJECT(").expr(v, 0).append(")");
            }
            return super.variantAwareCast(writer, c);
        }
        if (target instanceof SqlType.Array arr) {
            SqlType from = listElement(v);
            if (arr.element().equals(from) || arr.element() == SqlType.Scalar.JSON) {
                return writer.expr(v, 0);
            }
            // each element converted (the elements are VARIANTs: a read casts, but the cell must hold the type)
            String e = delimited(freshName("c"));
            return writer.append("TRANSFORM(").expr(v, 0).append(", " + e + " -> CAST(" + e + " AS "
                    + castTypeName(arr.element()) + "))");
        }
        if (target instanceof SqlType.Map || target instanceof SqlType.Struct) {
            return writer.expr(v, 0);
        }
        // text to an instant: DuckDB reads a +HHMM offset, Snowflake only +HH:MM ('...-0500' is "not recognized",
        // measured)
        if (target == SqlType.Scalar.TIMESTAMPTZ && isText(v)) {
            return writer.append("CAST(REGEXP_REPLACE(").expr(v, 0)
                    .append(", '([+-][0-9]{2})([0-9]{2})$', '\\\\1:\\\\2') AS TIMESTAMP_LTZ)");
        }
        // DuckDB's Float text keeps an integral value's '.0' (-1.0, where Snowflake writes -1, measured)
        if (target == SqlType.Scalar.VARCHAR && isDouble(v)) {
            writer.append("REGEXP_REPLACE(");
            super.variantAwareCast(writer, c);
            return writer.append(", '^(-?[0-9]+)$', '\\\\1.0')");
        }
        // DuckDB's timestamp text: the seconds' fraction to its last nonzero digit, none when zero (measured form)
        if (target == SqlType.Scalar.VARCHAR && isTimestamp(v)) {
            return writer.append("REGEXP_REPLACE(TO_CHAR(").expr(v, 0)
                    .append(", 'YYYY-MM-DD HH24:MI:SS.FF6'), '\\\\.?0+$', '')");
        }
        return super.variantAwareCast(writer, c);
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
            case AND, OR, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES ->
                    infix(writer, c, parentPrec);
            // the shared spellings (the base's named helpers)
            case NOT -> notPrefix(writer, c.args(), parentPrec);
            case NEGATE -> negation(writer, c.args());
            case IS_NULL -> isNullTest(writer, c.args());
            case IS_NOT_NULL -> isNotNullTest(writer, c.args());
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> distinctFrom(writer, c.args());
            case NULL_SAFE_EQUAL -> notDistinctFrom(writer, c.args());
            case XOR -> xorAsAndOr(writer, c.args(), parentPrec);
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
            case BOOL_TO_TEXT -> castAs(writer, c.args().get(0), "VARCHAR");
            // Pure's divide is a Float division: both operands DOUBLE
            case DIVIDE -> divideAsDoubles(writer, c.args());
            case MOD -> modFloored(writer, c.args());
            case REM -> modTruncated(writer, c.args());
            case FLOAT_CANON -> floatCanonByText(writer, c.args(), parentPrec);
            // the hooks: this dialect's own (or the base's refusal)
            case ROUND -> roundHalfEven(writer, c.args());
            case HASH -> hashSigned(writer, c.args());
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), c.args());
            case STRUCT_INSERT -> structInsert(writer, c.args());
            case PURE_SPLIT_PART -> splitPartCall(writer, c.args());
            case LIST_EXISTS -> listExists(writer, c.args());
            case ALL_DISTINCT -> allDistinct(writer, c.args());
            case LIST_FOR_ALL -> listForAll(writer, c.args());
            case TO_VARIANT -> variantConstruct(writer, c.args());
            case VARIANT_ELEMENTS -> variantElements(writer, c.args());
            case VARIANT_GET -> variantGet(writer, c.args());
            case UNNEST -> unnestProjection(writer, c.args());
            // this database's plain spellings: name(args)
            case ABS -> writer.function("abs", c.args());
            case ASCII_CODE -> writer.function("ascii", c.args());
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
            case FLOOR_RAW -> writer.function("floor", c.args());
            case GREATEST -> writer.function("greatest_ignore_nulls", c.args());
            case LEAST -> writer.function("least_ignore_nulls", c.args());
            case LEFT -> writer.function("left", c.args());
            case LENGTH -> writer.function("length", c.args());
            case LEVENSHTEIN -> writer.function("editdistance", c.args());
            case LN -> writer.function("ln", c.args());
            case LOWER -> writer.function("lower", c.args());
            case LTRIM -> writer.function("ltrim", c.args());
            case MAKE_DATE -> writer.function("date_from_parts", c.args());
            case MD5 -> writer.function("md5", c.args());
            case POW -> writer.function("power", c.args());
            case RADIANS -> writer.function("radians", c.args());
            case REPEAT_STR -> writer.function("repeat", c.args());
            case REPLACE -> writer.function("replace", c.args());
            case TRANSLATE -> writer.function("translate", c.args());
            case REVERSE_STRING -> writer.function("reverse", c.args());
            case RIGHT -> writer.function("right", c.args());
            case RTRIM -> writer.function("rtrim", c.args());
            case SHA1 -> writer.function("sha1", c.args());
            case SIN -> writer.function("sin", c.args());
            case SINH -> writer.function("sinh", c.args());
            case SPLIT_PART -> writer.function("split_part", c.args());
            case SQRT -> writer.function("sqrt", c.args());
            case STARTS_WITH -> writer.function("startswith", c.args());
            case ENDS_WITH -> writer.function("endswith", c.args());
            case SUBSTRING -> writer.function("substr", c.args());
            case TAN -> writer.function("tan", c.args());
            case TANH -> writer.function("tanh", c.args());
            case TRIM -> writer.function("trim", c.args());
            case UPPER -> writer.function("upper", c.args());
            case LIST_LENGTH, JSON_ARRAY_LENGTH -> writer.function("array_size", c.args());
            case LIST_REVERSE -> writer.function("array_reverse", c.args());
            case MAP_KEYS -> writer.function("object_keys", c.args());
            case ENCODE_BASE64 -> writer.function("base64_encode", c.args());
            case DECODE_BASE64 -> writer.function("base64_decode_string", c.args());

            // a list parameter is bound as one array on the base (= ANY(?)): Snowflake binds none
            case IN -> inArm(writer, c, parentPrec);
            // NULL-ignoring, as DuckDB's concat (|| and CONCAT are NULL if any argument is, measured)
            case CONCAT -> writer.piece(concat(flattenConcat(a)));
            case CONCAT_JOIN -> writer.piece(concat(a));
            // the engine's out-of-domain answer is NaN; Snowflake raises (measured: acos(2))
            case ACOS, ASIN -> acosArm(writer, c);
            case LOG10 -> writer.append("LOG(10, ").expr(a.get(0), 0).append(")");
            // Pure's divide-with-scale is HALF_UP: ROUND over a NUMBER (half away from zero, measured); over a FLOAT
            // in binary, as DuckDB's round (Snowflake's ROUND(FLOAT) rounds the decimal text: 2.675 is 2.68)
            case ROUND_HALF_UP -> roundHalfUp(writer, c);
            // DuckDB's // truncates toward zero; x // 0 is NULL (DIV0NULL answers 0, measured)
            case INT_DIVIDE -> intDivide(writer, c);
            case BIT_NOT -> writer.append("BITNOT(").expr(a.get(0), 0).append(")");
            // a 64-bit integer text: CAST rounds a decimal text ('1.5' is 2, measured), so the text is checked first
            case PARSE_INT -> writer.append("CAST(CASE WHEN REGEXP_LIKE(").expr(a.get(0), 0).append(", ")
                    .append(stringLit("\\s*[-+]?[0-9]+\\s*")).append(") THEN ").expr(a.get(0), 0).append(" ELSE ")
                    .expr(a.get(0), 0).append(" || ' is not an integer' END AS BIGINT)");
            // an ISO text with an offset is its UTC instant (a TIMESTAMP_NTZ cast drops the offset, measured)
            case PARSE_DATE -> writer.append("CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(").expr(a.get(0), 0)
                    .append(")) AS TIMESTAMP_NTZ)");
            case ERROR -> writer.piece(raise(a, null));

            // ---- strings
            // MATCHES is the PARTIAL test (REGEXP_LIKE is anchored, measured); REGEXP_FULL_MATCH is REGEXP_LIKE
            case MATCHES -> matches(writer, a);
            case REGEXP_FULL_MATCH -> fullMatch(writer, a);
            case REGEXP_EXTRACT -> regexpExtract(writer, a);
            case REGEXP_EXTRACT_ALL -> regexpExtractAll(writer, a);
            case REGEXP_REPLACE -> regexpReplace(writer, a);
            case SPLIT -> writer.piece(split(a.get(0), a.get(1)));
            case STRPOS -> writer.append("CHARINDEX(").expr(a.get(1), 0).append(", ").expr(a.get(0), 0).append(")");
            case SHA256 -> writer.append("SHA2(").expr(a.get(0), 0).append(", 256)");
            case GUID -> writer.append("UUID_STRING()");
            // an integer of 53 bits over 2^53, in [0, 1): UNIFORM's range is inclusive, so a FLOAT one could draw
            // 1 (Snowflake's documented UNIFORM, an integer one between integer bounds; the independent review,
            // 2026-10-18)
            case RANDOM -> writer.append("(UNIFORM(0, 9007199254740991, RANDOM()) / 9007199254740992::FLOAT)");
            case FORMAT -> writer.piece(format(a));
            case JARO_WINKLER -> writer.piece(jaroWinkler(a.get(0), a.get(1)));
            case CURRENT_USER_FN -> writer.append("CURRENT_USER()");

            // ---- temporal
            case NOW -> writer.append("CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)");
            case EXTRACT -> writer.piece(extract(a));
            case DATE_TRUNC -> writer.piece(dateTrunc(a));
            // seconds as a DOUBLE: whole seconds, then the fraction's microseconds
            case MAKE_TIMESTAMP -> makeTimestamp(writer, c);
            // DuckDB's date + interval is a TIMESTAMP at every unit (DATEADD of days keeps a DATE, measured)
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> writer.append("CAST(DATEADD(")
                    .append(intervalUnit(part(a.get(0)))).append(", ").expr(a.get(1), 0)
                    .append(", ").expr(a.get(2), 0).append(") AS TIMESTAMP_NTZ)");
            case DATE_DIFF -> writer.piece(dateDiff(a));
            case TIME_BUCKET -> writer.piece(timeBucket(a));
            // TIMESTAMP_FROM_PARTS(date, time): the date-time at that time of that day
            case DATE_AT_TIME -> writer.function("TIMESTAMP_NTZ_FROM_PARTS", c.args());
            // DuckDB's timezone(zone, t): a naive t read in the zone (to its instant); an instant to the zone's time
            case TIMEZONE -> instant(a.get(1))
                    ? writer.append("CONVERT_TIMEZONE('UTC', ").expr(a.get(0), 0).append(", CAST(").expr(a.get(1), 0)
                            .append(" AS TIMESTAMP_NTZ))")
                    : writer.append("CAST(CONVERT_TIMEZONE(").expr(a.get(0), 0).append(", 'UTC', CAST(")
                            .expr(a.get(1), 0).append(" AS TIMESTAMP_NTZ)) AS TIMESTAMP_LTZ)");
            // DuckDB's epoch(ts) is DOUBLE seconds; epoch_ms truncates toward zero (EXTRACT floors, measured)
            case EPOCH_SECONDS -> writer.append("(CAST(DATE_PART(EPOCH_MICROSECOND, ").piece(naive(a.get(0)))
                    .append(") AS DOUBLE) / 1000000)");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability("a nanosecond epoch: Snowflake keeps"
                    + " microseconds (the Cypher island's nanosecond leg is DuckDB's)");
            case EPOCH_MS -> writer.append("CAST(TRUNC(DATE_PART(EPOCH_MICROSECOND, ").piece(naive(a.get(0)))
                    .append(") / 1000) AS BIGINT)");
            case FROM_EPOCH_SECONDS -> writer.append("TO_TIMESTAMP_NTZ(CAST(ROUND(").expr(a.get(0), 6)
                    .append(" * 1000000) AS BIGINT), 6)");
            case FROM_EPOCH_MS -> writer.append("TO_TIMESTAMP_NTZ(CAST(").expr(a.get(0), 0).append(" AS BIGINT), 3)");
            // DAYNAME is 'Sun' here: the full name by the ISO weekday
            case DAYNAME -> writer.piece(weekdayName(naive(a.get(0))));
            case MONTHNAME -> writer.append("TO_CHAR(").piece(naive(a.get(0))).append(", 'MMMM')");
            case STRFTIME -> strftimeArm(writer, c);
            case STRPTIME -> strptime(writer, c);

            // ---- lists
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION, LIST_FLATTEN, LIST_ZIP,
                 LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, LIST_BOOL_AND,
                 LIST_BOOL_OR, TYPEOF -> list(writer, c.fn(), a, typeOf(c));
            // DuckDB's range: [start, stop) by step, start 0 and step 1 by default -- ARRAY_GENERATE_RANGE's own
            case RANGE_FN -> a.size() == 1
                    ? writer.append("ARRAY_GENERATE_RANGE(0, ").expr(a.get(0), 0).append(")")
                    : writer.function("ARRAY_GENERATE_RANGE", a);
            case REPEAT_VALUE -> repeatValue(writer, c);

            // ---- maps: OBJECTs (string keys only)
            case MAP_EMPTY -> writer.append("OBJECT_CONSTRUCT()");
            case MAP_FROM_LISTS -> mapFromLists(writer, c);
            // DuckDB's map_from_entries: each entry a struct of (key, value), by position whatever their names
            case MAP_FROM_ENTRIES -> mapFromEntries(writer, c);
            // DuckDB's map_extract: a list of the key's value, empty when absent
            case MAP_EXTRACT -> writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL WHEN")
                    .append(" ARRAY_CONTAINS(TO_VARIANT(").expr(a.get(1), 0).append("), OBJECT_KEYS(")
                    .expr(a.get(0), 0).append(")) THEN ARRAY_CONSTRUCT(COALESCE(GET(").expr(a.get(0), 0).append(", ")
                    .expr(a.get(1), 0).append("), PARSE_JSON('null'))) ELSE ARRAY_CONSTRUCT() END)");
            // in the keys' order (OBJECT_KEYS sorts them, measured; DuckDB's map keeps insertion order)
            case MAP_VALUES -> mapValues(writer, c);
            // the later map's keys win, as DuckDB's map_concat
            case MAP_CONCAT -> mapConcat(writer, c);

            // ---- variant
            // in DuckDB's json_type vocabulary (NULL, VARCHAR, BIGINT, DOUBLE, BOOLEAN, ARRAY, OBJECT); Snowflake
            // reads '1.0' as an INTEGER (measured), so a whole decimal is a BIGINT here
            case JSON_TYPE -> writer.append("(CASE WHEN ").expr(a.get(0), 4).append(" IS NULL THEN NULL ELSE DECODE(TYPEOF(")
                    .append("TO_VARIANT(").expr(a.get(0), 0).append(")), 'NULL_VALUE', 'NULL', 'BOOLEAN', 'BOOLEAN',")
                    .append(" 'INTEGER', 'BIGINT', 'DECIMAL', 'DOUBLE', 'DOUBLE', 'DOUBLE', 'ARRAY', 'ARRAY',")
                    .append(" 'OBJECT', 'OBJECT', 'VARCHAR') END)");
            case JSON_PRETTY -> throw wall(c.fn(), "Snowflake has no JSON pretty-printer in SQL");
            // RFC 7386 with a literal object patch of non-null values: each key replaced (OBJECT_INSERT)
            case JSON_MERGE_PATCH -> jsonMergePatch(writer, c);
        };
    }

    /** The {@code IN} arm of {@link #call}. */
    private SqlWriter inArm(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        if (a.size() == 2 && a.get(1) instanceof SqlExpr.PlanParam p) {
            throw new DialectCapability("list parameter '" + p.name() + "' reached Snowflake, which binds"
                    + " no array (= ANY(?))");
        }
        return inList(writer, a, parentPrec);
    }

    /** The {@code ACOS}, {@code ASIN} arm of {@link #call}. */
    private SqlWriter acosArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String f = c.fn() == SqlFn.ACOS ? "ACOS" : "ASIN";
        return writer.append("(CASE WHEN (").expr(a.get(0), 0).append(") BETWEEN -1 AND 1 THEN " + f + "(")
                .expr(a.get(0), 0).append(") ELSE CAST('NaN' AS DOUBLE) END)");
    }

    /** The {@code ROUND_HALF_UP} arm of {@link #call}. */
    private SqlWriter roundHalfUp(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!isDouble(a.get(0))) {
            return writer.function("ROUND", a);
        }
        SqlWriter.Piece scale = a.size() > 1 ? w -> w.append("POWER(10, ").expr(a.get(1), 0).append(")") : null;
        SqlWriter.Piece v = scale == null ? w -> w.expr(a.get(0), 6)
                : w -> w.append("(").expr(a.get(0), 6).append(" * ").piece(scale).append(")");
        writer.append("(SIGN(").piece(v).append(") * FLOOR(ABS(").piece(v).append(") + 0.5)");
        return (scale == null ? writer : writer.append(" / ").piece(scale)).append(")");
    }

    /** The {@code INT_DIVIDE} arm of {@link #call}. */
    private SqlWriter intDivide(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        boolean integers = isInteger(typeOf(a.get(0))) && isInteger(typeOf(a.get(1)));
        writer.append(integers ? "CAST(TRUNC(" : "TRUNC(").expr(a.get(0), 6).append(" / NULLIF(")
                .expr(a.get(1), 0).append(", 0))");
        return integers ? writer.append(" AS BIGINT)") : writer;
    }

    /** The {@code MAKE_TIMESTAMP} arm of {@link #call}. */
    private SqlWriter makeTimestamp(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() != 6) {
            throw wall(c.fn(), "make_timestamp of " + a.size() + " arguments");
        }
        return writer.append("DATEADD(MICROSECOND, ROUND(").expr(a.get(5), 6)
                .append(" * 1000000), TIMESTAMP_FROM_PARTS(").list(a.subList(0, 5)).append(", 0))");
    }

    /** The {@code STRFTIME} arm of {@link #call}. */
    private SqlWriter strftimeArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit fl)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        return writer.piece(strftime(a.get(0), fl));
    }

    /** The {@code STRPTIME} arm of {@link #call}. */
    private SqlWriter strptime(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit)) {
            throw wall(c.fn(), "a date format that is not a format literal: its codes are DuckDB's");
        }
        return writer.append("TO_TIMESTAMP_NTZ(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0).append(")");
    }

    /** The {@code REPEAT_VALUE} arm of {@link #call}. */
    private SqlWriter repeatValue(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String r = freshName("r");
        return writer.append("TRANSFORM(ARRAY_GENERATE_RANGE(0, ").expr(a.get(1), 0).append("), ")
                .piece(lambdaOf(List.of(new Param(r, "INTEGER")), Map.of(), w -> w.expr(a.get(0), 0)))
                .append(")");
    }

    /** The {@code MAP_FROM_LISTS} arm of {@link #call}. */
    private SqlWriter mapFromLists(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (listElement(a.get(0)) != SqlType.Scalar.VARCHAR) {
            throw wall(c.fn(), "a map keyed by " + listElement(a.get(0)) + " (an OBJECT's keys are text)");
        }
        String m = freshName("m");
        String i = freshName("i");
        return writer.append("REDUCE(ARRAY_GENERATE_RANGE(0, ARRAY_SIZE(").expr(a.get(0), 0)
                .append(")), OBJECT_CONSTRUCT(), ")
                .piece(lambdaOf(List.of(new Param(m, "OBJECT"), new Param(i, "INTEGER")), Map.of(),
                        w -> w.append("OBJECT_INSERT(" + delimited(m) + ", CAST(GET(").expr(a.get(0), 0)
                                .append(", " + delimited(i) + ") AS VARCHAR), COALESCE(GET(")
                                .expr(a.get(1), 0).append(", " + delimited(i) + "), PARSE_JSON('null')), TRUE)")))
                .append(")");
    }

    /** The {@code MAP_FROM_ENTRIES} arm of {@link #call}. */
    private SqlWriter mapFromEntries(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(listElement(a.get(0)) instanceof SqlType.Struct entry) || entry.fields().size() != 2
                || entry.fields().get(0).type() != SqlType.Scalar.VARCHAR) {
            throw wall(c.fn(), "entries of " + listElement(a.get(0)) + " (an OBJECT's keys are text)");
        }
        String m = freshName("m");
        String e = freshName("e");
        String key = stringLit(entry.fields().get(0).name());
        String value = stringLit(entry.fields().get(1).name());
        return writer.append("REDUCE(COALESCE(").expr(a.get(0), 0).append(", ARRAY_CONSTRUCT()), OBJECT_CONSTRUCT(), ")
                .piece(lambdaOf(List.of(new Param(m, "OBJECT"), new Param(e, "OBJECT")), Map.of(),
                        w -> w.append("OBJECT_INSERT(" + delimited(m) + ", CAST(GET(" + delimited(e) + ", " + key
                                + ") AS VARCHAR), COALESCE(GET(" + delimited(e) + ", " + value
                                + "), PARSE_JSON('null')), TRUE)")))
                .append(")");
    }

    /** The {@code MAP_VALUES} arm of {@link #call}. */
    private SqlWriter mapValues(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String k = freshName("k");
        return writer.append("TRANSFORM(OBJECT_KEYS(").expr(a.get(0), 0).append("), ")
                .piece(lambdaOf(List.of(new Param(k, "VARCHAR")), Map.of(), w -> w.append("GET(")
                        .expr(a.get(0), 0).append(", " + delimited(k) + ")")))
                .append(")");
    }

    /** RFC 7386 with a literal object patch of non-null values: each key replaced (OBJECT_INSERT). */
    private SqlWriter jsonMergePatch(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.JsonObject patch) || patch.kv().size() % 2 != 0) {
            throw wall(c.fn(), "a merge patch that is not a literal object");
        }
        SqlWriter.Piece acc = w -> w.expr(a.get(0), 0);
        for (int n = 0; n < patch.kv().size(); n += 2) {
            SqlExpr k = patch.kv().get(n);
            SqlExpr val = patch.kv().get(n + 1);
            if (!(k instanceof SqlExpr.StringLit) || val instanceof SqlExpr.NullLit) {
                throw wall(c.fn(), "a merge patch with a computed key or a null value");
            }
            SqlWriter.Piece left = acc;
            acc = w -> w.append("OBJECT_INSERT(").piece(left).append(", ").expr(k, 0).append(", ")
                    .expr(val, 0).append(", TRUE)");
        }
        return writer.piece(acc);
    }

    /** Maps concatenated, the later map's keys winning, as DuckDB's map_concat. */
    private SqlWriter mapConcat(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlWriter.Piece acc = w -> w.append("COALESCE(").expr(a.get(0), 0).append(", OBJECT_CONSTRUCT())");
        for (int n = 1; n < a.size(); n++) {
            SqlWriter.Piece left = acc;
            SqlExpr next = a.get(n);
            String m = freshName("m");
            String k = freshName("k");
            acc = w -> w.append("REDUCE(COALESCE(OBJECT_KEYS(").expr(next, 0).append("), ARRAY_CONSTRUCT()), ")
                    .piece(left).append(", ")
                    .piece(lambdaOf(List.of(new Param(m, "OBJECT"), new Param(k, "VARCHAR")), Map.of(),
                            v -> v.append("OBJECT_INSERT(" + delimited(m) + ", " + delimited(k)
                                    + ", COALESCE(GET(").expr(next, 0).append(", " + delimited(k)
                                    + "), PARSE_JSON('null')), TRUE)")))
                    .append(")");
        }
        return writer.piece(acc);
    }

    /** DuckDB's concat: each argument as text, a NULL one skipped (an all-NULL concat is ''). */
    private SqlWriter.Piece concat(List<SqlExpr> args) {
        return w -> {
            w.append("(");
            for (int i = 0; i < args.size(); i++) {
                SqlExpr x = args.get(i);
                if (i > 0) {
                    w.append(" || ");
                }
                if (x instanceof SqlExpr.StringLit) {
                    w.expr(x, 0);
                } else if (isText(x)) {
                    w.append("COALESCE(").expr(x, 0).append(", '')");
                } else {
                    w.append("COALESCE(CAST(").expr(x, 0).append(" AS VARCHAR), '')");
                }
            }
            if (args.isEmpty()) {
                w.append("''");
            }
            w.append(")");
        };
    }

    private static DialectCapability wall(SqlFn fn, String why) {
        return new DialectCapability(fn + " reached Snowflake: " + why);
    }

    // ---- error(): no raise function

    /**
     * error(msg[, 'line:col']): the sentinel-wrapped message cast to a type it cannot parse -- the driver's error
     * carries it whole ({@code Numeric value '␟msg␟' is not recognized}, measured with 300 characters), and
     * RaisedErrors reads between the U+001F sentinels. A CASE branch that is never taken never raises (measured: a
     * constant-false and a row guard). Typed as its slot where a CASE branch IS the raise, so the branches unify.
     */
    private SqlWriter.Piece raise(List<SqlExpr> a, @com.legend.base.Nullable SqlType slot) {
        SqlWriter.Piece text = w -> {
            w.append("CHR(31) || ");
            if (a.size() > 1) {
                w.expr(a.get(1), 0).append(" || CHR(30) || ");
            }
            w.append("(").expr(a.get(0), 0).append(") || CHR(31)");
        };
        SqlWriter.Piece number = w -> w.append("CAST(").piece(text).append(" AS INTEGER)");
        if (slot == null) {
            return w -> w.append("CAST(").piece(number).append(" AS VARCHAR)");
        }
        return switch (slot) {
            case SqlType.Scalar s -> switch (s) {
                case INTEGER, BIGINT, HUGEINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP, TIMESTAMPTZ, TIME ->
                        w -> w.append("CAST(").piece(text).append(" AS " + castTypeName(s) + ")");
                case JSON -> w -> w.append("TO_VARIANT(").piece(number).append(")");
                case VARCHAR, LITERAL, TEMPORAL_TEXT, DECIMAL_TEXT -> w -> w.append("CAST(").piece(number)
                        .append(" AS VARCHAR)");
            };
            case SqlType.Decimal d -> w -> w.append("CAST(").piece(text).append(" AS " + castTypeName(d) + ")");
            case SqlType.Array arr -> w -> w.append("TO_ARRAY(").piece(number).append(")");
            case SqlType.Map m -> w -> w.append("OBJECT_CONSTRUCT('error', ").piece(number).append(")");
            case SqlType.Struct st -> w -> w.append("OBJECT_CONSTRUCT('error', ").piece(number).append(")");
        };
    }

    /** The base CASE, with a branch that IS error() raised in the CASE's own type. */
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

    private SqlWriter.Piece branch(SqlExpr value, @com.legend.base.Nullable SqlType slot) {
        return value instanceof SqlExpr.Call call && call.fn() == SqlFn.ERROR
                ? raise(call.args(), slot) : w -> w.expr(value, 0);
    }

    // ---- format

    /** A printf directive: {@code %[0][width][.precision](s|d|f)}, or {@code %%}. */
    private static final java.util.regex.Pattern DIRECTIVE =
            java.util.regex.Pattern.compile("%(%|(0?)([0-9]*)(?:\\.([0-9]+))?([sdf]))");

    /**
     * DuckDB's printf over a LITERAL format, unrolled (Snowflake has none): its text and each directive's argument
     * concatenated -- {@code %s} the argument's text, {@code %d} an integer's digits (padded to a width, {@code 0}
     * with zeros after the sign), {@code %.Nf} a number at N decimals (6 by default), rounded half-up as printf's
     * (a cast to NUMBER(38,N)). The lowering spells %t and %r as %s already. Any other directive is refused by name.
     */
    private SqlWriter.Piece format(List<SqlExpr> a) {
        if (!(a.get(0) instanceof SqlExpr.StringLit f)) {
            throw new DialectCapability("a computed format reached Snowflake, which has no printf: only a literal one"
                    + " is unrolled");
        }
        List<SqlWriter.Piece> parts = new ArrayList<>();
        java.util.regex.Matcher m = DIRECTIVE.matcher(f.value());
        int at = 0;
        int arg = 1;
        StringBuilder text = new StringBuilder();
        while (m.find()) {
            text.append(f.value(), at, m.start());
            at = m.end();
            if (m.group(1).equals("%")) {
                text.append('%');
                continue;
            }
            if (arg >= a.size()) {
                throw new DialectCapability("a format with more directives than arguments reached Snowflake");
            }
            if (text.length() > 0) {
                String t = stringLit(text.toString());
                parts.add(w -> w.append(t));
                text.setLength(0);
            }
            SqlExpr x = a.get(arg++);
            boolean zero = !m.group(2).isEmpty();
            int width = m.group(3).isEmpty() ? 0 : Integer.parseInt(m.group(3));
            char conversion = m.group(5).charAt(0);
            SqlWriter.Piece value = switch (conversion) {
                case 's' -> isText(x) ? w -> w.expr(x, 0) : w -> w.append("TO_VARCHAR(").expr(x, 0).append(")");
                case 'd' -> w -> w.append("TO_VARCHAR(").expr(x, 0).append(")");
                case 'f' -> {
                    int precision = m.group(4) == null ? 6 : Integer.parseInt(m.group(4));
                    // a negative value rounding to zero keeps its sign in Java's %f (-0.00); a NUMBER has no -0
                    String number = " AS NUMBER(38, " + precision + "))";
                    yield w -> w.append("(CASE WHEN ").expr(x, 0).append(" < 0 AND CAST(").expr(x, 0).append(number)
                            .append(" = 0 THEN '-' ELSE '' END || TO_VARCHAR(CAST(").expr(x, 0).append(number)
                            .append("))");
                }
                default -> throw new IllegalStateException("the directive pattern admits s, d, f: " + conversion);
            };
            if (width > 0 && conversion == 'd' && zero) {
                SqlWriter.Piece digits = w -> w.append("TO_VARCHAR(ABS(").expr(x, 0).append("))");
                parts.add(w -> w.append("(CASE WHEN ").expr(x, 0).append(" < 0 THEN '-' || LPAD(").piece(digits)
                        .append(", " + (width - 1) + ", '0') ELSE LPAD(").piece(digits).append(", " + width + ", '0') END)"));
            } else if (width > 0) {
                parts.add(w -> w.append("LPAD(").piece(value).append(", " + width + ", ' ')"));
            } else {
                parts.add(value);
            }
        }
        text.append(f.value().substring(at));
        if (f.value().substring(at).contains("%") || f.value().replaceAll("%%", "").chars().filter(ch -> ch == '%')
                .count() != a.size() - 1) {
            throw new DialectCapability("a format directive other than %s, %d and %f reached Snowflake: " + f.value());
        }
        if (text.length() > 0 || parts.isEmpty()) {
            String t = stringLit(text.toString());
            parts.add(w -> w.append(t));
        }
        return w -> w.append("(").join(parts, " || ", (v, part) -> v.piece(part)).append(")");
    }

    // ---- regular-expression calls (each pattern through pattern())

    private SqlWriter matches(SqlWriter writer, List<SqlExpr> a) {
        Pattern p = pattern(a.get(1), 0);
        return writer.append("(REGEXP_INSTR(").expr(a.get(0), 0).append(", ").piece(p.text())
                .append(", 1, 1, 0, ").append(stringLit(p.params())).append(") > 0)");
    }

    private SqlWriter fullMatch(SqlWriter writer, List<SqlExpr> a) {
        Pattern p = pattern(a.get(1), 0);
        return writer.append("REGEXP_LIKE(").expr(a.get(0), 0).append(", ").piece(p.text()).append(", ")
                .append(stringLit(p.params())).append(")");
    }

    /** DuckDB's regexp_extract(s, p[, g]): group g (0, the match) of the first match, '' when none. */
    private SqlWriter regexpExtract(SqlWriter writer, List<SqlExpr> a) {
        SqlWriter.Piece before = textBeforeMatch(a);
        if (before != null) {
            return writer.piece(before);
        }
        Pattern p = pattern(a.get(1), groupRead(a));
        writer.append("COALESCE(REGEXP_SUBSTR(").expr(a.get(0), 0).append(", ").piece(p.text())
                .append(", 1, 1, ").append(stringLit(p.params() + "e")).append(", ");
        return (a.size() > 2 ? writer.expr(a.get(2), 0) : writer.append("0")).append("), '')");
    }

    private SqlWriter regexpExtractAll(SqlWriter writer, List<SqlExpr> a) {
        Pattern p = pattern(a.get(1), groupRead(a));
        writer.append("REGEXP_SUBSTR_ALL(").expr(a.get(0), 0).append(", ").piece(p.text())
                .append(", 1, 1, ").append(stringLit(p.params() + "e")).append(", ");
        return (a.size() > 2 ? writer.expr(a.get(2), 0) : writer.append("0")).append(")");
    }

    /** DuckDB replaces the first match unless 'g' (occurrence 1 here; 0 is every one); \1 is a group in both. */
    private SqlWriter regexpReplace(SqlWriter writer, List<SqlExpr> a) {
        String options = a.size() > 3 && a.get(3) instanceof SqlExpr.StringLit o ? o.value() : "";
        if (a.size() > 3 && !(a.get(3) instanceof SqlExpr.StringLit)) {
            throw wall(SqlFn.REGEXP_REPLACE, "computed regexp options");
        }
        // a replacement that reads no group reads none of the pattern's numbering
        boolean readsGroups = !(a.get(2) instanceof SqlExpr.StringLit r) || r.value().matches("(?s).*\\\\[0-9].*");
        Pattern p = pattern(a.get(1), readsGroups ? Integer.MAX_VALUE : 0);
        String params = p.params() + (options.contains("i") ? "i" : "");
        return writer.append("REGEXP_REPLACE(").expr(a.get(0), 0).append(", ").piece(p.text()).append(", ")
                .expr(a.get(2), 0).append(", 1, " + (options.contains("g") ? 0 : 1) + ", ")
                .append(stringLit(params)).append(")");
    }

    // ---- regular expressions

    /** A pattern as Snowflake reads it, and its parameters ('c', plus 'i', 'm', 's' from RE2's inline flags). */
    private record Pattern(SqlWriter.Piece text, String params) {
    }

    /** RE2's inline flags, as the lowering prefixes a pattern with them. */
    private static final java.util.regex.Pattern RE2_FLAGS = java.util.regex.Pattern.compile("\\(\\?([ims]+)\\)");

    /**
     * A platform pattern (RE2's, DuckDB's) as Snowflake's POSIX ERE reads it (measured): inline flags are refused
     * ({@code (?i)}: "no argument for repetition operator"), so they become parameters; a non-capturing group is
     * refused too, and is a plain group where no read group follows it ({@code group}: the group read, 0 none); a
     * lazy quantifier is
     * greedy here ({@code a+?} matched 'aaa'), and a lookaround or a named group is no ERE: each is refused by name.
     */
    private Pattern pattern(SqlExpr p, int group) {
        String literal = p instanceof SqlExpr.StringLit lit ? lit.value() : null;
        String flags = "";
        SqlExpr body = p;
        java.util.regex.Matcher m;
        if (literal != null && (m = RE2_FLAGS.matcher(literal)).lookingAt()) {
            flags = m.group(1);
            literal = literal.substring(m.end());
        } else if (p instanceof SqlExpr.Call c && c.fn() == SqlFn.CONCAT && c.args().size() == 2
                && c.args().get(0) instanceof SqlExpr.StringLit lit && (m = RE2_FLAGS.matcher(lit.value())).matches()) {
            flags = m.group(1);
            body = c.args().get(1);
        }
        String params = "c" + (flags.contains("i") ? "i" : "") + (flags.contains("m") ? "m" : "")
                + (flags.contains("s") ? "s" : "");
        if (literal == null) {
            SqlExpr dynamic = body;
            return new Pattern(w -> w.expr(dynamic, 0), params);
        }
        if (literal.matches("(?s).*(\\*\\?|\\+\\?|\\?\\?|\\}\\?).*")) {
            throw new DialectCapability("a lazy quantifier reached Snowflake, whose POSIX regular expressions are"
                    + " greedy (measured): " + literal);
        }
        String plain = literal;
        if (plain.contains("(?:")) {
            // a non-capturing group made a plain one renumbers only the groups opened after it: refused where a
            // numbered group is read and one opens before it (group 1 of -?([0-9]+(?:\.[0-9]+)?) stays group 1)
            if (group > 0 && opensBeforeGroup(plain, group)) {
                throw new DialectCapability("a non-capturing group in a pattern whose groups are numbered reached"
                        + " Snowflake (no (?: here, measured): " + literal);
            }
            plain = plain.replace("(?:", "(");
        }
        if (plain.contains("(?")) {
            throw new DialectCapability("a lookaround, flag group or named group reached Snowflake's POSIX regular"
                    + " expressions: " + literal);
        }
        String spelled = stringLit(plain);
        return new Pattern(w -> w.append(spelled), params);
    }

    /**
     * The text before a match, or before a group of it: the lowering's regexpIndexOf measures it as group 1 of
     * {@code ^((?:.*?)P)} (or {@code ^((?:.*?)B)F} for the group opening F, P = BF), a lazy prefix POSIX reads
     * greedily. The same text is the subject cut at REGEXP_INSTR's position of P's match (of the group: B's groups
     * plus one), '' when none; null for any other extraction.
     */
    private SqlWriter.@com.legend.base.Nullable Piece textBeforeMatch(List<SqlExpr> a) {
        if (a.size() != 3 || !(a.get(2) instanceof SqlExpr.IntLit g) || g.value() != 1
                || !(a.get(1) instanceof SqlExpr.StringLit lit)) {
            return null;
        }
        String s = lit.value();
        StringBuilder flags = new StringBuilder();
        java.util.regex.Matcher m;
        while ((m = RE2_FLAGS.matcher(s)).lookingAt()) {
            flags.append(m.group(1));
            s = s.substring(m.end());
        }
        String head = "^((?:.*?)";
        if (!s.startsWith(head)) {
            return null;
        }
        String rest = s.substring(head.length());
        int close = closingParen(rest);
        if (close < 0) {
            return null;
        }
        String b = rest.substring(0, close);
        String f = rest.substring(close + 1);
        boolean whole = b.isEmpty() && f.startsWith("(?:") && f.endsWith(")");
        String original = whole ? f.substring(3, f.length() - 1) : b + f;
        int group = whole ? 0 : capturingGroups(b) + 1;
        Pattern p = pattern(new SqlExpr.StringLit((flags.isEmpty() ? "" : "(?" + flags + ")") + original), group);
        SqlExpr x = a.get(0);
        return w -> w.append("COALESCE(LEFT(").expr(x, 0).append(", NULLIF(REGEXP_INSTR(").expr(x, 0).append(", ")
                .piece(p.text()).append(", 1, 1, 0, ").append(stringLit(p.params()))
                .append(group > 0 ? ", " + group : "").append("), 0) - 1), '')");
    }

    /** The index of the first {@code )} closing no group the text opens, escapes and bracket expressions skipped;
     *  -1 when none. */
    private static int closingParen(String pattern) {
        int depth = 0;
        boolean inClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (ch == '\\') {
                i++;
            } else if (inClass) {
                inClass = ch != ']';
            } else if (ch == '[') {
                inClass = true;
            } else if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                if (depth == 0) {
                    return i;
                }
                depth--;
            }
        }
        return -1;
    }

    /** The capturing groups a pattern opens, escapes and bracket expressions skipped. */
    private static int capturingGroups(String pattern) {
        int n = 0;
        boolean inClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (ch == '\\') {
                i++;
            } else if (inClass) {
                inClass = ch != ']';
            } else if (ch == '[') {
                inClass = true;
            } else if (ch == '(' && !pattern.startsWith("(?", i)) {
                n++;
            }
        }
        return n;
    }

    /** The group an extraction reads: its literal index, 0 (the whole match) by default, or every group when
     *  computed. */
    private static int groupRead(List<SqlExpr> a) {
        if (a.size() <= 2) {
            return 0;
        }
        return a.get(2) instanceof SqlExpr.IntLit i ? (int) Math.min(i.value(), Integer.MAX_VALUE) : Integer.MAX_VALUE;
    }

    /** Whether a non-capturing group opens before the {@code group}-th capturing group (or before any, past the
     *  last), escapes and bracket expressions skipped. */
    private static boolean opensBeforeGroup(String pattern, int group) {
        int capturing = 0;
        boolean inClass = false;
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (ch == '\\') {
                i++;
            } else if (inClass) {
                inClass = ch != ']';
            } else if (ch == '[') {
                inClass = true;
            } else if (ch == '(') {
                if (pattern.startsWith("(?:", i)) {
                    return true;
                }
                if (pattern.startsWith("(?", i)) {
                    continue;
                }
                capturing++;
                if (capturing >= group) {
                    return false;
                }
            }
        }
        return false;
    }

    // ---- temporal helpers

    /** Whether a timestamp is an instant: TIMESTAMPTZ-typed, or a timezone() of a naive one (each timezone() turns
     *  one into the other: format's zone shift is timezone(zone, timezone('UTC', t))). */
    private static boolean instant(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.TIMEZONE && c.args().size() == 2) {
            return !instant(c.args().get(1));
        }
        return typeOf(e) == SqlType.Scalar.TIMESTAMPTZ;
    }

    /** A temporal operand as a TIMESTAMP_NTZ (a DATE refuses the time parts, measured). */
    private SqlWriter.Piece naive(SqlExpr e) {
        if (isTimestamp(e)) {
            return w -> w.expr(e, 0);
        }
        return w -> w.append("CAST(").expr(e, 0).append(" AS TIMESTAMP_NTZ)");
    }

    /** A date part of a date, a Pure integer (seconds truncated). Weeks are ISO's. */
    private SqlWriter.Piece extract(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        SqlExpr x = a.get(1);
        return switch (part) {
            case YEAR, QUARTER, MONTH, DAY -> w -> w.append("DATE_PART(" + unit(part) + ", ").expr(x, 0).append(")");
            case WEEK -> w -> w.append("WEEKISO(").expr(x, 0).append(")");
            case DAY_OF_YEAR -> w -> w.append("DAYOFYEAR(").expr(x, 0).append(")");
            case ISO_DAY_OF_WEEK -> w -> w.append("DAYOFWEEKISO(").expr(x, 0).append(")");
            case HOUR, MINUTE, SECOND -> w -> w.append("DATE_PART(" + unit(part) + ", ").piece(naive(x)).append(")");
            // DuckDB counts the second's milli/microseconds with the seconds
            case MILLISECOND -> w -> w.append("(DATE_PART(SECOND, ").piece(naive(x))
                    .append(") * 1000 + TRUNC(DATE_PART(NANOSECOND, ").piece(naive(x)).append(") / 1000000))");
            case MICROSECOND -> w -> w.append("(DATE_PART(SECOND, ").piece(naive(x))
                    .append(") * 1000000 + TRUNC(DATE_PART(NANOSECOND, ").piece(naive(x)).append(") / 1000))");
            case NANOSECOND -> throw new DialectCapability("date part " + part
                    + " has no measured Snowflake spelling");
        };
    }

    /** As the DuckDB dialect delivers it: a date-grained truncation a DATE, a finer one a TIMESTAMP. WEEK starts on
     *  Monday (the session's WEEK_START, measured). */
    private SqlWriter.Piece dateTrunc(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        SqlExpr x = a.get(1);
        String unit = unit(part).toLowerCase(java.util.Locale.ROOT);
        return switch (part) {
            case YEAR, QUARTER, MONTH, WEEK -> w -> w.append("CAST(DATE_TRUNC('" + unit + "', ").expr(x, 0)
                    .append(") AS DATE)");
            case DAY, HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> w -> w.append("DATE_TRUNC('"
                    + unit + "', ").piece(naive(x)).append(")");
            case NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_trunc part " + part
                    + " has no probed Snowflake spelling");
        };
    }

    /** A unit as Snowflake's DATE_PART, DATE_TRUNC, DATEADD and DATEDIFF name it. */
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
            case NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("a " + part
                    + " unit has no Snowflake spelling");
        };
    }

    /** An interval's DATEADD unit. */
    private static String intervalUnit(DatePart part) {
        if (part == DatePart.QUARTER) {
            throw new DialectCapability("interval unit " + part + " has no Snowflake spelling");
        }
        return unit(part);
    }

    /** DuckDB's date_diff counts BOUNDARIES crossed, as DATEDIFF does (measured); its week is the day count over 7,
     *  truncated (Saturday to Monday is 0), where DATEDIFF counts week boundaries. */
    private SqlWriter.Piece dateDiff(List<SqlExpr> a) {
        DatePart part = part(a.get(0));
        SqlExpr from = a.get(1);
        SqlExpr to = a.get(2);
        return switch (part) {
            case YEAR, QUARTER, MONTH, DAY -> w -> w.append("DATEDIFF(" + unit(part) + ", ").expr(from, 0).append(", ")
                    .expr(to, 0).append(")");
            case WEEK -> w -> w.append("CAST(TRUNC(DATEDIFF(DAY, ").expr(from, 0).append(", ").expr(to, 0)
                    .append(") / 7) AS BIGINT)");
            case HOUR, MINUTE, SECOND, MILLISECOND, MICROSECOND -> w -> w.append("DATEDIFF(" + unit(part) + ", ")
                    .piece(naive(from)).append(", ").piece(naive(to)).append(")");
            case NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("date_diff part " + part
                    + " has no probed Snowflake spelling");
        };
    }

    /** DuckDB's time_bucket over its origins (1970-01-01; weeks the Monday 1969-12-29) is TIME_SLICE's own (measured:
     *  weeks, months, years before 1970); a TIMESTAMP whatever the input. TIME_SLICE has no sub-second unit. */
    private SqlWriter.Piece timeBucket(List<SqlExpr> a) {
        DatePart unit = part(a.get(0));
        String slice = switch (unit) {
            case YEAR, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND -> unit(unit);
            case QUARTER, MILLISECOND, MICROSECOND, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("a "
                    + unit + " time bucket has no Snowflake TIME_SLICE unit");
        };
        return w -> w.append("TIME_SLICE(").piece(naive(a.get(2))).append(", ").expr(a.get(1), 0)
                .append(", '" + slice + "', 'START')");
    }

    /** The full English weekday name (Snowflake's DAYNAME and DY are three letters). */
    private static SqlWriter.Piece weekdayName(SqlWriter.Piece x) {
        return w -> w.append("DECODE(DAYOFWEEKISO(").piece(x).append("), 1, 'Monday', 2, 'Tuesday', 3, 'Wednesday',")
                .append(" 4, 'Thursday', 5, 'Friday', 6, 'Saturday', 7, 'Sunday')");
    }

    /** A DuckDB strftime as TO_CHAR pieces: a run of codes and text is one TO_CHAR; a weekday name or an unpadded
     *  hour (no TO_CHAR code) is its own piece. */
    private SqlWriter.Piece strftime(SqlExpr value, SqlExpr.FormatLit fl) {
        SqlWriter.Piece x = naive(value);
        List<SqlWriter.Piece> pieces = new ArrayList<>();
        StringBuilder run = new StringBuilder();
        for (com.legend.sql.DateFmt p : fl.parts()) {
            if (p == com.legend.sql.DateFmt.Part.WEEKDAY_NAME || p == com.legend.sql.DateFmt.Part.HOUR12_NOPAD) {
                flushRun(pieces, run, x);
                pieces.add(p == com.legend.sql.DateFmt.Part.WEEKDAY_NAME ? weekdayName(x)
                        : w -> w.append("LTRIM(TO_CHAR(").piece(x).append(", 'HH12'), '0')"));
            } else if (p instanceof com.legend.sql.DateFmt.Text t && t.s().contains("\"")) {
                flushRun(pieces, run, x);
                String lit = stringLit(t.s());
                pieces.add(w -> w.append(lit));
            } else {
                run.append(formatCode(p));
            }
        }
        flushRun(pieces, run, x);
        if (pieces.isEmpty()) {
            return w -> w.append("''");
        }
        return w -> w.append("(").join(pieces, " || ", SqlWriter::piece).append(")");
    }

    private void flushRun(List<SqlWriter.Piece> pieces, StringBuilder run, SqlWriter.Piece x) {
        if (run.length() > 0) {
            String fmt = stringLit(run.toString());
            pieces.add(w -> w.append("TO_CHAR(").piece(x).append(", " + fmt + ")"));
            run.setLength(0);
        }
    }

    /** One part as a TO_CHAR / TO_TIMESTAMP format element (measured: FF3/FF6/FF9, HH12, AM, MON, MMMM; text in
     *  double quotes). */
    private static String formatCode(com.legend.sql.DateFmt p) {
        return switch (p) {
            case com.legend.sql.DateFmt.Text t -> t.s().isEmpty() ? "" : "\"" + t.s() + "\"";
            case com.legend.sql.DateFmt.Part part -> switch (part) {
                case YEAR4 -> "YYYY";
                case MONTH2 -> "MM";
                case DAY2 -> "DD";
                case HOUR2 -> "HH24";
                case MIN2 -> "MI";
                case SEC2 -> "SS";
                case SUBSEC_MICRO -> "FF6";
                case SUBSEC_NANO -> "FF9";
                // DuckDB's %g: milliseconds, three digits (Pure's SSS)
                case SUBSEC_MIN -> "FF3";
                case MONTH_ABBREV -> "MON";
                case MONTH_NAME -> "MMMM";
                case HOUR12 -> "HH12";
                case AMPM -> "AM";
                case WEEKDAY_NAME -> throw new DialectCapability("a full weekday name in a Snowflake date format"
                        + " (DY is three letters)");
                case HOUR12_NOPAD -> throw new DialectCapability("an unpadded 12-hour clock in a Snowflake date"
                        + " format");
            };
        };
    }

    /** The format of a FormatLit written as a literal (TO_TIMESTAMP's parse format). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        for (com.legend.sql.DateFmt p : fl.parts()) {
            if (p instanceof com.legend.sql.DateFmt.Text t && t.s().contains("\"")) {
                throw new DialectCapability("a double quote in a Snowflake date format's text");
            }
            out.append(formatCode(p));
        }
        return out.toString();
    }

    // ==================================================================
    // Hooks of the base
    // ==================================================================

    /** Pure's hashCode is a signed 64-bit integer: HASH is one (its values are Snowflake's own). */
    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("HASH(").list(a).append(")");
    }

    /** Pure round is half-EVEN: ROUND(x, s, 'HALF_TO_EVEN') over a NUMBER (measured), refused over a FLOAT. A FLOAT's
     *  scaled value v rounds as DuckDB's round_even does -- to the even neighbour when exactly halfway, else to the
     *  nearest -- in binary: Snowflake's ROUND(FLOAT) rounds the decimal text (2.675 at scale 2 is 2.68, where the
     *  double 267.49999999999997 is nearest 267, measured), so the nearest is {@code SIGN(v) * FLOOR(ABS(v) + 0.5)}. */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        if (!isDouble(a.get(0))) {
            writer.append("ROUND(").expr(a.get(0), 0).append(", ");
            return (a.size() > 1 ? writer.expr(a.get(1), 0) : writer.append("0")).append(", 'HALF_TO_EVEN')");
        }
        SqlWriter.Piece scale = a.size() > 1 ? w -> w.append("POWER(10, ").expr(a.get(1), 0).append(")") : null;
        SqlWriter.Piece v = scale == null ? w -> w.expr(a.get(0), 6)
                : w -> w.append("(").expr(a.get(0), 6).append(" * ").piece(scale).append(")");
        writer.append("((CASE WHEN ABS(").piece(v).append(" - TRUNC(").piece(v).append(")) = 0.5 THEN 2 * ROUND(")
                .piece(v).append(" / 2) ELSE SIGN(").piece(v).append(") * FLOOR(ABS(").piece(v)
                .append(") + 0.5) END)");
        return (scale == null ? writer : writer.append(" / ").piece(scale)).append(")");
    }

    /** BITAND and friends over NUMBER(38,0): a shift is not wrapped at 64 bits (BITSHIFTLEFT(1, 63) is 2^63). */
    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        String f = switch (fnName) {
            case BIT_AND -> "BITAND";
            case BIT_OR -> "BITOR";
            case BIT_XOR -> "BITXOR";
            case BIT_SHIFT_LEFT -> "BITSHIFTLEFT";
            case BIT_SHIFT_RIGHT -> "BITSHIFTRIGHT";
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
        return writer.function(f, a);
    }

    // ==================================================================
    // Aggregates and windows
    // ==================================================================

    /** A Float aggregate over a NUMBER answers a fixed scale here (AVG and VAR_POP six digits, MEDIAN three,
     *  measured): its integer or Float argument is a DOUBLE first, as DuckDB computes; a decimal's exact result is
     *  cast (Postgres's finding: 3.14 stays 3.14). */
    private static final Set<SqlAgg.Fn> MOMENTS = Set.of(SqlAgg.Fn.STDDEV_SAMP, SqlAgg.Fn.STDDEV_POP,
            SqlAgg.Fn.STDDEV, SqlAgg.Fn.VAR_SAMP, SqlAgg.Fn.VAR_POP, SqlAgg.Fn.VARIANCE, SqlAgg.Fn.CORR,
            SqlAgg.Fn.COVAR_SAMP, SqlAgg.Fn.COVAR_POP);

    private SqlAgg.Reducer doubleArgs(SqlAgg.Reducer r) {
        List<SqlExpr> args = new ArrayList<>();
        for (SqlExpr x : r.args()) {
            args.add(isDouble(x) ? x : new SqlExpr.Cast(x, SqlType.Scalar.DOUBLE));
        }
        return new SqlAgg.Reducer(r.fn(), args, r.distinct(), r.orderBy());
    }

    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM -> aggregate(unordered(writer, r), "SUM", r);
            case COUNT -> aggregate(unordered(writer, r), "COUNT", r);
            case MIN -> aggregate(unordered(writer, r), "MIN", r);
            case MAX -> aggregate(unordered(writer, r), "MAX", r);
            case ANY_VALUE -> aggregate(unordered(writer, r), "ANY_VALUE", r);
            case AVG -> avg(unordered(writer, r), r);
            case STDDEV_SAMP -> aggregate(unordered(writer, r), "STDDEV_SAMP", doubleArgs(r));
            case STDDEV_POP -> aggregate(unordered(writer, r), "STDDEV_POP", doubleArgs(r));
            case VAR_SAMP -> aggregate(unordered(writer, r), "VAR_SAMP", doubleArgs(r));
            case VAR_POP -> aggregate(unordered(writer, r), "VAR_POP", doubleArgs(r));
            case CORR -> aggregate(unordered(writer, r), "CORR", doubleArgs(r));
            case COVAR_SAMP -> aggregate(unordered(writer, r), "COVAR_SAMP", doubleArgs(r));
            case COVAR_POP -> aggregate(unordered(writer, r), "COVAR_POP", doubleArgs(r));
            case VARIANCE -> aggregate(unordered(writer, r), "VARIANCE", doubleArgs(r));
            case STDDEV -> aggregate(unordered(writer, r), "STDDEV", doubleArgs(r));
            case BOOL_AND -> unordered(writer, r).append("BOOLAND_AGG(").list(r.args()).append(")");
            case BOOL_OR -> unordered(writer, r).append("BOOLOR_AGG(").list(r.args()).append(")");
            // LISTAGG answers '' over no row (measured); DuckDB's string_agg NULL
            case STRING_AGG -> writer.append("(CASE WHEN COUNT(").expr(r.args().get(0), 0)
                    .append(") = 0 THEN NULL ELSE ").piece(listagg(r)).append(" END)");
            // DuckDB's median interpolates numbers in DOUBLE (an integer's median is a DOUBLE)
            case MEDIAN -> unordered(writer, r).append("MEDIAN(").piece(numericArg(r.args().get(0))).append(")");
            case MODE -> unordered(writer, r).append("MODE(").expr(r.args().get(0), 0).append(")");
            case QUANTILE_CONT -> percentile(writer, "PERCENTILE_CONT", r, true);
            case QUANTILE_DISC -> percentile(writer, "PERCENTILE_DISC", r, false);
            // a NULL value kept (ARRAY_AGG drops it, measured), in the keys' order
            case LIST -> arrayAgg(writer, r);
            // the value at the key's greatest (least) row, rows of a NULL key ignored
            case ARG_MAX -> byExtreme(unordered(writer, r), "MAX_BY", r);
            case ARG_MIN -> byExtreme(unordered(writer, r), "MIN_BY", r);
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // a window function is a ranking or value function, never an aggregate (windowCall writes it)
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

    /** {@code writer}, for an aggregate Snowflake cannot order: an ordered one is refused. */
    private static SqlWriter unordered(SqlWriter writer, SqlAgg.Reducer r) {
        if (!r.orderBy().isEmpty()) {
            throw new DialectCapability("an ordered " + r.fn() + " reached Snowflake");
        }
        return writer;
    }

    /** The {@code AVG} arm of {@link #reducer}: a DOUBLE, a Decimal's average cast to one. */
    private SqlWriter avg(SqlWriter writer, SqlAgg.Reducer r) {
        return r.args().size() == 1 && typeOf(r.args().get(0)) instanceof SqlType.Decimal
                ? aggregate(writer.append("CAST("), "AVG", r).append(" AS DOUBLE)")
                : aggregate(writer, "AVG", doubleArgs(r));
    }

    /** {@code name(level) WITHIN GROUP (ORDER BY value[ DESC])}, the value a number when {@code numeric}. */
    private SqlWriter percentile(SqlWriter writer, String name, SqlAgg.Reducer r, boolean numeric) {
        if (r.args().size() != 2 || r.distinct() || r.orderBy().size() > 1) {
            throw new DialectCapability(r.fn() + " of this shape reached Snowflake");
        }
        boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
        writer.append(name).append("(").expr(r.args().get(1), 0)
                .append(") WITHIN GROUP (ORDER BY ");
        return (numeric ? writer.piece(numericArg(r.args().get(0))) : writer.expr(r.args().get(0), 0))
                .append(desc ? " DESC" : "").append(")");
    }

    /** The {@code LIST} arm of {@link #reducer}. */
    private SqlWriter arrayAgg(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1) {
            throw new DialectCapability("a LIST aggregate of " + r.args().size() + " arguments reached Snowflake");
        }
        writer.append("ARRAY_AGG(").append(r.distinct() ? "DISTINCT " : "").piece(keepNull(r.args().get(0)))
                .append(")");
        if (!r.orderBy().isEmpty()) {
            writer.append(" WITHIN GROUP (ORDER BY ").join(r.orderBy(), ", ", this::aggregateOrderKey)
                    .append(")");
        }
        return writer;
    }

    /** {@code MAX_BY(value, key)} / {@code MIN_BY}: the value at the key's greatest (least) row. */
    private static SqlWriter byExtreme(SqlWriter writer, String name, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct()) {
            throw new DialectCapability(r.fn() + " of this shape reached Snowflake");
        }
        return writer.append(name).append("(").list(r.args()).append(")");
    }

    private SqlWriter.Piece numericArg(SqlExpr x) {
        return isInteger(typeOf(x)) ? w -> w.append("CAST(").expr(x, 0).append(" AS DOUBLE)") : w -> w.expr(x, 0);
    }

    private SqlWriter.Piece listagg(SqlAgg.Reducer r) {
        return w -> {
            w.append("LISTAGG(").append(r.distinct() ? "DISTINCT " : "").list(r.args()).append(")");
            if (!r.orderBy().isEmpty()) {
                w.append(" WITHIN GROUP (ORDER BY ").join(r.orderBy(), ", ", this::aggregateOrderKey).append(")");
            }
        };
    }

    /**
     * Windowed, as DuckDB windows. Snowflake's own rules, each measured: a frame needs an ORDER BY (a whole-partition
     * frame without one is no frame); RANGE CURRENT ROW to CURRENT ROW over a date or timestamp is refused (its peers
     * are the zero-interval frame); FIRST/LAST/NTH_VALUE take no cumulative frame and default to the whole partition,
     * not ANSI's; LISTAGG, MEDIAN and the percentiles take no ordered window, so an ordered string aggregate is its
     * ordered ARRAY_AGG joined. A decimal average is cast around its OVER; LISTAGG's empty answer is guarded.
     */
    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        SqlExpr.WindowCall.Frame frame = w.frame();
        if (frame != null && w.orderBy().isEmpty()) {
            if (!(frame.from() instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding
                    && frame.to() instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing)) {
                throw new DialectCapability("a window frame without an ORDER BY reached Snowflake (a frame needs one,"
                        + " measured)");
            }
            frame = null;
        }
        Window win = new Window(w.partitionBy(), w.orderBy(), frame);
        switch (w.fn()) {
            case SqlAgg.Reducer r -> {
                return windowedReducer(writer, r, win);
            }
            // a ranking function takes no frame (ANSI ignores one; Snowflake refuses it, measured)
            case SqlAgg.RankingFn r -> {
                writer.append(aggregateName(r.fn())).append("(").list(r.args()).append(")");
                return over(writer, win.withFrame(null));
            }
            case SqlAgg.ValueFn v -> {
                return valueFn(writer, v, win);
            }
        }
    }

    /** A window's partition, order and (normalized) frame. */
    private record Window(List<SqlExpr> partitionBy, List<SqlSelect.SortKey> orderBy,
            SqlExpr.WindowCall.@com.legend.base.Nullable Frame frame) {
        Window withFrame(SqlExpr.WindowCall.@com.legend.base.Nullable Frame f) {
            return new Window(partitionBy, orderBy, f);
        }
    }

    /** Whether the frame runs from the partition's start to the current row (or its peers): cumulative. ANSI's
     *  frame for an ordered window with none written. */
    private static boolean cumulative(Window win) {
        SqlExpr.WindowCall.Frame f = win.frame();
        return !win.orderBy().isEmpty() && (f == null
                || f.from() instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding
                && f.to() instanceof SqlExpr.WindowCall.Frame.Bound.CurrentRow);
    }

    private static boolean rowsFrame(Window win) {
        return win.frame() != null && win.frame().kind() == SqlExpr.WindowCall.Frame.Kind.ROWS;
    }

    private static final SqlExpr.WindowCall.Frame WHOLE = new SqlExpr.WindowCall.Frame(
            SqlExpr.WindowCall.Frame.Kind.ROWS, new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding(),
            new SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing());

    /**
     * FIRST/LAST/NTH_VALUE under a cumulative frame (refused here: "Cumulative window frame unsupported", measured):
     * the first value is the partition's first; the last is the current row's (ROWS), or the last of its peers
     * (RANGE: the partition narrowed to the peers); the nth is the partition's nth once the frame holds n rows.
     */
    private SqlWriter valueFn(SqlWriter writer, SqlAgg.ValueFn v, Window win) {
        boolean value = v.fn() == SqlAgg.Fn.FIRST_VALUE || v.fn() == SqlAgg.Fn.LAST_VALUE
                || v.fn() == SqlAgg.Fn.NTH_VALUE;
        if (!value || !cumulative(win)) {
            writer.append(aggregateName(v.fn())).append("(").list(v.args()).append(")");
            return over(writer, win);
        }
        Window whole = win.withFrame(WHOLE);
        if (v.fn() == SqlAgg.Fn.FIRST_VALUE) {
            writer.append("FIRST_VALUE(").list(v.args()).append(")");
            return over(writer, whole);
        }
        if (v.fn() == SqlAgg.Fn.LAST_VALUE) {
            if (rowsFrame(win)) {
                return writer.append("(").expr(v.args().get(0), 0).append(")");
            }
            List<SqlExpr> peers = new ArrayList<>(win.partitionBy());
            win.orderBy().forEach(k -> peers.add(k.expr()));
            writer.append("LAST_VALUE(").list(v.args()).append(")");
            return over(writer, new Window(peers, win.orderBy(), WHOLE));
        }
        if (v.args().size() != 2) {
            throw new DialectCapability("NTH_VALUE of " + v.args().size() + " arguments reached Snowflake");
        }
        writer.append("(CASE WHEN COUNT(*)");
        over(writer, win.frame() != null ? win : win.withFrame(new SqlExpr.WindowCall.Frame(
                SqlExpr.WindowCall.Frame.Kind.RANGE, new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding(),
                new SqlExpr.WindowCall.Frame.Bound.CurrentRow())));
        writer.append(" >= ").expr(v.args().get(1), 0).append(" THEN NTH_VALUE(").list(v.args()).append(")");
        return over(writer, whole).append(" END)");
    }

    private static final Set<SqlAgg.Fn> UNORDERED_ONLY = Set.of(SqlAgg.Fn.MEDIAN, SqlAgg.Fn.MODE,
            SqlAgg.Fn.QUANTILE_CONT, SqlAgg.Fn.QUANTILE_DISC);

    private SqlWriter windowedReducer(SqlWriter writer, SqlAgg.Reducer r, Window win) {
        boolean ordered = !win.orderBy().isEmpty();
        // over a whole-partition frame, the aggregate in the window's order is the partition's ordered aggregate (an
        // ARRAY_AGG over an ordered whole frame keeps no order, measured: 'BFH' for DuckDB's 'HFB')
        if ((r.fn() == SqlAgg.Fn.STRING_AGG || r.fn() == SqlAgg.Fn.LIST) && ordered && win.frame() != null
                && win.frame().from() instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding
                && win.frame().to() instanceof SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing) {
            SqlAgg.Reducer within = r.orderBy().isEmpty()
                    ? new SqlAgg.Reducer(r.fn(), r.args(), r.distinct(), win.orderBy(), r.type()) : r;
            return windowedReducer(writer, within, new Window(win.partitionBy(), List.of(), null));
        }
        if (UNORDERED_ONLY.contains(r.fn()) && (ordered || win.frame() != null)) {
            throw new DialectCapability(r.fn() + " over an ordered or framed window reached Snowflake");
        }
        if ((r.fn() == SqlAgg.Fn.STRING_AGG || r.fn() == SqlAgg.Fn.LIST) && ordered) {
            if (!r.orderBy().isEmpty() && !r.orderBy().equals(win.orderBy())) {
                throw new DialectCapability("an ordered " + r.fn() + " over a window of another order reached"
                        + " Snowflake (WITHIN GROUP is refused beside an OVER's ORDER BY, measured)");
            }
            if (r.distinct()) {
                throw new DialectCapability("a DISTINCT " + r.fn() + " over an ordered window reached Snowflake");
            }
        }
        if (r.fn() == SqlAgg.Fn.STRING_AGG) {
            writer.append("(CASE WHEN COUNT(").expr(r.args().get(0), 0).append(")");
            over(writer, win);
            writer.append(" = 0 THEN NULL ELSE ");
            if (ordered) {
                // the frame's values in the window's order (ARRAY_AGG drops a NULL, as string_agg)
                writer.append("ARRAY_TO_STRING(ARRAY_AGG(").expr(r.args().get(0), 0).append(")");
                over(writer, win);
                writer.append(", ").expr(r.args().get(1), 0).append(")");
            } else {
                writer.piece(listagg(r));
                over(writer, win);
            }
            return writer.append(" END)");
        }
        if (r.fn() == SqlAgg.Fn.LIST && ordered) {
            if (r.args().size() != 1) {
                throw new DialectCapability("a LIST aggregate of " + r.args().size() + " arguments reached Snowflake");
            }
            writer.append("ARRAY_AGG(").piece(keepNull(r.args().get(0))).append(")");
            return over(writer, win);
        }
        if (r.fn() == SqlAgg.Fn.AVG && r.args().size() == 1 && typeOf(r.args().get(0)) instanceof SqlType.Decimal) {
            writer.append("CAST(");
            aggregate(writer, "AVG", r);
            over(writer, win);
            return writer.append(" AS DOUBLE)");
        }
        reducer(writer, r);
        return over(writer, win);
    }

    /**
     * A window's {@code OVER (...)}. RANGE CURRENT ROW to CURRENT ROW over one date or timestamp key is its peers,
     * written as the zero-interval frame (the bare form is refused there, measured).
     */
    private SqlWriter over(SqlWriter writer, Window win) {
        writer.append(" OVER (");
        if (!win.partitionBy().isEmpty()) {
            writer.append("PARTITION BY ").list(win.partitionBy());
        }
        if (!win.orderBy().isEmpty()) {
            writer.append(win.partitionBy().isEmpty() ? "" : " ").append("ORDER BY ")
                    .join(win.orderBy(), ", ", this::sortKey);
        }
        SqlExpr.WindowCall.Frame f = win.frame();
        if (f != null) {
            boolean peers = f.kind() == SqlExpr.WindowCall.Frame.Kind.RANGE
                    && f.from() instanceof SqlExpr.WindowCall.Frame.Bound.CurrentRow
                    && f.to() instanceof SqlExpr.WindowCall.Frame.Bound.CurrentRow
                    && win.orderBy().size() == 1 && temporal(win.orderBy().get(0).expr());
            writer.append(" " + f.kind() + " BETWEEN ");
            if (peers) {
                String unit = typeOf(win.orderBy().get(0).expr()) == SqlType.Scalar.DATE ? "DAYS" : "SECONDS";
                writer.append("INTERVAL '0 " + unit + "' PRECEDING AND INTERVAL '0 " + unit + "' FOLLOWING");
            } else {
                writer.append(bound(f.from()) + " AND " + bound(f.to()));
            }
        }
        return writer.append(")");
    }

    private static boolean temporal(SqlExpr e) {
        SqlType t = typeOf(e);
        return t == SqlType.Scalar.DATE || t == SqlType.Scalar.TIMESTAMP || t == SqlType.Scalar.TIMESTAMPTZ;
    }

    /** Interval frame offsets: {@code INTERVAL '3 DAYS'} (plural units measured). */
    @Override
    protected String bound(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow cr -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.Following f -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p ->
                    "INTERVAL '" + p.n() + " " + p.unit() + "' PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f ->
                    "INTERVAL '" + f.n() + " " + f.unit() + "' FOLLOWING";
        };
    }

    // ==================================================================
    // JSON constructors: OBJECTs and ARRAYs
    // ==================================================================

    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        return writer.function("OBJECT_CONSTRUCT_KEEP_NULL", j.kv());
    }

    /** A NULL element is JSON null, as DuckDB's json_array. */
    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.append("ARRAY_CONSTRUCT(").join(j.elements(), ", ", (w, e) -> w.piece(keepNull(e))).append(")");
    }

    /** ARRAY_AGG answers [] over no row; a NULL value is kept as JSON null. */
    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        writer.append("ARRAY_AGG(").piece(keepNull(j.value())).append(")");
        if (!j.orderKeys().isEmpty()) {
            writer.append(" WITHIN GROUP (ORDER BY ").join(j.orderKeys(), ", ", (w, k) -> w.expr(k.expr(), 0)
                    .append(k.desc() ? " DESC" : " ASC").append(" NULLS LAST")).append(")");
        }
        return writer;
    }

    // ==================================================================
    // Selects: LIMIT NULL OFFSET, constant-only VALUES, lateral FLATTEN
    // ==================================================================

    /** The base select, but for OFFSET without LIMIT (a syntax error here: {@code LIMIT NULL OFFSET n}, measured), a
     *  select-list explosion the passes left as a marker (written over a FLATTEN of its positions), and the lateral
     *  FLATTENs of its FROM put in scope for its column reads. */
    @Override
    protected SqlWriter select(SqlWriter writer, SqlSelect s, int depth) {
        if (isMarker(s)) {
            String alias = freshName("u");
            List<SqlSelect.Projection> ps = new ArrayList<>();
            // the root's slots are the plan's own labels, before this dialect's passes re-typed its expressions
            List<com.legend.sql.OutputCol> labels = depth == 0 && rootLabels().size() == s.projections().size()
                    ? rootLabels() : s.outputs();
            boolean aligned = labels.size() == s.projections().size();
            for (int i = 0; i < s.projections().size(); i++) {
                SqlSelect.Projection p = s.projections().get(i);
                SqlType slot = aligned ? labels.get(i).type() : p.out() != null ? p.out().type() : null;
                ps.add(new SqlSelect.Projection(explodedRead(alias, p, slot), markerColumn(p), p.out()));
            }
            return select(writer, new SqlSelect(ps, false, new SqlSource.Subselect(s, alias, null), null, List.of(),
                    null, null, List.of(), null, null, List.of()), depth);
        }
        Map<String, Map<String, SqlExpr>> scope = new LinkedHashMap<>();
        lateralsOf(s.from(), scope);
        laterals.push(scope);
        try {
            writer.append("SELECT ");
            if (s.distinct()) {
                writer.append("DISTINCT ");
            }
            if (s.projections().isEmpty() && !scope.isEmpty()) {
                List<SqlWriter.Piece> star = new ArrayList<>();
                star(s.from(), star);
                writer.join(star, ", ", SqlWriter::piece);
            } else if (s.projections().isEmpty()) {
                writer.append("*");
            } else {
                // a list's element, min or max over the NULL list is NULL: written as its slot's NULL (ARRAY_MIN(NULL)
                // and GET(NULL, 0) are VARIANT columns under the label, measured)
                List<com.legend.sql.OutputCol> labels = depth == 0 && rootLabels().size() == s.projections().size()
                        ? rootLabels() : s.outputs();
                boolean aligned = labels.size() == s.projections().size();
                for (int i = 0; i < s.projections().size(); i++) {
                    SqlSelect.Projection p = s.projections().get(i);
                    if (i > 0) {
                        writer.append(", ");
                    }
                    if (p.expr() instanceof SqlExpr.Star st && !scope.isEmpty()) {
                        // a star beside other columns, over a FROM holding a marker's FLATTEN: spelled out as well
                        List<SqlWriter.Piece> star = new ArrayList<>();
                        star(st.table() == null ? s.from() : sourceNamed(s.from(), st.table()), star);
                        writer.join(star, ", ", SqlWriter::piece);
                    } else if (aligned && nullOverNullList(p.expr())) {
                        writer.append("CAST(NULL AS " + castTypeName(labels.get(i).type()) + ")");
                        writer.append(" AS ").append(aliasIdent(markerColumn(p)));
                    } else {
                        projection(writer, p);
                    }
                }
            }
            if (!(s.from() instanceof SqlSource.Dual)) {
                nl(writer, depth).append("FROM ");
                source(writer, s.from(), depth);
            }
            if (s.where() != null) {
                nl(writer, depth).append("WHERE ").expr(s.where(), 0);
            }
            if (!s.groupBy().isEmpty()) {
                nl(writer, depth).append("GROUP BY ").list(s.groupBy());
            }
            if (s.having() != null) {
                nl(writer, depth).append("HAVING ").expr(s.having(), 0);
            }
            if (s.qualify() != null) {
                appendQualify(writer, s, depth);
            }
            if (!s.orderBy().isEmpty()) {
                nl(writer, depth).append("ORDER BY ").join(s.orderBy(), ", ", this::sortKey);
            }
            if (s.limit() != null) {
                nl(writer, depth).append("LIMIT ").append(s.limit());
            } else if (s.offset() != null) {
                nl(writer, depth).append("LIMIT NULL");
            }
            if (s.offset() != null) {
                nl(writer, depth).append("OFFSET ").append(s.offset());
            }
            return writer;
        } finally {
            laterals.pop();
        }
    }

    /** {@code *} over a FROM holding a marker's FLATTEN, spelled out: FLATTEN's own columns (SEQ, INDEX, VALUE...)
     *  are not the marker's, so each other source's columns, then each exploded column read off its position. */
    private void star(SqlSource src, List<SqlWriter.Piece> into) {
        if (src instanceof SqlSource.Join j) {
            star(j.left(), into);
            star(j.right(), into);
        } else if (src instanceof SqlSource.Subselect sub && sub.inner() instanceof SqlSelect m && isMarker(m)) {
            for (SqlSelect.Projection p : m.projections()) {
                String name = markerColumn(p);
                SqlExpr read = explodedRead(sub.alias(), p, p.out() != null ? p.out().type() : null);
                into.add(w -> w.expr(read, 0).append(" AS ").append(aliasIdent(name)));
            }
        } else if (!(src instanceof SqlSource.Dual)) {
            into.add(w -> w.append(aliasIdent(src.alias()) + ".*"));
        }
    }

    /** The source of a FROM bound to {@code alias}. */
    private static SqlSource sourceNamed(SqlSource from, String alias) {
        SqlSource s = sourceNamedOrNull(from, alias);
        if (s == null) {
            throw new DialectCapability("a star over '" + alias + "', which the FROM does not bind, reached Snowflake");
        }
        return s;
    }

    private static @com.legend.base.Nullable SqlSource sourceNamedOrNull(SqlSource from, String alias) {
        if (from instanceof SqlSource.Join j) {
            SqlSource l = sourceNamedOrNull(j.left(), alias);
            return l != null ? l : sourceNamedOrNull(j.right(), alias);
        }
        return !(from instanceof SqlSource.Dual) && alias.equals(from.alias()) ? from : null;
    }

    /** The marker's exploded columns, by FLATTEN alias, anywhere in a FROM. */
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

    /** A marker's FLATTEN: over the positions of its longest list (DuckDB's parallel UNNEST pads the shorter with
     *  NULL); a lateral one CROSS JOINs (FLATTEN takes no ON, measured), a LEFT one keeps an empty list's row
     *  ({@code OUTER => TRUE}). */
    @Override
    protected SqlWriter source(SqlWriter writer, SqlSource src, int depth) {
        if (src instanceof SqlSource.Join j && j.right() instanceof SqlSource.Subselect sub
                && isMarker(sub.inner()) && (j.kind() == SqlSource.Join.Kind.CROSS_LATERAL
                        || j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)) {
            if (j.on() != null && !(j.on() instanceof SqlExpr.BoolLit b && b.value())) {
                throw new DialectCapability("a lateral explosion with a join condition reached Snowflake");
            }
            source(writer, j.left(), depth);
            nl(writer, depth).append("CROSS JOIN LATERAL FLATTEN(INPUT => ").piece(markerPositions((SqlSelect) sub.inner()))
                    .append(j.kind() == SqlSource.Join.Kind.LEFT_LATERAL ? ", OUTER => TRUE" : "").append(") AS ")
                    .append(aliasIdent(sub.alias()));
            return writer;
        }
        // DuckDB's ASOF LEFT JOIN ... ON eq AND t >= u: the one inequality is the MATCH_CONDITION, the equalities ON
        // (an unmatched left row is kept, as DuckDB's)
        if (src instanceof SqlSource.Join j && j.kind() == SqlSource.Join.Kind.ASOF_LEFT) {
            List<SqlExpr> conjuncts = new ArrayList<>();
            conjuncts(j.on(), conjuncts);
            List<SqlExpr> match = conjuncts.stream().filter(Snowflake::inequality).toList();
            List<SqlExpr> on = conjuncts.stream().filter(e -> !inequality(e)).toList();
            if (match.size() != 1) {
                throw new DialectCapability("an as-of join of " + match.size() + " inequalities reached Snowflake (its"
                        + " MATCH_CONDITION takes one)");
            }
            source(writer, j.left(), depth);
            nl(writer, depth).append("ASOF JOIN ");
            source(writer, j.right(), depth);
            writer.append(" MATCH_CONDITION (").expr(match.get(0), 0).append(")");
            if (!on.isEmpty()) {
                writer.append(" ON ").join(on, " AND ", (w, e) -> w.append("(").expr(e, 0).append(")"));
            }
            return writer;
        }
        if (src instanceof SqlSource.Subselect sub && isMarker(sub.inner())) {
            return writer.append("TABLE(FLATTEN(INPUT => ").piece(markerPositions((SqlSelect) sub.inner()))
                    .append(")) AS ").append(aliasIdent(sub.alias()));
        }
        if (src instanceof SqlSource.Join j && (j.kind() == SqlSource.Join.Kind.LEFT_LATERAL)) {
            throw new DialectCapability("a LEFT JOIN LATERAL of a subquery reached Snowflake (only FLATTEN joins"
                    + " laterally, and without ON)");
        }
        return super.source(writer, src, depth);
    }

    private static void conjuncts(@com.legend.base.Nullable SqlExpr e, List<SqlExpr> into) {
        if (e == null || e instanceof SqlExpr.BoolLit b && b.value()) {
            return;
        }
        if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            c.args().forEach(x -> conjuncts(x, into));
        } else {
            into.add(e);
        }
    }

    private static boolean inequality(SqlExpr e) {
        return e instanceof SqlExpr.Call c && (c.fn() == SqlFn.GREATER || c.fn() == SqlFn.GREATER_EQUAL
                || c.fn() == SqlFn.LESS || c.fn() == SqlFn.LESS_EQUAL);
    }

    private static SqlWriter.Piece markerPositions(SqlSelect m) {
        List<SqlExpr> lists = m.projections().stream().map(p -> ((SqlExpr.Call) p.expr()).args().get(0)).toList();
        return w -> {
            w.append("ARRAY_GENERATE_RANGE(0, ");
            if (lists.size() == 1) {
                w.append("ARRAY_SIZE(").expr(lists.get(0), 0).append(")");
            } else {
                w.append("GREATEST_IGNORE_NULLS(").join(lists, ", ", (v, l) -> v.append("ARRAY_SIZE(").expr(l, 0)
                        .append(")")).append(")");
            }
            w.append(")");
        };
    }

    /** VALUES takes constants only (ARRAY_CONSTRUCT and PARSE_JSON are refused there, measured): rows holding any
     *  other expression are a UNION ALL of selects. */
    @Override
    protected SqlWriter valuesSource(SqlWriter writer, SqlSource.Values v) {
        boolean constants = v.rows().stream().allMatch(r -> r.stream().allMatch(Snowflake::constant));
        if (constants && !v.rows().isEmpty()) {
            return super.valuesSource(writer, v);
        }
        if (v.rows().isEmpty()) {
            throw new DialectCapability("an empty VALUES reached Snowflake");
        }
        writer.append("(");
        for (int r = 0; r < v.rows().size(); r++) {
            if (r > 0) {
                writer.append(" UNION ALL ");
            }
            writer.append("SELECT ");
            List<SqlExpr> row = v.rows().get(r);
            for (int c = 0; c < row.size(); c++) {
                if (c > 0) {
                    writer.append(", ");
                }
                writer.expr(row.get(c), 0);
                if (r == 0) {
                    writer.append(" AS ").append(aliasIdent(v.columns().get(c)));
                }
            }
        }
        return writer.append(") AS ").append(aliasIdent(v.alias()));
    }

    private static boolean constant(SqlExpr e) {
        return switch (e) {
            case SqlExpr.IntLit i -> true;
            case SqlExpr.FloatLit f -> Double.isFinite(f.value());
            case SqlExpr.DecimalLit d -> true;
            case SqlExpr.StringLit s -> s.value().indexOf('\u0000') < 0;
            case SqlExpr.BoolLit b -> true;
            case SqlExpr.NullLit n -> true;
            case SqlExpr.DateLit d -> true;
            case SqlExpr.TimestampLit t -> true;
            case SqlExpr.TimeLit t -> true;
            case SqlExpr.Cast c -> !(c.target() instanceof SqlType.Array || c.target() instanceof SqlType.Map
                    || c.target() instanceof SqlType.Struct || c.target() == SqlType.Scalar.JSON) && constant(c.value());
            default -> false;
        };
    }

    // ==================================================================
    // Values: what the driver hands back
    // ==================================================================

    /** A struct or a map arrives as JSON text (OBJECT; keys sorted, measured): decoded to the platform's map, a
     *  struct's fields in its declared order. A list cell is decoded by the executor's JSON carrier (compact and
     *  strict by the session). */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            @com.legend.base.Nullable SqlType type) {
        if (jdbcValue instanceof String s && (type instanceof SqlType.Struct || type instanceof SqlType.Map)
                && s.startsWith("{")) {
            Object parsed = com.legend.sql.Json.parse(s);
            return type instanceof SqlType.Struct st ? declaredOrder(parsed, st) : parsed;
        }
        // a decimal of scale 0 is a NUMBER(p,0), which the driver hands back as a Long (measured: CAST(8 AS
        // DECIMAL(38, 0)) reads 8L)
        if (type instanceof SqlType.Decimal && (jdbcValue instanceof Long || jdbcValue instanceof Integer)) {
            return java.math.BigDecimal.valueOf(((Number) jdbcValue).longValue());
        }
        return jdbcValue;
    }

    /** A parsed object's fields in the struct's declared order (nested structs too); a field Snowflake dropped is
     *  null, an undeclared one follows. */
    private static @com.legend.base.Nullable Object declaredOrder(@com.legend.base.Nullable Object v, SqlType t) {
        if (t instanceof SqlType.Struct st && v instanceof Map<?, ?> m) {
            LinkedHashMap<String, Object> out = new LinkedHashMap<>();
            for (SqlType.Struct.Field f : st.fields()) {
                out.put(f.name(), declaredOrder(m.get(f.name()), f.type()));
            }
            for (Map.Entry<?, ?> e : m.entrySet()) {
                out.putIfAbsent(String.valueOf(e.getKey()), e.getValue());
            }
            return out;
        }
        if (t instanceof SqlType.Array a && v instanceof List<?> l) {
            List<Object> out = new ArrayList<>(l.size());
            for (Object e : l) {
                out.add(declaredOrder(e, a.element()));
            }
            return out;
        }
        return v;
    }

    // ==================================================================
    // Passes (MOVE TO their own files)
    // ==================================================================

    /**
     * MOVE TO its own file. STATIC PIVOT as filtered aggregates (CarrierStrategies' PV1 emulation, which stands aside
     * when lists are native): GROUP BY the non-pivoted columns, one {@code AGG(CASE WHEN on = v THEN arg END) AS
     * "v__|__alias"} per value and aggregate -- the platform's pivot-column naming, which Snowflake's own PIVOT (one
     * aggregate, columns named {@code 'v'}) does not give.
     */
    static final class PivotAsGroupBy extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlSource source(SqlSource s) {
            if (!(s instanceof SqlSource.Pivot p) || p.in().isEmpty() || p.on().size() != 1) {
                return s;
            }
            List<SqlSelect.Projection> ps = new ArrayList<>();
            List<SqlExpr> group = new ArrayList<>();
            for (com.legend.sql.OutputCol oc : p.outputs()) {
                if (!oc.name().contains("__|__")) {
                    SqlExpr g = SqlExpr.Column.of(p.source().alias(), oc);
                    group.add(g);
                    ps.add(new SqlSelect.Projection(g, oc.name(), oc));
                }
            }
            for (SqlExpr v : p.in()) {
                for (SqlSource.Pivot.Using u : p.usings()) {
                    SqlAgg.Reducer agg = u.agg();
                    List<SqlExpr> args = new ArrayList<>(agg.args());
                    if (args.isEmpty()) {
                        return s;   // COUNT(*)-style using: the writer refuses the pivot by name
                    }
                    args.set(0, new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.EQUAL, p.on().get(0), v), args.get(0))), null));
                    String name = valueText(v) + "__|__" + u.alias();
                    com.legend.sql.OutputCol slot = p.outputs().stream().filter(o -> o.name().equals(name))
                            .findFirst().orElse(null);
                    ps.add(new SqlSelect.Projection(new SqlAgg.Reducer(agg.fn(), args, agg.distinct(), agg.orderBy()),
                            name, slot));
                }
            }
            SqlSelect sel = SqlSelect.starOf(p.source()).withProjections(ps).withGroupBy(group);
            return new SqlSource.Subselect(sel, p.alias(), null);
        }

        private static String valueText(SqlExpr v) {
            return switch (v) {
                case SqlExpr.StringLit s -> s.value();
                case SqlExpr.IntLit i -> String.valueOf(i.value());
                case SqlExpr.BoolLit b -> String.valueOf(b.value());
                default -> throw new DialectCapability("a pivot value " + v.getClass().getSimpleName()
                        + " reached Snowflake without a column name");
            };
        }
    }

    /** {@inheritDoc} Its JSON_TYPE answers DuckDB's names (a DECODE over TYPEOF). */
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

    /** {@inheritDoc} Refused, as TYPEOF is here (TYPEOF names Snowflake's variant types, not DuckDB's). */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        throw new DialectCapability("typeof reached Snowflake: TYPEOF names Snowflake's variant types, not DuckDB's");
    }

    /** {@inheritDoc} Its driver names every fixed-point number NUMBER, its precision and scale the column's. */
    @Override
    public String wireTypeName(@com.legend.base.Nullable String driverTypeName) {
        return isNumber(driverTypeName) ? "DECIMAL" : WireNames.fold(driverTypeName);
    }

    /** {@inheritDoc} NUMBER does not say it: by its scale it is an integer (every integer type a synonym of
     *  NUMBER(38,0)) or a decimal. Typing such a column by its scale, as the driver's own type code does, is a
     *  measured change still to make (docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md, Phase 4). */
    @Override
    public @com.legend.base.Nullable String typingWireName(@com.legend.base.Nullable String driverTypeName) {
        return isNumber(driverTypeName) ? null : wireTypeName(driverTypeName);
    }

    private static boolean isNumber(@com.legend.base.Nullable String driverTypeName) {
        return driverTypeName != null && driverTypeName.trim().equalsIgnoreCase("NUMBER");
    }

    /** {@inheritDoc} Its own types, the platform's by design (2026-10-11): its integers are NUMBER(p,0), every integer
     *  type a synonym of NUMBER(38,0) and an integer expression reported at its own precision (42 is NUMBER(2,0),
     *  measured); and its one object type, OBJECT, carries a struct and a map alike. */
    @Override
    public boolean deliversOwn(SqlType label, String wire) {
        boolean integral = label == SqlType.Scalar.BIGINT || label == SqlType.Scalar.INTEGER;
        return integral && wire.startsWith("DECIMAL(") && wire.endsWith(",0)")
                || (label instanceof SqlType.Struct || label instanceof SqlType.Map) && wire.equals("OBJECT");
    }
}
