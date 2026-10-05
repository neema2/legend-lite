package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The DuckDB dialect: {@link AnsiSqlRenderer} plus DuckDB's genuine
 * capabilities and idioms — native QUALIFY, native PIVOT (with its
 * unqualified-USING quirk), ASOF joins, list lambdas (folds, list
 * predicates), bracket array literals, and the {@code ->}/{@code ->>} JSON
 * operators (text extraction under scalar casts). Everything here is
 * SPELLING or SHAPE for this backend; meaning lives in the IR.
 */
public final class DuckDb extends AnsiSqlRenderer {

    /** B6 — DuckDB sessions pin UTC: the driver's Timestamps are
     * wall-preserving under it (the platform's naive-UTC temporal
     * contract, PureDateLiteral); a local-zone session shifts wall
     * times at the JDBC boundary. H2 deliberately does NOT pin (its
     * driver funnels zone-less TIMESTAMPs through the session zone —
     * a UTC session + local JVM shifted every wall time; witnessed
     * 2026-01-07T00:00 read back as 01-06T19:00). The FACT lives here;
     * execution stays in the exec funnel (F1.3). */
    @Override
    public java.util.List<String> sessionSetup() {
        return java.util.List.of("SET TimeZone='UTC'");
    }

    /** An effect segment runs as one transaction: DuckDB's DDL is transactional, so a
     *  failing statement applies nothing and the referee's ledger records nothing
     *  (homework §19). */
    @Override
    public String script(java.util.List<String> statements) {
        return "BEGIN TRANSACTION;\n" + String.join(";\n", statements) + ";\nCOMMIT;";
    }

    @Override
    public String scriptAbort() {
        return "ROLLBACK";
    }

    /** DuckDB's documented evaluate-once: {@code AS MATERIALIZED}. */
    @Override
    protected String cteAs(com.legend.sql.SqlWith.Cte c) {
        return c.materialized() ? " AS MATERIALIZED (" : " AS (";
    }

    /** DuckDB's bare {@code TIMESTAMP} is MICROSECOND precision — a
     *  literal with NONZERO sub-microsecond digits silently truncates
     *  (proven with a standalone repro: comparisons against a
     *  TIMESTAMP_NS column then invert). Only those bind as
     *  {@code TIMESTAMP_NS}: the milestoning INFINITY sentinel spells
     *  nine ZERO ns digits, where truncation is lossless and the plain
     *  spelling must stay. */
    @Override
    protected String timestampLit(String iso) {
        int dot = iso.lastIndexOf('.');
        if (dot >= 0 && iso.length() - dot - 1 > 6) {
            String subMicro = iso.substring(dot + 7)
                    .replaceAll("[^0-9].*$", "");
            if (!subMicro.isEmpty() && !subMicro.matches("0+")) {
                return "TIMESTAMP_NS '" + iso + "'";
            }
        }
        return super.timestampLit(iso);
    }

    /**
     * A DuckDB column, from its STRUCTURED catalog (T2; the user, 2026-10-01: no type strings
     * parsed): its canonical type ({@code duckdb_types().logical_type}, joined on the column's
     * {@code data_type_id}), and a DECIMAL's precision and scale as numbers. Every type the DDL can
     * say is declared as itself; the unsigned and 128-bit integers are converted at the source to
     * exact decimals; a type Pure cannot name -- a time of day, a UUID, an interval, an enum, a bit
     * string -- is declared OTHER, a String read as its text in place; a zoned timestamp is its UTC instant, read in place
     * under the UTC session and converted only in a copy ({@link CatalogType.Read#COPY_CONVERTED}); a
     * nested value (STRUCT, LIST, MAP, UNION, ARRAY) is a Variant as stored; JSON (an ALIAS of VARCHAR,
     * so known by its alias name) is a Variant; bytes are left out, by name. Every other canonical
     * type is refused, by a decision recorded in {@link #CATALOG_REFUSED} -- never guessed.
     */
    @Override
    public CatalogType catalogType(CatalogModel.Column column) {
        return CATALOG_RULES.typeOf(column);
    }

    /**
     * Every canonical DuckDB type ({@code duckdb_types().logical_type}) a Database declares, and how.
     * DECIMAL is declared from its precision and scale ({@link #catalogType}). A canonical type is
     * here, in {@link #CATALOG_REFUSED}, or DECIMAL: a test holds the three to every canonical type
     * the DuckDB this builds with has, so a new one is a decision, not a silent refusal. Public, as
     * data, so a writer outside this JVM (DataCube's, datacube/tools/catalogfacts) is generated from
     * it and tested against {@link CatalogModel}.
     */
    public static final java.util.Map<String, CatalogType> CATALOG_TYPES = catalogTypes();

    /**
     * THE question every reader asks DuckDB's catalog for a table's columns -- the tab's, the
     * warehouse's, a test's: each column's name, its own type name, its canonical type (joined on
     * its type id, inside the same DuckDB), and a DECIMAL's precision and scale. A LEFT join: a column
     * whose type names no canonical one is kept, with none, and refused by name -- never dropped.
     * {@code {schema}} and {@code {table}} are filled with SQL string literals by the reader.
     */
    public static final String CATALOG_COLUMNS_SQL = """
            SELECT c.column_name, c.data_type, t.logical_type, c.numeric_precision, c.numeric_scale, NOT c.is_nullable AS not_null
            FROM duckdb_columns() c
            LEFT JOIN (SELECT DISTINCT type_oid, logical_type FROM duckdb_types()
                       WHERE internal AND type_oid IS NOT NULL) t ON t.type_oid = c.data_type_id
            WHERE c.database_name = current_database() AND c.schema_name = {schema} AND c.table_name = {table}
            ORDER BY c.column_index""";

    /** Type ALIASES (a column's own type name, upper-cased) that are not their canonical type's: JSON. */
    public static final java.util.Map<String, CatalogType> CATALOG_ALIASES =
            java.util.Map.of("JSON", CatalogType.asStored("SEMISTRUCTURED"));

    /** Canonical types refused, each with its reason. */
    public static final java.util.Map<String, String> CATALOG_REFUSED = catalogRefused();

    /** DuckDB's catalog decisions as one {@link CatalogRules}: an unsized DECIMAL is refused. */
    public static final CatalogRules CATALOG_RULES =
            new CatalogRules("DuckDB", CATALOG_TYPES, CATALOG_ALIASES, CATALOG_REFUSED, "DECIMAL", null);

    private static java.util.Map<String, CatalogType> catalogTypes() {
        java.util.Map<String, CatalogType> m = new java.util.LinkedHashMap<>();
        // a type Pure cannot name is declared OTHER: a Pure String, read as its text wherever it is
        // referenced (StoredReads; SEMANTICS_REGISTER S26) -- in place on any source, so a read-only
        // table keeps the column
        CatalogType other = CatalogType.asStored("OTHER");
        CatalogType variant = CatalogType.asStored("SEMISTRUCTURED");
        m.put("VARCHAR", CatalogType.asStored("VARCHAR(4096)"));
        m.put("BOOLEAN", CatalogType.asStored("BIT"));
        m.put("TINYINT", CatalogType.asStored("TINYINT"));
        m.put("SMALLINT", CatalogType.asStored("SMALLINT"));
        m.put("INTEGER", CatalogType.asStored("INTEGER"));
        m.put("BIGINT", CatalogType.asStored("BIGINT"));
        // an unsigned or 128-bit integer holds values its signed width cannot: the next width, or exact decimals
        m.put("UTINYINT", CatalogType.asStored("SMALLINT"));
        m.put("USMALLINT", CatalogType.asStored("INTEGER"));
        m.put("UINTEGER", CatalogType.asStored("BIGINT"));
        m.put("UBIGINT", CatalogType.converted("DECIMAL(20,0)", "CAST(%s AS DECIMAL(20,0))"));
        m.put("HUGEINT", CatalogType.converted("DECIMAL(38,0)", "CAST(%s AS DECIMAL(38,0))"));
        m.put("FLOAT", CatalogType.asStored("REAL"));
        m.put("DOUBLE", CatalogType.asStored("DOUBLE"));
        m.put("DATE", CatalogType.asStored("DATE"));
        m.put("TIMESTAMP", CatalogType.asStored("TIMESTAMP"));
        m.put("TIMESTAMP_S", CatalogType.asStored("TIMESTAMP"));
        m.put("TIMESTAMP_MS", CatalogType.asStored("TIMESTAMP"));
        m.put("TIMESTAMP_NS", CatalogType.asStored("TIMESTAMP"));
        // a zoned timestamp is its UTC instant: read in place under the UTC session (every reading
        // session's, sessionSetup), and a copy holds its UTC wall time, whatever zone later reads it
        m.put("TIMESTAMP WITH TIME ZONE", CatalogType.copyConverted("TIMESTAMP", "CAST(timezone('UTC', %s) AS TIMESTAMP)"));
        m.put("TIME", other);
        m.put("TIME WITH TIME ZONE", other);
        m.put("UUID", other);
        m.put("INTERVAL", other);
        m.put("BIT", other);
        m.put("BIGNUM", other);
        m.put("ENUM", other);
        // nested: a Variant AS STORED -- no conversion: navigation reads it as it is, and a whole value
        // is read through to_json where it is used whole (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md)
        m.put("STRUCT", variant);
        m.put("LIST", variant);
        m.put("MAP", variant);
        m.put("UNION", variant);
        m.put("ARRAY", variant);
        // bytes: the column is left out, by name, and the table still opens (a Postgres bytea is one)
        m.put("BLOB", CatalogType.leftOut("bytes: no Pure Database type holds them"));
        return java.util.Collections.unmodifiableMap(m);
    }

    private static java.util.Map<String, String> catalogRefused() {
        java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
        m.put("GEOMETRY", "a spatial value: no Pure Database type holds it");
        m.put("UHUGEINT", "not yet decided");
        m.put("TIME_NS", "not yet decided");
        m.put("VARIANT", "not yet decided");
        m.put("NULL", "a column of no type");
        m.put("TYPE", "a type, not a value");
        return java.util.Collections.unmodifiableMap(m);
    }

    public DuckDb() {
        super(Lexicon.DUCKDB, TypeNames.DUCKDB, Spellings.DUCKDB);
    }

    /** DuckDB DDL: a store FLOAT is DOUBLE (H2's FLOAT is double
     *  precision; DuckDB's is single), a BIT column is BOOLEAN.
     *  Identifiers follow the ONE DuckDB rule ({@code ident}: the DuckDB
     *  lexicon quotes the words DuckDB reserves — default, else, do ...). */
    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        if (t instanceof com.legend.sql.SqlDdl.ColumnType.Plain p) {
            if (p.kind() == com.legend.sql.SqlDdl.ColumnType.Kind.FLOAT) {
                return "DOUBLE";
            }
            if (p.kind() == com.legend.sql.SqlDdl.ColumnType.Kind.BIT) {
                return "BOOLEAN";
            }
        }
        return super.ddlType(t);
    }

    @Override
    protected String call(SqlExpr.Call c, int parentPrec) {
        // ENGINE DOMAIN SEMANTICS (goal #18 dialect gaps, E2E §4.1):
        // the engine's H2 returns NaN for out-of-domain acos/asin;
        // DuckDB THROWS 'Unable to compute acos of 1.1'. Same rows on
        // both backends means guarding the domain and yielding NaN —
        // the engine's answer — not propagating DuckDB's exception.
        if (c.fn() == com.legend.sql.SqlFn.ACOS
                || c.fn() == com.legend.sql.SqlFn.ASIN) {
            String arg = expr(c.args().get(0), 0);
            String fn = c.fn() == com.legend.sql.SqlFn.ACOS
                    ? "acos" : "asin";
            return "(CASE WHEN (" + arg + ") BETWEEN -1 AND 1 THEN " + fn
                    + "(" + arg + ") ELSE 'NaN'::DOUBLE END)";
        }
        // now(): DuckDB returns TIMESTAMPTZ; the engine's H2 returns a
        // plain (session-local naive) TIMESTAMP, and DuckDB 1.5 refuses
        // implicit TIMESTAMP_NS<->TZ comparison — cast to the engine's
        // type (session TZ is pinned UTC).
        if (c.fn() == com.legend.sql.SqlFn.NOW) {
            return "CAST(now() AS TIMESTAMP)";
        }
        // len(DOUBLE): the corpus spells length() over numeric-typed
        // expressions (engine H2 coerces); DuckDB has no len(DOUBLE) —
        // stringify the argument first, matching the engine's implicit
        // varchar coercion.
        if (c.fn() == com.legend.sql.SqlFn.LENGTH
                && !(c.args().get(0) instanceof SqlExpr.StringLit)) {
            return "length(CAST(" + expr(c.args().get(0), 0)
                    + " AS VARCHAR))";
        }
        // date_trunc('day', ts): DuckDB returns a DATE at day grain and
        // coarser (TIMESTAMP only for hour and finer); the semantic fact is
        // pure's firstHourOfDay(Date):DateTime — the engine's H2 keeps the
        // TIMESTAMP, so this backend casts back to it. The coarser parts
        // are the Date-typed heads (firstDayOf*) and stay as they are.
        if (c.fn() == com.legend.sql.SqlFn.DATE_TRUNC
                && c.args().get(0) instanceof SqlExpr.StringLit part
                && part.value().equals("day")) {
            return "CAST(" + super.call(c, 0) + " AS TIMESTAMP)";
        }
        return super.call(c, parentPrec);
    }

    @Override
    protected java.util.List<com.legend.sql.SqlRewriter> passes() {
        // carrier strategies FIRST (base contract), then this dialect's
        // structural rewrites
        // StableScanOrder is ENGINE-CORPUS-COMPAT ONLY (user ruling,
        // 2026-08-29): the engine's own tests assert positionally while
        // relying on H2's implicit scan order — replaying them on DuckDB
        // needs the order made explicit. The PLATFORM default stays
        // order-honest (no sort demanded = no order guaranteed); the
        // corpus runner opts in, the ENGINE_CASED precedent.
        java.util.List<com.legend.sql.SqlRewriter> ps =
                new java.util.ArrayList<>(java.util.List.of(
                        new CarrierStrategies(CarrierStrategies.Caps.DUCKDB),
                        new QuantileOrder(),
                        new UnqualifyPivotArgs(), new FoldToListReduce(),
                        new CheckedDefectsToLists(),
                        new SubstringClamp(), new RawSqlAdapt()));
        if (Boolean.getBoolean("legend.exec.engineScanOrder")) {
            ps.add(new StableScanOrder());
        }
        return java.util.List.copyOf(ps);
    }

    /** DDL IDENTIFIER IDENTITY (2026-09-22). Standard SQL folds an unquoted
     * identifier to uppercase — H2 does, and the engine corpus depends on it
     * ({@code createTempTable('tt', ^Column(name='col', …))} reports {@code COL}).
     * DuckDB deviates: it preserves an identifier's spelling as written and
     * matches case-insensitively, quoted or not. So an unquoted identifier is
     * spelled FOLDED here — the same identity on every target, which is the
     * dialect's job — and a declared-quoted name keeps its case, as everywhere.
     * Matching is unaffected (DuckDB compares case-insensitively); only what the
     * database REPORTS as the name changes, to what the standard reports. */
    @Override
    protected String ddlIdentifier(String name, boolean declaredQuoted) {
        return declaredQuoted ? '"' + name + '"' : ident(name.toUpperCase(java.util.Locale.ROOT));
    }

    /** DuckDB's native list carrier: {@code list_aggregate(list,
     * 'name', extras...)} — byte-identical to the pre-R1 emission. */
    @Override
    protected String reduceCollection(SqlExpr.ReduceCollection rc) {
        return "list_aggregate(" + expr(rc.collection(), 0) + ", '"
                + rc.reducer().name().toLowerCase(java.util.Locale.ROOT) + "'"
                + rc.extras().stream().map(x -> ", " + expr(x, 0))
                        .collect(java.util.stream.Collectors.joining())
                + ")";
    }

    /** DuckDB native membership (byte-identical to the pre-R2 call). */
    @Override
    protected String membership(SqlExpr.Membership m) {
        return "list_contains(" + expr(m.collection(), 0) + ", "
                + expr(m.needle(), 0) + ")";
    }

    // ---- structural capabilities ----

    @Override
    protected boolean supportsQualify() {
        return true;
    }

    @Override
    protected void appendQualify(StringBuilder sb, SqlSelect s, int depth) {
        nl(sb, depth).append("QUALIFY ").append(expr(
                java.util.Objects.requireNonNull(s.qualify(),
                        "appendQualify without a qualify clause"), 0));
    }

    @Override
    protected String asOfJoinClause() {
        return "ASOF LEFT JOIN";
    }

    /** Native PIVOT; DuckDB forbids qualified column refs inside ON/USING. */
    @Override
    protected void pivotSource(StringBuilder sb, SqlSource.Pivot p, int depth) {
        sb.append("(PIVOT ");
        source(sb, p.source(), depth);
        // ON columns quote UNCONDITIONALLY (the corpus pins "year" — the
        // usual pivot keys are date-part words DuckDB half-reserves).
        // args arrive pre-unqualified (the UnqualifyPivotArgs pass)
        sb.append(" ON ").append(p.on().stream()
                .map(e -> e instanceof SqlExpr.Column c
                        ? delimited(c.name())
                        : expr(e, 0))
                .collect(Collectors.joining(", ")));
        if (!p.in().isEmpty()) {
            sb.append(" IN (").append(p.in().stream()
                    .map(e -> expr(e, 0))
                    .collect(Collectors.joining(", "))).append(")");
        }
        sb.append(" USING ").append(p.usings().stream()
                .map(u -> reducer(u.agg())
                        // real pure names pivot columns value__|__agg; DuckDB
                        // joins value + '_' + alias, so the alias carries the
                        // '_|__agg' tail.
                        + " AS " + ident("_|__" + u.alias()))
                .collect(Collectors.joining(", ")));
        sb.append(") AS ").append(ident(p.alias()));
    }

    // ---- list idioms: DuckDB is the lambda backend ----

    @Override
    protected String lambda(SqlExpr.Lambda l) {
        return (l.params().size() == 1
                ? l.params().get(0)
                : "(" + String.join(", ", l.params()) + ")") + " -> " + expr(l.body(), 0);
    }



    /**
     * {@code data:application/json,[...]} inlines the payload as a JSON
     * array unnested one row per element; {@code file:} reads objects.
     * One {@code data} column either way (the engine's scheme dispatch).
     */
    @Override
    protected String sourceUrl(String url) {
        if (url.startsWith("data:")) {
            int comma = url.indexOf(',');
            if (comma < 0) {
                throw new IllegalStateException("invalid data: URI (no comma): " + url);
            }
            String content = url.substring(comma + 1);
            return "SELECT unnest(CAST(" + stringLit(content) + " AS JSON[])) AS data";
        }
        if (url.startsWith("file:")) {
            // Path.of(URI), NOT URI.getPath(): a file: URI's PATH component is
            // "/D:/data/x.json" on Windows, which is not a filesystem path at
            // all — DuckDB reports "No files found that match the pattern" and
            // every file:-backed JSON source is unreadable there. Path.of
            // resolves the URI through the filesystem provider, giving
            // D:\data\x.json on Windows and /data/x.json on POSIX. Forward
            // slashes then keep the SQL literal free of backslashes; Windows
            // accepts them everywhere. (Windows CI, 2026-09-09.)
            String path = java.nio.file.Path.of(java.net.URI.create(url))
                    .toString().replace(java.io.File.separatorChar, '/');
            return "SELECT json AS data FROM read_json_objects(" + stringLit(path) + ")";
        }
        throw new IllegalStateException("unsupported sourceUrl scheme: " + url);
    }

    /** Pure semantics ride the expansion: exists([])=false, forAll([])=true. */
    @Override
    protected String listExists(List<SqlExpr> args) {
        return listPredicate(args, "list_bool_or", false);
    }

    @Override
    protected String listForAll(List<SqlExpr> args) {
        return listPredicate(args, "list_bool_and", true);
    }

    /** len(list_distinct(x)) = len(x) — no duplicates iff dedup is a
     * no-op; NULL (empty) coalesces to true. */
    @Override
    protected String allDistinct(List<SqlExpr> args) {
        String x = expr(args.get(0), 0);
        return "coalesce(len(list_distinct(" + x + ")) = len(" + x
                + "), TRUE)";
    }

    private String listPredicate(List<SqlExpr> args, String agg, boolean emptyDefault) {
        return "coalesce(" + agg + "(" + fn("list_transform", args) + "), "
                + boolLit(emptyDefault) + ")";
    }

    @Override
    protected String listCall(SqlFn fnName, List<SqlExpr> args) {
        return switch (fnName) {
            case LIST_FILTER -> fn("list_filter", args);
            case LIST_TRANSFORM -> fn("list_transform", args);
            case LIST_FLATTEN -> fn("flatten", args);
            case LIST_CONCAT -> fn("list_concat", args);
            case JSON_MERGE_PATCH -> fn("json_merge_patch", args);
            case LIST_GET -> fn("list_extract", args);
            case LIST_POSITION -> fn("list_position", args);
            case LIST_ZIP -> fn("list_zip", args);
            case LIST_DISTINCT -> fn("list_distinct", args);
            case LIST_APPEND -> fn("list_append", args);
            case LIST_SUM -> fn("list_sum", args);
            case LIST_MIN -> fn("list_min", args);
            case LIST_MAX -> fn("list_max", args);
            case LIST_AVG -> fn("list_avg", args);
            case LIST_MEDIAN -> fn("list_median", args);
            case LIST_MODE -> "list_aggregate(" + expr(args.get(0), 0) + ", 'mode')";
            case LIST_PRODUCT -> "list_aggregate(" + expr(args.get(0), 0) + ", 'product')";
            case LIST_REDUCE -> fn("list_reduce", args);
            case LIST_SLICE -> fn("array_slice", args);
            case LIST_BOOL_AND -> "list_aggregate(" + expr(args.get(0), 0) + ", 'bool_and')";
            case LIST_BOOL_OR -> "list_aggregate(" + expr(args.get(0), 0) + ", 'bool_or')";
            case LIST_REVERSE -> fn("list_reverse", args);
            case TYPEOF -> fn("typeof", args);
            case LIST_SORT -> fn("list_sort", args);
            case LIST_SORT_DESC -> fn("list_reverse_sort", args);
            case LIST_TAIL -> expr(args.get(0), 8) + "[2:]";
            case LIST_INIT -> expr(args.get(0), 8) + "[:-2]";
            case RANGE_FN -> fn("range", args);
            case REPEAT_VALUE -> "list_transform(range(" + expr(args.get(1), 0) + "), _i -> "
                    + expr(args.get(0), 0) + ")";
            default -> throw new IllegalStateException("not a list call: " + fnName);
        };
    }

    @Override
    protected String hashSigned(List<SqlExpr> a) {
        // hash() is UBIGINT and CAST is range-checked, not
        // bit-reinterpreting: flip the sign bit in unsigned space, then
        // shift down by 2^63 in HUGEINT space — exact two's-complement
        // reinterpretation, bijective, hash evaluated once
        return "CAST(CAST(xor(" + fn("hash", a)
                + ", CAST(9223372036854775808 AS UBIGINT)) AS HUGEINT)"
                + " - 9223372036854775808 AS BIGINT)";
    }

    @Override
    protected String roundHalfEven(List<SqlExpr> a) {
        // round_even is a 2-arg macro — bare round(x) means precision 0.
        return a.size() == 1
                ? "ROUND_EVEN(" + expr(a.get(0), 0) + ", 0)"
                : fn("ROUND_EVEN", a);
    }

    @Override
    protected String bitOp(SqlFn fnName, List<SqlExpr> a) {
        String x = expr(a.get(0), 6);
        String y = expr(a.get(1), 6);
        return switch (fnName) {
            case BIT_AND -> "(" + x + " & " + y + ")";
            case BIT_OR -> "(" + x + " | " + y + ")";
            case BIT_XOR -> fn("xor", a);
            case BIT_SHIFT_LEFT -> "(" + x + " << " + y + ")";
            case BIT_SHIFT_RIGHT -> "(" + x + " >> " + y + ")";
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
    }

    @Override
    protected String variantConstruct(List<SqlExpr> a) {
        // a stored Variant column is read as it is: to_json converts either storage once
        return a.size() == 1 && storedVariant(a.get(0))
                ? "to_json(" + navigated(a.get(0)) + ")" : fn("to_json", a);
    }

    /** DuckDB explodes select-list unnest into rows — placement idiom. */
    @Override
    protected String unnestProjection(List<SqlExpr> args) {
        return fn("UNNEST", args);
    }

    @Override
    protected String arrayLit(List<SqlExpr> elements) {
        return "[" + list(elements) + "]";
    }

    @Override
    protected String structLit(SqlExpr.StructLit s) {
        // stringLit, not raw interpolation: a Pure property name may carry
        // quotes ('quoted name' declarations) — C2.1 injection surface
        return "{" + s.fields().stream()
                .map(f -> stringLit(f.name()) + ": " + structFieldValue(f))
                .collect(java.util.stream.Collectors.joining(", ")) + "}";
    }

    /** A NULL-valued field spells its builder-DECLARED slot type (the
     * {@link SqlExpr.StructLit.Field#declared} half the IR types by —
     * SqlTyping's declared-slot arm): DuckDB otherwise infers the
     * "NULL" type for the slot and two instances of ONE class stop
     * unifying (list_reduce accumulator vs reducer struct — the fold
     * family's {@code VARCHAR[] -> "NULL"} cast wall). */
    private String structFieldValue(SqlExpr.StructLit.Field f) {
        return f.declared() != null
                && f.value().type() instanceof com.legend.sql.TypeFact.Bottom
                ? "CAST(" + expr(f.value(), 0) + " AS "
                        + castTypeName(f.declared()) + ")"
                : expr(f.value(), 0);
    }

    @Override
    protected String structGet(SqlExpr.StructGet g) {
        // a POSITIONAL field (list_zip yields unnamed structs): the
        // 1-based index, never a quoted name
        if (g.field().chars().allMatch(Character::isDigit)) {
            return "struct_extract(" + expr(g.source(), 0) + ", " + g.field() + ")";
        }
        return "struct_extract(" + expr(g.source(), 0) + ", "
                + stringLit(g.field()) + ")";
    }

    // ---- variant (JSON) idioms ----

    @Override
    protected String variantGet(List<SqlExpr> args) {
        // Parenthesized ALWAYS: DuckDB's lambda arrow and the JSON arrow
        // collide inside list lambdas (i -> i -> 'k' fails to parse). An
        // INTEGER key takes the arrow too, never a subscript: on JSON the two
        // agree (0-based, -1 the last, past the end NULL), but a native LIST's
        // subscript is 1-based -- (xs)[1] is the FIRST element there, the
        // second in JSON (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md, D1).
        return "(" + navigated(args.get(0)) + " -> " + expr(args.get(1), 8) + ")";
    }

    /**
     * A STORED Variant column may hold DuckDB JSON or a native STRUCT/LIST/MAP
     * (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md). Navigation reads either as it is (it casts to
     * JSON itself); a whole value used as-is -- projected, grouped, sorted, compared -- is read as
     * JSON ({@code CAST(col AS JSON)}), so every storage answers alike.
     */
    private static boolean storedVariant(SqlExpr e) {
        return e instanceof SqlExpr.Column c
                && (c.origin() == com.legend.sql.OutputCol.Origin.PHYSICAL
                        || c.origin() == com.legend.sql.OutputCol.Origin.PHYSICAL_QUOTED)
                && c.type() instanceof com.legend.sql.TypeFact.Typed t
                && t.type() == com.legend.sql.SqlType.Scalar.JSON;
    }

    /** The operand of a navigation: a stored Variant column as it is. */
    private String navigated(SqlExpr e) {
        return storedVariant(e) ? super.columnRef((SqlExpr.Column) e) : expr(e, 7);
    }

    @Override
    protected String columnRef(SqlExpr.Column c) {
        String ref = super.columnRef(c);
        // CAST AS JSON, not to_json: it passes JSON through untouched, parses JSON text held in
        // a VARCHAR (to_json would quote it as a string), and turns a STRUCT/LIST/MAP into the
        // same JSON to_json would
        return storedVariant(c) ? "CAST(" + ref + " AS JSON)" : ref;
    }

    @Override
    protected @com.legend.base.Nullable String implicitLabel(com.legend.sql.SqlSelect.Projection p) {
        return storedVariant(p.expr()) ? aliasIdent(((SqlExpr.Column) p.expr()).name()) : super.implicitLabel(p);
    }

    @Override
    protected String variantElements(List<SqlExpr> args) {
        return "CAST(" + (storedVariant(args.get(0)) ? navigated(args.get(0)) : expr(args.get(0), 0)) + " AS JSON[])";
    }

    @Override
    protected String structInsert(List<SqlExpr> args) {
        String name = ((SqlExpr.StringLit) args.get(1)).value();
        return "struct_insert(" + expr(args.get(0), 0) + ", \""
                + name.replace("\"", "\"\"") + "\" := " + expr(args.get(2), 0) + ")";
    }

    /**
     * A scalar cast whose value is a variant ACCESS extracts TEXT first
     * ({@code ->>} strips JSON quoting) — the swap lives HERE, in rendering,
     * not in the IR.
     */
    @Override
    protected String variantAwareCast(SqlExpr.Cast c) {
        if (!(c.target() instanceof com.legend.sql.SqlType.Array)
                && c.value() instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET) {
            String text = "(" + navigated(call.args().get(0)) + " ->> "
                    + expr(call.args().get(1), 8) + ")";
            return "CAST(" + text + " AS " + castTypeName(c.target()) + ")";
        }
        // to/toMany of a whole stored Variant: a cast reads either storage as it is (a native
        // LIST casts to BIGINT[] directly) -- except to TEXT, where a native value would print in
        // DuckDB's own syntax ({'sku': ABC}); that one reads it as JSON first
        if (storedVariant(c.value()) && c.target() != com.legend.sql.SqlType.Scalar.VARCHAR
                && c.target() != com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT
                && c.target() != com.legend.sql.SqlType.Scalar.DECIMAL_TEXT) {
            return "CAST(" + navigated(c.value()) + " AS " + castTypeName(c.target()) + ")";
        }
        return super.variantAwareCast(c);
    }

    /** splitPart over the list encoding: list_extract(list_filter(string_split(s, t),
     *  x -> x <> ''), p) — a list index past the end is NULL. */
    @Override
    protected String splitPartCall(List<SqlExpr> a) {
        SqlExpr parts = SqlExpr.Call.of(SqlFn.SPLIT, a.get(0), a.get(1));
        SqlExpr nonEmpty = SqlExpr.Call.of(SqlFn.LIST_FILTER, parts,
                new SqlExpr.Lambda(List.of("x"),
                        SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                                SqlExpr.Column.param("x", parts),
                                new SqlExpr.StringLit(""))));
        return expr(SqlExpr.Call.of(SqlFn.LIST_GET, nonEmpty, a.get(2)), 0);
    }
}
