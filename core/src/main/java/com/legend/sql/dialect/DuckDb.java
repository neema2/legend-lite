package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.List;

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

    /** A date-time finer than a microsecond is a {@code TIMESTAMP_NS}, as its literal is ({@link #timestampLit}). */
    @Override
    protected RenderedStatement.TypeSpelling holeType(com.legend.sql.ValueKind kind) {
        return kind == com.legend.sql.ValueKind.DATE_TIME_NANOS
                ? new RenderedStatement.TypeSpelling("TIMESTAMP_NS", RenderedStatement.Digits.NONE)
                : super.holeType(kind);
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
     * the DuckDB this builds with has, so a new one is a decision, not a silent refusal (and DataCube's
     * typed-values test holds every canonical type of the browser's DuckDB to a decision, asking the
     * writer).
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
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        // ENGINE DOMAIN SEMANTICS (goal #18 dialect gaps, E2E §4.1):
        // the engine's H2 returns NaN for out-of-domain acos/asin;
        // DuckDB THROWS 'Unable to compute acos of 1.1'. Same rows on
        // both backends means guarding the domain and yielding NaN —
        // the engine's answer — not propagating DuckDB's exception.
        if (c.fn() == com.legend.sql.SqlFn.ACOS
                || c.fn() == com.legend.sql.SqlFn.ASIN) {
            String fn = c.fn() == com.legend.sql.SqlFn.ACOS
                    ? "acos" : "asin";
            return writer.append("(CASE WHEN (").expr(c.args().get(0), 0).append(") BETWEEN -1 AND 1 THEN ").append(fn)
                    .append("(").expr(c.args().get(0), 0).append(") ELSE 'NaN'::DOUBLE END)");
        }
        // now(): DuckDB returns TIMESTAMPTZ; the engine's H2 returns a
        // plain (session-local naive) TIMESTAMP, and DuckDB 1.5 refuses
        // implicit TIMESTAMP_NS<->TZ comparison — cast to the engine's
        // type (session TZ is pinned UTC).
        if (c.fn() == com.legend.sql.SqlFn.NOW) {
            return writer.append("CAST(now() AS TIMESTAMP)");
        }
        // len(DOUBLE): the corpus spells length() over numeric-typed
        // expressions (engine H2 coerces); DuckDB has no len(DOUBLE) —
        // stringify the argument first, matching the engine's implicit
        // varchar coercion.
        if (c.fn() == com.legend.sql.SqlFn.LENGTH
                && !(c.args().get(0) instanceof SqlExpr.StringLit)) {
            return writer.append("length(CAST(").expr(c.args().get(0), 0).append(" AS VARCHAR))");
        }
        // date_trunc('day', ts): DuckDB returns a DATE at day grain and
        // coarser (TIMESTAMP only for hour and finer); the semantic fact is
        // pure's firstHourOfDay(Date):DateTime — the engine's H2 keeps the
        // TIMESTAMP, so this backend casts back to it. The coarser parts
        // are the Date-typed heads (firstDayOf*) and stay as they are.
        if (c.fn() == com.legend.sql.SqlFn.DATE_TRUNC
                && c.args().get(0) instanceof SqlExpr.StringLit part
                && part.value().equals("day")) {
            writer.append("CAST(");
            super.call(writer, c, 0);
            return writer.append(" AS TIMESTAMP)");
        }
        return super.call(writer, c, parentPrec);
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
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        writer.append("list_aggregate(").expr(rc.collection(), 0).append(", '")
                .append(rc.reducer().name().toLowerCase(java.util.Locale.ROOT)).append("'");
        for (SqlExpr x : rc.extras()) {
            writer.append(", ").expr(x, 0);
        }
        return writer.append(")");
    }

    /** DuckDB native membership (byte-identical to the pre-R2 call). */
    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        return writer.append("list_contains(").expr(m.collection(), 0).append(", ").expr(m.needle(), 0).append(")");
    }

    // ---- structural capabilities ----

    @Override
    protected boolean supportsQualify() {
        return true;
    }

    @Override
    protected SqlWriter appendQualify(SqlWriter writer, SqlSelect s, int depth) {
        return nl(writer, depth).append("QUALIFY ").expr(java.util.Objects.requireNonNull(s.qualify(),
                "appendQualify without a qualify clause"), 0);
    }

    @Override
    protected String asOfJoinClause() {
        return "ASOF LEFT JOIN";
    }

    /** Native PIVOT; DuckDB forbids qualified column refs inside ON/USING. */
    @Override
    protected SqlWriter pivotSource(SqlWriter writer, SqlSource.Pivot p, int depth) {
        writer.append("(PIVOT ");
        source(writer, p.source(), depth);
        // ON columns quote UNCONDITIONALLY (the corpus pins "year" — the
        // usual pivot keys are date-part words DuckDB half-reserves).
        // args arrive pre-unqualified (the UnqualifyPivotArgs pass)
        writer.append(" ON ").join(p.on(), ", ", (w, e) -> {
            if (e instanceof SqlExpr.Column c) {
                w.append(delimited(c.name()));
            } else {
                w.expr(e, 0);
            }
        });
        if (!p.in().isEmpty()) {
            writer.append(" IN (").list(p.in()).append(")");
        }
        // real pure names pivot columns value__|__agg; DuckDB joins value + '_' + alias, so the alias carries the
        // '_|__agg' tail.
        writer.append(" USING ").join(p.usings(), ", ", (w, u) -> reducer(w, u.agg()).append(" AS ")
                .append(ident("_|__" + u.alias())));
        return writer.append(") AS ").append(ident(p.alias()));
    }

    // ---- list idioms: DuckDB is the lambda backend ----

    @Override
    protected SqlWriter lambda(SqlWriter writer, SqlExpr.Lambda l) {
        return writer.append(l.params().size() == 1 ? l.params().get(0) : "(" + String.join(", ", l.params()) + ")")
                .append(" -> ").expr(l.body(), 0);
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
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "list_bool_or", false);
    }

    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "list_bool_and", true);
    }

    /** len(list_distinct(x)) = len(x) — no duplicates iff dedup is a
     * no-op; NULL (empty) coalesces to true. */
    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        return writer.append("coalesce(len(list_distinct(").expr(args.get(0), 0).append(")) = len(")
                .expr(args.get(0), 0).append("), TRUE)");
    }

    private SqlWriter listPredicate(SqlWriter writer, List<SqlExpr> args, String agg, boolean emptyDefault) {
        return writer.append("coalesce(").append(agg).append("(").function("list_transform", args).append("), ")
                .append(boolLit(emptyDefault)).append(")");
    }

    @Override
    protected SqlWriter listCall(SqlWriter writer, SqlFn fnName, List<SqlExpr> args) {
        return switch (fnName) {
            case LIST_FILTER -> writer.function("list_filter", args);
            case LIST_TRANSFORM -> writer.function("list_transform", args);
            case LIST_FLATTEN -> writer.function("flatten", args);
            case LIST_CONCAT -> writer.function("list_concat", args);
            case JSON_MERGE_PATCH -> writer.function("json_merge_patch", args);
            case LIST_GET -> writer.function("list_extract", args);
            case LIST_POSITION -> writer.function("list_position", args);
            case LIST_ZIP -> writer.function("list_zip", args);
            case LIST_DISTINCT -> writer.function("list_distinct", args);
            case LIST_APPEND -> writer.function("list_append", args);
            case LIST_SUM -> writer.function("list_sum", args);
            case LIST_MIN -> writer.function("list_min", args);
            case LIST_MAX -> writer.function("list_max", args);
            case LIST_AVG -> writer.function("list_avg", args);
            case LIST_MEDIAN -> writer.function("list_median", args);
            case LIST_MODE -> writer.append("list_aggregate(").expr(args.get(0), 0).append(", 'mode')");
            case LIST_PRODUCT -> writer.append("list_aggregate(").expr(args.get(0), 0).append(", 'product')");
            case LIST_REDUCE -> writer.function("list_reduce", args);
            case LIST_SLICE -> writer.function("array_slice", args);
            case LIST_BOOL_AND -> writer.append("list_aggregate(").expr(args.get(0), 0).append(", 'bool_and')");
            case LIST_BOOL_OR -> writer.append("list_aggregate(").expr(args.get(0), 0).append(", 'bool_or')");
            case LIST_REVERSE -> writer.function("list_reverse", args);
            case TYPEOF -> writer.function("typeof", args);
            case LIST_SORT -> writer.function("list_sort", args);
            case LIST_SORT_DESC -> writer.function("list_reverse_sort", args);
            case LIST_TAIL -> writer.expr(args.get(0), 8).append("[2:]");
            case LIST_INIT -> writer.expr(args.get(0), 8).append("[:-2]");
            case RANGE_FN -> writer.function("range", args);
            case REPEAT_VALUE -> writer.append("list_transform(range(").expr(args.get(1), 0).append("), _i -> ")
                    .expr(args.get(0), 0).append(")");
            default -> throw new IllegalStateException("not a list call: " + fnName);
        };
    }

    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        // hash() is UBIGINT and CAST is range-checked, not
        // bit-reinterpreting: flip the sign bit in unsigned space, then
        // shift down by 2^63 in HUGEINT space — exact two's-complement
        // reinterpretation, bijective, hash evaluated once
        return writer.append("CAST(CAST(xor(").function("hash", a)
                .append(", CAST(9223372036854775808 AS UBIGINT)) AS HUGEINT) - 9223372036854775808 AS BIGINT)");
    }

    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        // round_even is a 2-arg macro — bare round(x) means precision 0.
        return a.size() == 1
                ? writer.append("ROUND_EVEN(").expr(a.get(0), 0).append(", 0)")
                : writer.function("ROUND_EVEN", a);
    }

    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        return switch (fnName) {
            case BIT_AND -> writer.append("(").expr(a.get(0), 6).append(" & ").expr(a.get(1), 6).append(")");
            case BIT_OR -> writer.append("(").expr(a.get(0), 6).append(" | ").expr(a.get(1), 6).append(")");
            case BIT_XOR -> writer.function("xor", a);
            case BIT_SHIFT_LEFT -> writer.append("(").expr(a.get(0), 6).append(" << ").expr(a.get(1), 6).append(")");
            case BIT_SHIFT_RIGHT -> writer.append("(").expr(a.get(0), 6).append(" >> ").expr(a.get(1), 6).append(")");
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
    }

    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        // a stored Variant column is read as it is: to_json converts either storage once
        if (a.size() == 1 && storedVariant(a.get(0))) {
            writer.append("to_json(");
            return navigated(writer, a.get(0)).append(")");
        }
        return writer.function("to_json", a);
    }

    /** DuckDB explodes select-list unnest into rows — placement idiom. */
    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        return writer.function("UNNEST", args);
    }

    @Override
    protected SqlWriter arrayLit(SqlWriter writer, List<SqlExpr> elements) {
        return writer.append("[").list(elements).append("]");
    }

    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit s) {
        // stringLit, not raw interpolation: a Pure property name may carry
        // quotes ('quoted name' declarations) — C2.1 injection surface
        return writer.append("{").join(s.fields(), ", ", (w, f) -> structFieldValue(w.append(stringLit(f.name()))
                .append(": "), f)).append("}");
    }

    /** A NULL-valued field spells its builder-DECLARED slot type (the
     * {@link SqlExpr.StructLit.Field#declared} half the IR types by —
     * SqlTyping's declared-slot arm): DuckDB otherwise infers the
     * "NULL" type for the slot and two instances of ONE class stop
     * unifying (list_reduce accumulator vs reducer struct — the fold
     * family's {@code VARCHAR[] -> "NULL"} cast wall). */
    private SqlWriter structFieldValue(SqlWriter writer, SqlExpr.StructLit.Field f) {
        return f.declared() != null
                && f.value().type() instanceof com.legend.sql.TypeFact.Bottom
                ? writer.append("CAST(").expr(f.value(), 0).append(" AS ").append(castTypeName(f.declared()))
                        .append(")")
                : writer.expr(f.value(), 0);
    }

    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        // a POSITIONAL field (list_zip yields unnamed structs): the
        // 1-based index, never a quoted name
        return writer.append("struct_extract(").expr(g.source(), 0).append(", ")
                .append(g.field().chars().allMatch(Character::isDigit) ? g.field() : stringLit(g.field())).append(")");
    }

    // ---- variant (JSON) idioms ----

    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        // Parenthesized ALWAYS: DuckDB's lambda arrow and the JSON arrow
        // collide inside list lambdas (i -> i -> 'k' fails to parse). An
        // INTEGER key takes the arrow too, never a subscript: on JSON the two
        // agree (0-based, -1 the last, past the end NULL), but a native LIST's
        // subscript is 1-based -- (xs)[1] is the FIRST element there, the
        // second in JSON (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md, D1).
        writer.append("(");
        return navigated(writer, args.get(0)).append(" -> ").expr(args.get(1), 8).append(")");
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
    private SqlWriter navigated(SqlWriter writer, SqlExpr e) {
        return storedVariant(e) ? writer.append(super.columnRef((SqlExpr.Column) e)) : writer.expr(e, 7);
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
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        writer.append("CAST(");
        if (storedVariant(args.get(0))) {
            navigated(writer, args.get(0));
        } else {
            writer.expr(args.get(0), 0);
        }
        return writer.append(" AS JSON[])");
    }

    @Override
    protected SqlWriter structInsert(SqlWriter writer, List<SqlExpr> args) {
        String name = ((SqlExpr.StringLit) args.get(1)).value();
        return writer.append("struct_insert(").expr(args.get(0), 0).append(", \"").append(name.replace("\"", "\"\""))
                .append("\" := ").expr(args.get(2), 0).append(")");
    }

    /**
     * A scalar cast whose value is a variant ACCESS extracts TEXT first
     * ({@code ->>} strips JSON quoting) — the swap lives HERE, in rendering,
     * not in the IR.
     */
    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        if (!(c.target() instanceof com.legend.sql.SqlType.Array)
                && c.value() instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET) {
            writer.append("CAST((");
            return navigated(writer, call.args().get(0)).append(" ->> ").expr(call.args().get(1), 8).append(") AS ")
                    .append(castTypeName(c.target())).append(")");
        }
        // to/toMany of a whole stored Variant: a cast reads either storage as it is (a native
        // LIST casts to BIGINT[] directly) -- except to TEXT, where a native value would print in
        // DuckDB's own syntax ({'sku': ABC}); that one reads it as JSON first
        if (storedVariant(c.value()) && c.target() != com.legend.sql.SqlType.Scalar.VARCHAR
                && c.target() != com.legend.sql.SqlType.Scalar.TEMPORAL_TEXT
                && c.target() != com.legend.sql.SqlType.Scalar.DECIMAL_TEXT) {
            writer.append("CAST(");
            return navigated(writer, c.value()).append(" AS ").append(castTypeName(c.target())).append(")");
        }
        return super.variantAwareCast(writer, c);
    }

    /** splitPart over the list encoding: list_extract(list_filter(string_split(s, t),
     *  x -> x <> ''), p) — a list index past the end is NULL. */
    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        SqlExpr parts = SqlExpr.Call.of(SqlFn.SPLIT, a.get(0), a.get(1));
        SqlExpr nonEmpty = SqlExpr.Call.of(SqlFn.LIST_FILTER, parts,
                new SqlExpr.Lambda(List.of("x"),
                        SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                                SqlExpr.Column.param("x", parts),
                                new SqlExpr.StringLit(""))));
        return writer.expr(SqlExpr.Call.of(SqlFn.LIST_GET, nonEmpty, a.get(2)), 0);
    }
}
