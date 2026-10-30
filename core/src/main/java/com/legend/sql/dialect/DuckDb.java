package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.List;
import java.util.Set;

/**
 * The DuckDB dialect: {@link AnsiSqlRenderer} plus DuckDB's genuine
 * capabilities and idioms — native QUALIFY, native PIVOT (with its
 * unqualified-USING quirk), ASOF joins, list lambdas (folds, list
 * predicates), bracket array literals, and the {@code ->}/{@code ->>} JSON
 * operators (text extraction under scalar casts). Everything here is
 * SPELLING or SHAPE for this backend; meaning lives in the IR.
 */
public final class DuckDb extends AnsiSqlRenderer {

    /**
     * An outer join's ON without the IS NOT NULL tests a comparison beside them already makes (2026-10-17, the IQ
     * island's TPC-H corpus lane): DuckDB plans a LEFT JOIN whose ON tests a preserved column IS NOT NULL as a nested
     * loop (BLOCKWISE_NL_JOIN), where the bare equality is a hash join -- 56 s against 0.6 s at TPC-H's scale 0.01.
     * The two are one condition: an ON matches only where it is TRUE, and a comparison is never TRUE for a NULL.
     */
    @Override
    protected SqlExpr joinCondition(SqlSource.Join.Kind kind, SqlExpr on) {
        if (kind != SqlSource.Join.Kind.LEFT && kind != SqlSource.Join.Kind.RIGHT && kind != SqlSource.Join.Kind.FULL) {
            return on;
        }
        List<SqlExpr> conjuncts = new java.util.ArrayList<>();
        conjuncts(on, conjuncts);
        java.util.Set<SqlExpr> compared = new java.util.HashSet<>();
        for (SqlExpr c : conjuncts) {
            if (c instanceof SqlExpr.Call call && COMPARISONS.contains(call.fn())) {
                compared.addAll(call.args());
            }
        }
        List<SqlExpr> kept = new java.util.ArrayList<>();
        for (SqlExpr c : conjuncts) {
            if (!(c instanceof SqlExpr.Call call && call.fn() == SqlFn.IS_NOT_NULL && call.args().size() == 1
                    && compared.contains(call.args().get(0)))) {
                kept.add(c);
            }
        }
        if (kept.size() == conjuncts.size()) {
            return on;
        }
        SqlExpr out = kept.get(0);
        for (int i = 1; i < kept.size(); i++) {
            out = SqlExpr.Call.of(SqlFn.AND, out, kept.get(i));
        }
        return out;
    }

    /** The comparisons that are never TRUE for a NULL operand. */
    private static final Set<SqlFn> COMPARISONS = Set.of(SqlFn.EQUAL, SqlFn.NOT_EQUAL, SqlFn.LESS, SqlFn.LESS_EQUAL,
            SqlFn.GREATER, SqlFn.GREATER_EQUAL);

    /** Into {@code out}, {@code e}'s conjuncts under AND. */
    private static void conjuncts(SqlExpr e, List<SqlExpr> out) {
        if (e instanceof SqlExpr.Call call && call.fn() == SqlFn.AND) {
            call.args().forEach(a -> conjuncts(a, out));
        } else {
            out.add(e);
        }
    }

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "BOOLEAN";
            case INTEGER -> "INTEGER";
            case BIGINT -> "BIGINT";
            case HUGEINT -> "HUGEINT";
            case DOUBLE -> "DOUBLE";
            case VARCHAR, TEMPORAL_TEXT -> "VARCHAR";
            case DATE -> "DATE";
            case TIMESTAMP -> "TIMESTAMP";
            case TIMESTAMPTZ -> "TIMESTAMPTZ";
            case JSON -> "JSON";
            case TIME -> "TIME";
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** A struct type: {@code STRUCT(a T, ...)}, each field its name (spelled) and type. */
    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        return "STRUCT(" + structFields(st).stream().map(f -> f.getKey() + " " + f.getValue())
                .collect(java.util.stream.Collectors.joining(", ")) + ")";
    }

    /** A date format's parts: DuckDB's strftime codes, the base's {@code strftimeText} (the dialect program's
     *  Phase 1, 2026-10-13: each dialect declares its date-format vocabulary). */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        return strftimeText(fl);
    }

    /** {@code strftime}, its format split after {@code %n} when more of it follows: DuckDB 1.5.5 writes nine fraction
     *  digits ({@code %n}) with a wrong length when the format goes on -- six digits, the rest, then three bytes past
     *  the text (measured 2026-10-26 on the 1.5.5 wheel: NULs on x64, {@code e\r\0]} on aarch64, where PR #78's arm
     *  lane found the JSON wire's {@code "2024-03-10T03:00:00.000000+0000e\r\u0000"}); {@code %n} last is whole on
     *  every timestamp kind. So {@code strftime(x, '...%n') || '+0000'}: the text after it a literal when it is text
     *  alone, else its own strftime. */
    private SqlWriter strftime(SqlWriter writer, java.util.List<SqlExpr> args) {
        if (args.size() == 2 && args.get(1) instanceof SqlExpr.FormatLit fl) {
            java.util.List<com.legend.sql.DateFmt> parts = fl.parts();
            int nano = parts.indexOf(com.legend.sql.DateFmt.Part.SUBSEC_NANO);
            if (nano >= 0 && nano < parts.size() - 1) {
                java.util.List<com.legend.sql.DateFmt> tail = parts.subList(nano + 1, parts.size());
                writer.append("(");
                writer.function("strftime", java.util.List.of(args.get(0), new SqlExpr.FormatLit(parts.subList(0, nano + 1))));
                writer.append(" || ");
                if (tail.stream().allMatch(p -> p instanceof com.legend.sql.DateFmt.Text)) {
                    writer.expr(new SqlExpr.FormatLit(tail), 0);
                } else {
                    writer.function("strftime", java.util.List.of(args.get(0), new SqlExpr.FormatLit(tail)));
                }
                return writer.append(")");
            }
        }
        return writer.function("strftime", args);
    }

    /** {@inheritDoc} DuckDB's: one giant INSERT cost most of a 61 s first query (its loader, the Appender, stages the
     *  rows as text, and one INSERT ... SELECT casts them). */
    @Override
    public boolean loadsRowsInBulk() {
        return true;
    }

    /** {@inheritDoc} Yes: an embedded engine. */
    @Override
    public boolean inProcess() {
        return true;
    }

    /** {@inheritDoc} Yes: DuckDB's JSON writes a double that is not finite as {@code NaN}, {@code Infinity} or
     *  {@code -Infinity} (a float divided by a zero inside a list; the Cypher TCK's ordered values of every type). */
    @Override
    public boolean jsonSpellsNonFiniteNumbers() {
        return true;
    }

    /** {@inheritDoc} Written beside this dialect ({@code DuckDbHostOperations}). */
    @Override
    public HostOperations hostOperations() {
        return DuckDbHostOperations.INSTANCE;
    }

    /** DuckDB's own name for an in-memory database: a connection's path ({@code jdbc:duckdb::memory:}, which
     *  legend-engine's DuckDB opens) and an {@code ATTACH}'s. */
    static final String IN_MEMORY = ":memory:";

    /** {@inheritDoc} DuckDB's own name for an in-memory database: legend-engine's DuckDB opens
     *  {@code jdbc:duckdb:<path>}, and its datasource refuses a specification with no path (it puts the path into a
     *  {@code Properties}, which holds no null). */
    @Override
    public String inMemoryPath() {
        return IN_MEMORY;
    }

    /** The engine's H2 text translated for DuckDB (the raw-SQL boundary's own rewrite: keyword column names quoted,
     *  H2's type kinds mapped, {@code CURRENT_TIMESTAMP()} without parens). */
    @Override
    public String fromEngineText(String statement) {
        return RawSqlBoundary.translated(statement, types -> types, bare -> bare);
    }

    /** DuckDB's aggregates, each by its name in the shared {@code name(args)} form (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new aggregate asks this dialect too). */
    @Override
    protected SqlWriter reducer(SqlWriter writer, com.legend.sql.SqlAgg.Reducer r) {
        return switch (r.fn()) {
            case SUM -> aggregate(writer, "SUM", r);
            case COUNT -> aggregate(writer, "COUNT", r);
            case AVG -> aggregate(writer, "AVG", r);
            case MIN -> aggregate(writer, "MIN", r);
            case MAX -> aggregate(writer, "MAX", r);
            case ANY_VALUE -> aggregate(writer, "ANY_VALUE", r);
            case STDDEV_SAMP -> aggregate(writer, "STDDEV_SAMP", r);
            case STDDEV_POP -> aggregate(writer, "STDDEV_POP", r);
            case VAR_SAMP -> aggregate(writer, "VAR_SAMP", r);
            case VAR_POP -> aggregate(writer, "VAR_POP", r);
            case MEDIAN -> aggregate(writer, "MEDIAN", r);
            case MODE -> aggregate(writer, "MODE", r);
            case STRING_AGG -> aggregate(writer, "STRING_AGG", r);
            case LIST -> aggregate(writer, "LIST", r);
            case QUANTILE_CONT -> aggregate(writer, "QUANTILE_CONT", r);
            case QUANTILE_DISC -> aggregate(writer, "QUANTILE_DISC", r);
            case CORR -> aggregate(writer, "CORR", r);
            case COVAR_SAMP -> aggregate(writer, "COVAR_SAMP", r);
            case COVAR_POP -> aggregate(writer, "COVAR_POP", r);
            case ARG_MAX -> aggregate(writer, "ARG_MAX", r);
            case ARG_MIN -> aggregate(writer, "ARG_MIN", r);
            case BOOL_AND -> aggregate(writer, "BOOL_AND", r);
            case BOOL_OR -> aggregate(writer, "BOOL_OR", r);
            case VARIANCE -> aggregate(writer, "VARIANCE", r);
            case STDDEV -> aggregate(writer, "STDDEV", r);
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // a window function is a ranking or value function, never an aggregate (windowCall writes it)
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

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

    /** {@inheritDoc} DuckDB matches a name case-insensitively, so a bare declaration reaches every catalog name. */
    @Override
    public boolean bareNameReaches(String catalogName) {
        return true;
    }

    /** {@inheritDoc} DuckDB's star modifier, {@code * REPLACE (<conversion> AS "<column>", ...)}: an upload's
     *  rewrite, a Snap into the tab, a Python frame. */
    @Override
    public String copySelectList(List<CatalogModel.Conversion> conversions) {
        if (conversions.isEmpty()) {
            return "*";
        }
        List<String> items = new java.util.ArrayList<>();
        for (CatalogModel.Conversion c : conversions) {
            items.add(c.sql() + " AS " + CatalogModel.sqlIdent(c.column()));
        }
        return "* REPLACE (" + String.join(", ", items) + ")";
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
     * {@code {schema}} and {@code {table}} are filled with SQL string literals ({@link #catalogColumnsQuery}).
     */
    private static final String CATALOG_COLUMNS_SQL = """
            SELECT c.column_name, c.data_type, t.logical_type, c.numeric_precision, c.numeric_scale, NOT c.is_nullable AS not_null
            FROM duckdb_columns() c
            LEFT JOIN (SELECT DISTINCT type_oid, logical_type FROM duckdb_types()
                       WHERE internal AND type_oid IS NOT NULL) t ON t.type_oid = c.data_type_id
            WHERE c.database_name = current_database() AND c.schema_name = {schema} AND c.table_name = {table}
            ORDER BY c.column_index""";

    /** {@inheritDoc} {@link #CATALOG_COLUMNS_SQL}, each placeholder filled by its place in the template, so a name
     *  holding {@code {table}} stays a name. */
    @Override
    public String catalogColumnsQuery(String schema, String table) {
        int s = CATALOG_COLUMNS_SQL.indexOf("{schema}");
        int t = CATALOG_COLUMNS_SQL.indexOf("{table}");
        return CATALOG_COLUMNS_SQL.substring(0, s) + stringLit(schema)
                + CATALOG_COLUMNS_SQL.substring(s + "{schema}".length(), t) + stringLit(table)
                + CATALOG_COLUMNS_SQL.substring(t + "{table}".length());
    }

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

    /** DuckDB (the execution backend): every keyword DuckDB will not take as a table or column name (its
     *  {@code reserved} and {@code type_function} categories, {@code duckdb_keywords()}), held so by
     *  DuckDbKeywordsTest against the pinned DuckDB; a few more DML words besides. */
    public static final Lexicon LEXICON = new Lexicon('"', Set.of(
            "all", "analyse", "analyze", "and", "anti", "any", "array", "as", "asc", "asof", "asymmetric", "at",
            "authorization", "between", "binary", "both", "by", "case", "cast", "check", "collate", "collation",
            "column", "columns", "concurrently", "constraint", "create", "cross", "default", "deferrable", "delete",
            "desc", "describe", "distinct", "do", "drop", "else", "end", "except", "exists", "false", "fetch", "for",
            "foreign", "freeze", "from", "full", "generated", "glob", "group", "having", "ilike", "in", "initially",
            "inner", "insert", "intersect", "into", "is", "isnull", "join", "lambda", "lateral", "leading", "left",
            "like", "limit", "map", "natural", "not", "notnull", "null", "offset", "on", "only", "or", "order",
            "outer", "overlaps", "pivot", "pivot_longer", "pivot_wider", "placing", "positional", "primary",
            "qualify", "references", "returning", "right", "select", "semi", "show", "similar", "some", "struct",
            "summarize", "symmetric", "table", "tablesample", "then", "to", "trailing", "true", "try_cast", "union",
            "unique", "unpack", "unpivot", "update", "using", "values", "variadic", "verbose", "when", "where",
            "window", "with"));

    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(true, true, true, true);

    public DuckDb() {
        super(LEXICON);
    }

    /** DuckDB DDL: a store FLOAT is DOUBLE (H2's FLOAT is double
     *  precision; DuckDB's is single), a BIT column is BOOLEAN.
     *  Identifiers follow the ONE DuckDB rule ({@code ident}: the DuckDB
     *  lexicon quotes the words DuckDB reserves — default, else, do ...). */
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
                case TIMESTAMP -> "TIMESTAMP";
                case DATE -> "DATE";
                case TIME -> "TIME";
                case TIMESTAMP_NS -> "TIMESTAMP_NS";
                case JSON -> "JSON";
                case VARCHAR -> "VARCHAR";
                case OTHER, DISTINCT, ARRAY, OBJECT -> throw DdlSpelling.unspelled(p.kind());
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> DdlSpelling.sized(z);
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> DdlSpelling.scaled(s);
        };
    }

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        // every function, each by name (the dialect program's Phase 1, 2026-10-13: an exhaustive switch, so a new
        // function asks this dialect too)
        return switch (c.fn()) {
            // ---- DuckDB's own spellings (the base renderer's until the dialect program's Phase 1, D1, 2026-10-13)
            // ENGINE DOMAIN SEMANTICS (goal #18 dialect gaps, E2E §4.1):
            // the engine's H2 returns NaN for out-of-domain acos/asin;
            // DuckDB THROWS 'Unable to compute acos of 1.1'. Same rows on
            // both backends means guarding the domain and yielding NaN —
            // the engine's answer — not propagating DuckDB's exception.
            case ACOS -> domainGuarded(writer, "acos", a);
            case ASIN -> domainGuarded(writer, "asin", a);
            // now(): DuckDB returns TIMESTAMPTZ; the engine's H2 returns a
            // plain (session-local naive) TIMESTAMP, and DuckDB 1.5 refuses
            // implicit TIMESTAMP_NS<->TZ comparison — cast to the engine's
            // type (session TZ is pinned UTC).
            case NOW -> writer.append("CAST(now() AS TIMESTAMP)");
            // the functions only DuckDB runs (DuckDbSpellings: the engine-H2 text names them too)
            case BIT_NOT -> DuckDbSpellings.bitNot(writer, a);
            case MAP_EMPTY -> DuckDbSpellings.mapEmpty(writer);
            case JSON_MERGE_PATCH -> DuckDbSpellings.jsonMergePatch(writer, a);
            case ENCODE_BASE64 -> DuckDbSpellings.encodeBase64(writer, a);
            case DECODE_BASE64 -> DuckDbSpellings.decodeBase64(writer, a);
            case MAKE_TIMESTAMP -> DuckDbSpellings.makeTimestamp(writer, a);
            case TIME_BUCKET -> DuckDbSpellings.timeBucket(writer, a);
            case FROM_EPOCH_MS -> DuckDbSpellings.fromEpochMs(writer, a);
            case INT_DIVIDE -> DuckDbSpellings.intDivide(writer, a);
            // (unitFn literal, amount, date) — the unit FUNCTION NAME rides
            // as a string literal and renders bare: d + to_years(n).
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> op(writer, parentPrec, () -> writer.expr(a.get(2), 5)
                    .append(" + ").append(DuckDbSpellings.intervalFunction(part(a.get(0)))).append("(").expr(a.get(1), 0)
                    .append(")"));
            // ---- each function as this dialect spells it, by name (the dialect program's Phase 1, D1, 2026-10-13:
            // an exhaustive switch; a spelling several databases share is a named helper of the base)
            // the ANSI operators
            case AND, OR, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL, PLUS, MINUS, TIMES ->
                    infix(writer, c, parentPrec);
            // the shared spellings this dialect's lanes prove
            case NOT -> notPrefix(writer, c.args(), parentPrec);
            // Pure's divide is a Float division: both operands DOUBLE
            case DIVIDE -> divideAsDoubles(writer, c.args());
            case MOD -> modFloored(writer, c.args());
            case REM -> modTruncated(writer, c.args());
            case NEGATE -> negation(writer, c.args());
            case IS_NULL -> isNullTest(writer, c.args());
            case IS_NOT_NULL -> isNotNullTest(writer, c.args());
            case IN -> inList(writer, c.args(), parentPrec);
            // 64-bit parse (PCT Long.MIN/MAX round-trips)
            case PARSE_INT -> castAs(writer, c.args().get(0), "BIGINT");
            // NULL-skipping flat concat (the database's concat skips NULL arguments)
            case CONCAT -> concatFlat(writer, c.args());
            // joinStrings' interleaved concat, never flattened
            case CONCAT_JOIN -> writer.function("concat", c.args());
            case PI -> writer.append("pi()");
            case CEILING -> bigintOf(writer, "ceil", c.args());
            case FLOOR -> bigintOf(writer, "floor", c.args());
            case SIGN -> bigintOf(writer, "sign", c.args());
            case XOR -> xorAsAndOr(writer, c.args(), parentPrec);
            case LPAD -> padWithSpaceToInteger(writer, "lpad", c.args());
            case RPAD -> padWithSpaceToInteger(writer, "rpad", c.args());
            case UC_FIRST -> firstCharCased(writer, "upper", c.args(), parentPrec);
            case LC_FIRST -> firstCharCased(writer, "lower", c.args(), parentPrec);
            // generateGuid's contract is text: the uuid as VARCHAR
            case GUID -> writer.append("CAST(uuid() AS VARCHAR)");
            // the database's random float in [0, 1), once per row
            case RANDOM -> writer.append("random()");
            case TODAY -> writer.append("current_date");
            case DATE_TRUNC_DAY -> castAs(writer, c.args().get(0), "DATE");
            case DATE_TRUNC -> dateTruncArm(writer, c);
            case CURRENT_USER_FN -> writer.append("current_user");
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> distinctFrom(writer, c.args());
            case NULL_SAFE_EQUAL -> notDistinctFrom(writer, c.args());
            // parseDate(text): the ISO text as a timestamp
            case PARSE_DATE -> castAs(writer, c.args().get(0), "TIMESTAMP");
            // Pure's divide-with-scale is HALF_UP: plain ROUND (half away from zero)
            case ROUND_HALF_UP -> writer.function("ROUND", c.args());
            case ERROR -> raiseEnvelope(writer, c.args(), "error");
            case BOOL_TO_TEXT -> castAs(writer, c.args().get(0), "VARCHAR");
            case FLOAT_CANON -> floatCanonByText(writer, c.args(), parentPrec);
            case FLOAT_PRINT -> floatPrintByText(writer, c, parentPrec);
            // the hooks: this dialect's own (or the base's refusal)
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION, LIST_ZIP, LIST_DISTINCT,
                 LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN, LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, REPEAT_VALUE,
                 LIST_BOOL_AND, LIST_BOOL_OR, LIST_REVERSE, TYPEOF -> listCall(writer, c.fn(), c.args());
            case LIST_EXISTS -> listExists(writer, c.args());
            case LIST_FOR_ALL -> listForAll(writer, c.args());
            case STRUCT_INSERT -> structInsert(writer, c.args());
            case UNNEST -> unnestProjection(writer, c.args());
            case ROUND -> roundHalfEven(writer, c.args());
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), c.args());
            case PURE_SPLIT_PART -> splitPartCall(writer, c.args());
            case HASH -> hashSigned(writer, c.args());
            case ALL_DISTINCT -> allDistinct(writer, c.args());
            case TO_VARIANT -> variantConstruct(writer, c.args());
            case VARIANT_ELEMENTS -> variantElements(writer, c.args());
            case VARIANT_GET -> variantGet(writer, c.args());
            // this database's plain spellings: name(args)
            case ABS -> writer.function("abs", c.args());
            case COALESCE -> writer.function("coalesce", c.args());
            case LENGTH -> lengthArm(writer, c);
            case UPPER -> writer.function("upper", c.args());
            case LOWER -> writer.function("lower", c.args());
            case GREATEST -> writer.function("greatest", c.args());
            case LEAST -> writer.function("least", c.args());
            case LIST_FLATTEN -> writer.function("flatten", c.args());
            case SQRT -> writer.function("sqrt", c.args());
            case CBRT -> writer.function("cbrt", c.args());
            case EXP -> writer.function("exp", c.args());
            case LN -> writer.function("ln", c.args());
            case LOG10 -> writer.function("log10", c.args());
            case POW -> writer.function("power", c.args());
            case SIN -> writer.function("sin", c.args());
            case COS -> writer.function("cos", c.args());
            case TAN -> writer.function("tan", c.args());
            case ATAN -> writer.function("atan", c.args());
            case ATAN2 -> writer.function("atan2", c.args());
            case SINH -> writer.function("sinh", c.args());
            case COSH -> writer.function("cosh", c.args());
            case TANH -> writer.function("tanh", c.args());
            case REGEXP_EXTRACT -> writer.function("regexp_extract", c.args());
            case REGEXP_EXTRACT_ALL -> writer.function("regexp_extract_all", c.args());
            case REGEXP_REPLACE -> writer.function("regexp_replace", c.args());
            case REGEXP_FULL_MATCH -> writer.function("regexp_full_match", c.args());
            case MAP_FROM_LISTS -> writer.function("map", c.args());
            case MAP_FROM_ENTRIES -> writer.function("map_from_entries", c.args());
            case MAP_EXTRACT -> writer.function("map_extract", c.args());
            case MAP_KEYS -> writer.function("map_keys", c.args());
            case MAP_VALUES -> writer.function("map_values", c.args());
            case MAP_CONCAT -> writer.function("map_concat", c.args());
            case SUBSTRING -> writer.function("substr", c.args());
            case STRPOS -> writer.function("strpos", c.args());
            case STARTS_WITH -> writer.function("starts_with", c.args());
            case ENDS_WITH -> writer.function("ends_with", c.args());
            case MATCHES -> writer.function("regexp_matches", c.args());
            case LEFT -> writer.function("left", c.args());
            case RIGHT -> writer.function("right", c.args());
            case TRIM -> writer.function("trim", c.args());
            case LTRIM -> writer.function("ltrim", c.args());
            case RTRIM -> writer.function("rtrim", c.args());
            case REPLACE -> writer.function("replace", c.args());
            case TRANSLATE -> writer.function("translate", c.args());
            case SPLIT -> writer.function("string_split", c.args());
            case SPLIT_PART -> writer.function("split_part", c.args());
            case REVERSE_STRING -> writer.function("reverse", c.args());
            case ASCII_CODE -> writer.function("ascii", c.args());
            // chr takes a 32-bit code; a computed Integer is a BIGINT here (IQ island, 2026-10-17: CHAR(65 + MOD(id, 26)))
            case CHR -> writer.append("chr(CAST(").expr(c.args().get(0), 0).append(" AS INTEGER))");
            case LEVENSHTEIN -> writer.function("levenshtein", c.args());
            case FORMAT -> writer.function("printf", c.args());
            case MD5 -> writer.function("md5", c.args());
            case SHA1 -> writer.function("sha1", c.args());
            case SHA256 -> writer.function("sha256", c.args());
            case EXTRACT -> writer.append("date_part(").append(stringLit(DuckDbSpellings.datePart(part(a.get(0)))))
                    .append(", ").list(a.subList(1, a.size())).append(")");
            case MAKE_DATE -> writer.function("make_date", c.args());
            case DATE_DIFF -> writer.append("date_diff(").append(stringLit(DuckDbSpellings.datePart(part(a.get(0)))))
                    .append(", ").list(a.subList(1, a.size())).append(")");
            case TIMEZONE -> writer.function("timezone", c.args());
            // a date plus a time of day is the timestamp at it (measured, 1.4.4)
            case DATE_AT_TIME -> op(writer, parentPrec, () -> writer.expr(a.get(0), 5).append(" + ")
                    .expr(a.get(1), 6));
            case EPOCH_SECONDS -> writer.function("epoch", c.args());
            case EPOCH_MS -> writer.function("epoch_ms", c.args());
            // the nanosecond epoch (exact over a TIMESTAMP_NS, measured 1.4.4) and the TIMESTAMP_NS it makes
            case EPOCH_NS -> writer.function("epoch_ns", c.args());
            case FROM_EPOCH_NS -> writer.function("make_timestamp_ns", c.args());
            case FROM_EPOCH_SECONDS -> writer.function("to_timestamp", c.args());
            case DAYNAME -> writer.function("dayname", c.args());
            case MONTHNAME -> writer.function("monthname", c.args());
            case COT -> writer.function("cot", c.args());
            case RADIANS -> writer.function("radians", c.args());
            case DEGREES -> writer.function("degrees", c.args());
            case REPEAT_STR -> writer.function("repeat", c.args());
            case JARO_WINKLER -> writer.function("jaro_winkler_similarity", c.args());
            case LIST_LENGTH -> writer.function("len", c.args());
            case STRFTIME -> strftime(writer, c.args());
            case STRPTIME -> writer.function("strptime", c.args());
            case FLOOR_RAW -> writer.function("floor", c.args());
            case JSON_TYPE -> writer.function("json_type", c.args());
            case JSON_ARRAY_LENGTH -> writer.function("json_array_length", c.args());
            case JSON_PRETTY -> writer.function("json_pretty", c.args());
        };
    }

    /** The {@code ACOS} and {@code ASIN} arms of {@link #call}: NaN outside the domain, as the engine's H2. */
    private SqlWriter domainGuarded(SqlWriter writer, String fn, List<SqlExpr> a) {
        return writer.append("(CASE WHEN (").expr(a.get(0), 0).append(") BETWEEN -1 AND 1 THEN ").append(fn).append("(")
                .expr(a.get(0), 0).append(") ELSE 'NaN'::DOUBLE END)");
    }

    /** len(DOUBLE): the corpus spells length() over numeric-typed expressions (engine H2 coerces); DuckDB has no
     *  len(DOUBLE) — stringify the argument first, matching the engine's implicit varchar coercion. */
    private SqlWriter lengthArm(SqlWriter writer, SqlExpr.Call c) {
        return c.args().get(0) instanceof SqlExpr.StringLit
                ? writer.function("length", c.args())
                : writer.append("length(CAST(").expr(c.args().get(0), 0).append(" AS VARCHAR))");
    }

    /** date_trunc('day', ts): DuckDB returns a DATE at day grain and coarser (TIMESTAMP only for hour and finer); the
     *  semantic fact is pure's firstHourOfDay(Date):DateTime — the engine's H2 keeps the TIMESTAMP, so this backend
     *  casts back to it. The coarser parts are the Date-typed heads (firstDayOf*) and stay as they are. */
    private SqlWriter dateTruncArm(SqlWriter writer, SqlExpr.Call c) {
        String spelled = stringLit(DuckDbSpellings.datePart(part(c.args().get(0))));
        if (part(c.args().get(0)) == com.legend.sql.DatePart.DAY) {
            writer.append("CAST(");
            dateTruncDateGrained(writer, c.args(), spelled);
            return writer.append(" AS TIMESTAMP)");
        }
        return dateTruncDateGrained(writer, c.args(), spelled);
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
                        new CarrierStrategies(CAPS),
                        new QuantileOrder(),
                        new UnqualifyPivotArgs(), new FoldToListReduce(),
                        new CheckedDefectsToLists(),
                        new SubstringClamp(), ConstantKeysAsExpressions.groupByOnly(),
                        // IQ island, 2026-10-17: a LEFT JOIN condition's scalar subquery of the null-supplying side,
                        // which DuckDB cannot plan, filters that side before the join
                        new OuterJoinSubqueryFilters()));
        if (Boolean.getBoolean("legend.exec.engineScanOrder")) {
            ps.add(new StableScanOrder());
        }
        // last of this dialect's passes (StoredReads follows), on the statement root only: a stored Variant
        // column leaving through a star is read as JSON (docs/VARIANT_STORAGE_CENSUS_2026_09_27.md, the 2026-10-12
        // addendum)
        ps.add(new VariantEgress());
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



    /** {@inheritDoc} DuckDB keeps the SQL standard's {@code information_schema} (and a {@code pg_catalog}, which no
     *  grid lists): the corpus's catalog-grid tests prove it, the primary keys' {@code key_column_usage} and
     *  {@code table_constraints} pair included (DuckDB 1.4.4, probed 2026-09-05: ordinal positions, quoted names). */
    @Override
    protected SqlWriter catalog(SqlWriter writer, SqlSource.Catalog c) {
        return InformationSchemaCatalog.write(writer, c, this, CATALOG_GRID_TYPES);
    }

    /** {@inheritDoc} DuckDB runs the standard spelling: the test-data generator's sessions prove it (the corpus's
     *  generator tests, ReservedNamesGenerationTest). */
    @Override
    protected SqlWriter tempTableAs(com.legend.sql.SqlDml.CreateTempTableAs ct) {
        return createTemporaryTableAs(ct);
    }

    /** DuckDB's catalog names a column's type as its JDBC driver does, a DECIMAL with its precision and scale
     *  ({@code DECIMAL(10,2)}), whose code is DECIMAL's; each name's code is the driver's (measured 2026-10-16 against
     *  {@code DatabaseMetaData.getColumns}, CatalogGridTest). REAL is FLOAT there. */
    private static final InformationSchemaCatalog.Types CATALOG_GRID_TYPES = new InformationSchemaCatalog.Types(
            "data_type", "CASE WHEN data_type LIKE 'DECIMAL%' THEN 'DECIMAL' ELSE data_type END",
            gridTypeCodes());

    private static java.util.Map<String, InformationSchemaCatalog.TypeCode> gridTypeCodes() {
        java.util.Map<String, InformationSchemaCatalog.TypeCode> m = new java.util.LinkedHashMap<>();
        m.put("INTEGER", InformationSchemaCatalog.TypeCode.INTEGER);
        m.put("VARCHAR", InformationSchemaCatalog.TypeCode.VARCHAR);
        m.put("BIGINT", InformationSchemaCatalog.TypeCode.BIGINT);
        m.put("SMALLINT", InformationSchemaCatalog.TypeCode.SMALLINT);
        m.put("TINYINT", InformationSchemaCatalog.TypeCode.TINYINT);
        m.put("DOUBLE", InformationSchemaCatalog.TypeCode.DOUBLE);
        m.put("FLOAT", InformationSchemaCatalog.TypeCode.FLOAT);
        m.put("DECIMAL", InformationSchemaCatalog.TypeCode.DECIMAL);
        m.put("DATE", InformationSchemaCatalog.TypeCode.DATE);
        m.put("TIMESTAMP", InformationSchemaCatalog.TypeCode.TIMESTAMP);
        m.put("BOOLEAN", InformationSchemaCatalog.TypeCode.BOOLEAN);
        m.put("TIME", InformationSchemaCatalog.TypeCode.TIME);
        m.put("TIMESTAMP WITH TIME ZONE", InformationSchemaCatalog.TypeCode.TIMESTAMP_WITH_TIMEZONE);
        m.put("BLOB", InformationSchemaCatalog.TypeCode.BLOB);
        return m;
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
        if (a.size() == 2 && a.get(0).type() instanceof com.legend.sql.TypeFact.Typed t
                && t.type() instanceof com.legend.sql.SqlType.Decimal && a.get(1) instanceof SqlExpr.IntLit n
                && n.value() >= 0 && n.value() <= 37) {
            // an exact decimal to a constant scale (IQ island, 2026-10-17, the corpus lane's wide CASTs: ROUND_EVEN of a
            // DECIMAL is a DOUBLE, 1524157887653619.89 came back .84): DuckDB's ROUND of a DECIMAL is exact, half away
            // from zero, so an exact half -- the remainder by the unit half of it -- whose rounding is odd steps back
            // one unit toward zero, to its even neighbour
            java.math.BigDecimal unit = java.math.BigDecimal.ONE.movePointLeft((int) n.value());
            String u = unit.toPlainString();
            String h = unit.divide(java.math.BigDecimal.valueOf(2)).toPlainString();
            String twice = unit.multiply(java.math.BigDecimal.valueOf(2)).toPlainString();
            SqlWriter.Piece x = w -> w.append("(").expr(a.get(0), 0).append(")");
            SqlWriter.Piece rounded = w -> w.append("ROUND(").piece(x).append(", " + n.value() + ")");
            return writer.append("(CASE WHEN abs(").piece(x).append(" % " + u + ") = " + h + " AND ").piece(rounded)
                    .append(" % " + twice + " <> 0 THEN ").piece(rounded).append(" - sign(").piece(x).append(") * "
                            + u + " ELSE ").piece(rounded).append(" END)");
        }
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
        // a value built of literals under a BIGINT-declared slot spells the slot's type too: DuckDB types a bare
        // literal by its magnitude (INT32 below 2^31), arithmetic of such literals and the struct's field with it,
        // and arithmetic over the field then overflows at 32 bits (3,661 seconds times a billion, the Cypher
        // island's duration leg, measured 1.4.4); a column's own type and a cast's stand
        return f.declared() != null
                && (f.value().type() instanceof com.legend.sql.TypeFact.Bottom
                        || f.declared() == com.legend.sql.SqlType.Scalar.BIGINT
                                && !(f.value() instanceof SqlExpr.Column) && !(f.value() instanceof SqlExpr.Cast))
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

    /**
     * A Variant's elements (toMany(@Variant)): an array's, a JSON null element kept as JSON's null -- a cast of the
     * ARRAY to JSON[] makes it SQL's NULL, which a list reads as no element (2026-10-12, the #SQL{}# line:
     * jsonb_array_elements and an array's length disagreed with Postgres's); NULL for SQL's NULL or JSON's null; any
     * other value is the error Pure's toMany raises (PCT testToManyFromNonArray), as the cast's was. The wildcard's
     * list is spelled {@code CAST(json_extract(.., '$[*]') AS JSON[])} (2026-10-17, docs/CYPHER_STUDY_2026_10_15.md
     * §12.62): DuckDB's binder types {@code json_extract}'s wildcard over a constant NULL input as JSON, not a list,
     * and {@code list_transform} over the CASE then fails to bind; the cast of a list to its own type is the
     * identity, and the Cypher island's typed null Variant ({@code CAST(NULL AS JSON)}, a class lacking a property
     * typed two ways) is such a constant.
     */
    @Override
    protected SqlWriter variantElements(SqlWriter writer, List<SqlExpr> args) {
        SqlWriter.Piece json = w -> {
            w.append("CAST(");
            if (storedVariant(args.get(0))) {
                navigated(w, args.get(0));
            } else {
                w.expr(args.get(0), 0);
            }
            w.append(" AS JSON)");
        };
        return writer.append("(CASE WHEN json_type(").piece(json).append(") = 'ARRAY' THEN CAST(json_extract(")
                .piece(json).append(", '$[*]') AS JSON[]) WHEN ").piece(json).append(" IS NULL OR json_type(")
                .piece(json).append(") = 'NULL' THEN NULL ELSE error('Expect variant that contains an ''ARRAY'',")
                .append(" but got ''' || json_type(").piece(json).append(") || '''') END)");
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

    /** {@inheritDoc} Its JSON_TYPE answers DuckDB's names (its own json_type). */
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

    /** {@inheritDoc} A DATE is {@code DATE}; no other type's name is compared. */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        if (type == com.legend.sql.SqlType.Scalar.DATE) {
            return "DATE";
        }
        throw new DialectCapability("a TYPEOF compared with " + type + "'s name reached DuckDB");
    }

    // the probe plan's facts (2026-10-20): measured 2026-10-20: 70,000 columns answered, no limit met; setTransactionIsolation refused, every transaction a snapshot
    @Override
    public KeysSnapshot keysSnapshot() {
        return KeysSnapshot.TRANSACTION;
    }

    @Override
    public @com.legend.base.Nullable Integer resultColumnLimit() {
        return null;
    }

    /** {@inheritDoc} Measured on DuckDB 1.4 (2026-10-18, the writes' leg 2): the standard spellings, an alias written with
     *  {@code AS}; each statement atomic on an error (a key or NOT NULL violation leaves the table as it was). */
    @Override
    protected SqlWriter insertSelect(com.legend.sql.SqlDml.InsertSelect is) {
        return standardInsertSelect(is);
    }

    @Override
    protected SqlWriter update(com.legend.sql.SqlDml.Update u) {
        return standardUpdate(u);
    }

    @Override
    protected SqlWriter deleteWhere(com.legend.sql.SqlDml.DeleteWhere dw) {
        return standardDeleteWhere(dw);
    }

    @Override
    protected SqlWriter merge(com.legend.sql.SqlDml.Merge m) {
        return standardMerge(m);
    }
}
