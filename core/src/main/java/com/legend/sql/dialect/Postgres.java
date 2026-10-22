// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;
import java.util.Set;

/**
 * The PostgreSQL EXECUTION dialect, version 16 and later (2026-10-01 W5.5/P1 Postgres
 * dialect; docs/POSTGRES_BACKEND.md is the probe study). A sibling of {@link H2} and
 * {@link DuckDb} over {@link AnsiSqlRenderer}: it overrides the base's DuckDB-isms and
 * changes nothing the other dialects render.
 *
 * <p>Product contract: {@code render(SqlQuery)} is ONE SELECT with no trailing
 * {@code ;} — the warehouse splices it into {@code COPY (SELECT cols FROM (<sql>) sub)
 * TO STDOUT} — and every projection is labelled, so output names are what the plan
 * declares. Postgres folds an unquoted identifier to lowercase, so EVERY identifier is
 * quoted: a Database must declare its tables and columns in their physical case.
 *
 * <p>Every {@link SqlFn} and {@link SqlAgg.Fn} is decided HERE by an exhaustive switch:
 * its spelling, a shared helper of the base named in its arm, or a {@link DialectCapability} wall. A wall is
 * honest; a guessed spelling is a silent wrong answer. Collections, structs, lambdas,
 * folds and variant navigation are walls until the jsonb carrier lands (leg P4;
 * POSTGRES_BACKEND.md §5: never Postgres' native ARRAY).
 */
public final class Postgres extends AnsiSqlRenderer {

    /** PostgreSQL 16+ (2026-10-01 W5.5/P1 Postgres dialect). The {@code Postgres} dialect
     * QUOTES EVERY identifier (Postgres folds an unquoted name to lowercase, so a bare
     * {@code NAME} reads {@code name}), so this list never decides a spelling there; it
     * is the documented reserved set (SQL key words appendix, "reserved" in the
     * PostgreSQL column) for any reader that asks. */
    public static final Lexicon LEXICON = new Lexicon('"', Set.of(
            "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric",
            "authorization", "binary", "both", "case", "cast", "check", "collate", "collation",
            "column", "concurrently", "constraint", "create", "cross", "current_catalog",
            "current_date", "current_role", "current_schema", "current_time",
            "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct",
            "do", "else", "end", "except", "false", "fetch", "for", "foreign", "freeze", "from",
            "full", "grant", "group", "having", "ilike", "in", "initially", "inner", "intersect",
            "into", "is", "isnull", "join", "lateral", "leading", "left", "like", "limit",
            "localtime", "localtimestamp", "natural", "not", "notnull", "null", "offset", "on",
            "only", "or", "order", "outer", "overlaps", "placing", "primary", "references",
            "returning", "right", "select", "session_user", "similar", "some", "symmetric",
            "system_user", "table", "tablesample", "then", "to", "trailing", "true", "union",
            "unique", "user", "using", "variadic", "verbose", "when", "where", "window", "with"));

    /** Postgres (2026-10-01 W5.5/P1 Postgres dialect): the portable
     * strategies (no native-list carrier — jsonb, POSTGRES_BACKEND.md §5)
     * WITHOUT H2's FULL OUTER emulation: Postgres joins FULL OUTER
     * natively. */
    public static final CarrierStrategies.Caps CAPS = new CarrierStrategies.Caps(false, true, true, true);

    public Postgres() {
        super(LEXICON);
    }

    /** Postgres's {@code FUNC_MAX_ARGS}: a call takes at most 100 arguments ("cannot pass more than 100 arguments to a
     *  function", measured 2026-10-18 on a concatenation of a spelled list's texts). */
    @Override
    protected int maxFunctionArguments() {
        return 100;
    }

    // ==================================================================
    // The catalog: a Postgres table's model, from Postgres's own catalog
    // ==================================================================

    /** This database's name for each scalar type, a refusal where it has none (the dialect program's Phase 1,
     *  2026-10-13: an exhaustive switch, so a new scalar type asks this dialect too). */
    @Override
    protected String scalarTypeName(com.legend.sql.SqlType.Scalar s) {
        return switch (s) {
            case BOOLEAN -> "BOOLEAN";
            case INTEGER -> "INTEGER";
            case BIGINT -> "BIGINT";
            case HUGEINT -> "NUMERIC(38)";
            case DOUBLE -> "DOUBLE PRECISION";
            case VARCHAR, LITERAL, TEMPORAL_TEXT -> "VARCHAR";
            case DATE -> "DATE";
            case TIMESTAMP -> "TIMESTAMP";
            case TIMESTAMPTZ -> "TIMESTAMPTZ";
            case JSON -> "JSONB";
            case TIME -> "TIME";
            case DECIMAL_TEXT -> 
                    throw new IllegalStateException(s + " cast reached a dialect without " + s + " support");
        };
    }

    /** No STRUCT type here: a struct cast is refused. */
    @Override
    protected String structTypeName(com.legend.sql.SqlType.Struct st) {
        throw new DialectCapability("a STRUCT type reached a dialect without struct support");
    }

    /**
     * A Postgres column, from Postgres's OWN catalog (the warehouse asks it inside the attached
     * database, {@code pg_catalog}; docs/STORE_TYPES_HOMEWORK_2026_10_02.md step 7): its canonical
     * type is the base type's {@code pg_catalog} name ({@code int4}, {@code numeric}, {@code inet};
     * a domain is its base type's), or, outside {@code pg_catalog}, the kind information_schema
     * names -- {@code ARRAY}, {@code ENUM}, {@code COMPOSITE}, {@code RANGE}, {@code USER-DEFINED}; a
     * {@code numeric} column's precision and scale come as numbers. Never DuckDB's view of the table:
     * it reads a Postgres array as a LIST and a {@code point} as a STRUCT, which this dialect would
     * read as {@code jsonb} (Postgres refuses the cast), and {@code inet} as VARCHAR, which Postgres
     * will not search as text.
     */
    @Override
    public CatalogType catalogType(CatalogModel.Column column) {
        return CATALOG_RULES.typeOf(column);
    }

    /**
     * Every built-in Postgres 17 base type (its {@code pg_catalog} name, upper-cased) and every
     * information_schema kind, decided. A number, a text, a date or a timestamp is declared as
     * itself; json and jsonb are a Variant (read as {@code jsonb}); bytes are left out, by name; every
     * other type -- arrays, enums, uuid, interval, times of day, network, geometric and range types,
     * money, xml, bit strings, text search, object identifiers, and any user or extension type -- is
     * OTHER: a Pure String, read as its text (every Postgres type has one) wherever it is referenced.
     */
    public static final java.util.Map<String, CatalogType> CATALOG_TYPES = catalogTypes();

    /** Postgres's catalog decisions as one {@link CatalogRules}: an unconstrained numeric is a DOUBLE. */
    public static final CatalogRules CATALOG_RULES = new CatalogRules("Postgres", CATALOG_TYPES,
            java.util.Map.of(), java.util.Map.of(), "NUMERIC", CatalogType.asStored("DOUBLE"));

    private static java.util.Map<String, CatalogType> catalogTypes() {
        java.util.Map<String, CatalogType> m = new java.util.LinkedHashMap<>();
        CatalogType text = CatalogType.asStored("VARCHAR(4096)");
        CatalogType other = CatalogType.asStored("OTHER");
        m.put("BOOL", CatalogType.asStored("BIT"));
        m.put("INT2", CatalogType.asStored("SMALLINT"));
        m.put("INT4", CatalogType.asStored("INTEGER"));
        m.put("INT8", CatalogType.asStored("BIGINT"));
        m.put("FLOAT4", CatalogType.asStored("REAL"));
        m.put("FLOAT8", CatalogType.asStored("DOUBLE"));
        m.put("TEXT", text);
        m.put("VARCHAR", text);
        m.put("BPCHAR", text);
        m.put("NAME", text);
        m.put("DATE", CatalogType.asStored("DATE"));
        m.put("TIMESTAMP", CatalogType.asStored("TIMESTAMP"));
        // a zoned timestamp is its UTC instant: read in place under the UTC session the attach pins,
        // and a copy (in the tab's DuckDB, where conversions run) holds its UTC wall time
        m.put("TIMESTAMPTZ", CatalogType.copyConverted("TIMESTAMP", "CAST(timezone('UTC', %s) AS TIMESTAMP)"));
        m.put("JSON", CatalogType.asStored("SEMISTRUCTURED"));
        m.put("JSONB", CatalogType.asStored("SEMISTRUCTURED"));
        m.put("BYTEA", CatalogType.leftOut("bytes: no Pure Database type holds them"));
        for (String t : List.of(
                // the kinds outside pg_catalog
                "ARRAY", "ENUM", "COMPOSITE", "RANGE", "USER-DEFINED",
                // numbers Pure cannot hold as numbers: money is formatted, an oid an identifier
                "MONEY", "OID", "REGCLASS", "REGCOLLATION", "REGCONFIG", "REGDICTIONARY", "REGNAMESPACE",
                "REGOPER", "REGOPERATOR", "REGPROC", "REGPROCEDURE", "REGROLE", "REGTYPE",
                // times of day and spans
                "TIME", "TIMETZ", "INTERVAL",
                // geometric, network, bit strings
                "POINT", "LINE", "LSEG", "BOX", "PATH", "POLYGON", "CIRCLE", "INET", "CIDR", "MACADDR", "MACADDR8",
                "BIT", "VARBIT",
                // built-in ranges and multiranges
                "INT4RANGE", "INT8RANGE", "NUMRANGE", "DATERANGE", "TSRANGE", "TSTZRANGE",
                "INT4MULTIRANGE", "INT8MULTIRANGE", "NUMMULTIRANGE", "DATEMULTIRANGE", "TSMULTIRANGE", "TSTZMULTIRANGE",
                // the rest of pg_catalog's base types
                "UUID", "XML", "JSONPATH", "TSVECTOR", "TSQUERY", "GTSVECTOR", "CHAR", "ACLITEM", "CID", "TID", "XID",
                "XID8", "PG_LSN", "PG_SNAPSHOT", "TXID_SNAPSHOT", "REFCURSOR", "PG_NODE_TREE", "PG_NDISTINCT",
                "PG_DEPENDENCIES", "PG_MCV_LIST", "PG_BRIN_BLOOM_SUMMARY", "PG_BRIN_MINMAX_MULTI_SUMMARY")) {
            m.put(t, other);
        }
        return java.util.Collections.unmodifiableMap(m);
    }

    // ==================================================================
    // Session and scripts (the JVM lane; the product path pins the zone in
    // the attach DSN — a planner has no connection)
    // ==================================================================

    /** The platform's naive-UTC temporal contract (POSTGRES_BACKEND.md §6: the
     * session zone is necessary, not sufficient — the JVM pins TZ=UTC too). */
    @Override
    public List<String> sessionSetup() {
        return List.of("SET TimeZone='UTC'");
    }

    /** Postgres DDL is transactional: an effect segment is one transaction, so a
     * failing statement applies nothing (DuckDB's form). */
    @Override
    public String script(List<String> statements) {
        return "BEGIN TRANSACTION;\n" + String.join(";\n", statements) + ";\nCOMMIT;";
    }

    @Override
    public String scriptAbort() {
        return "ROLLBACK";
    }

    /** {@inheritDoc} Written beside this dialect ({@code PostgresHostOperations}). */
    @Override
    public HostOperations hostOperations() {
        return PostgresHostOperations.INSTANCE;
    }

    /** The engine's H2 text in the flavor DuckDB shares, its column types Postgres's (2026-10-12,
     *  docs/CLOUD_BACKENDS_PCT_2026_10_11.md §20: the 8-byte double is DOUBLE PRECISION, and SMALLINT is the smallest
     *  integer, there is no TINYINT) and a bare column name in lower case, as Postgres folds a bare name and spells a
     *  store's ({@link #physicalName}, decision D8). */
    @Override
    public String fromEngineText(String statement) {
        return RawSqlBoundary.translated(statement,
                types -> ENGINE_TINYINT.matcher(ENGINE_DOUBLE.matcher(types).replaceAll("DOUBLE PRECISION"))
                        .replaceAll("SMALLINT"),
                bare -> bare.toLowerCase(java.util.Locale.ROOT));
    }

    private static final java.util.regex.Pattern ENGINE_DOUBLE =
            java.util.regex.Pattern.compile("(?i)\\bDOUBLE\\b(?!\\s+PRECISION)");

    private static final java.util.regex.Pattern ENGINE_TINYINT = java.util.regex.Pattern.compile("(?i)\\bTINYINT\\b");

    /** No native PIVOT: static pivots are emulated by the carrier pass; a dynamic
     * pivot needs a key-discovery round trip the browser planner cannot make. */
    // the probe plan's facts (2026-10-20): Postgres: "target lists can have at most 1664 entries" (MaxTupleAttributeNumber); REPEATABLE READ is its snapshot
    @Override
    public KeysSnapshot keysSnapshot() {
        return KeysSnapshot.REPEATABLE_READ;
    }

    @Override
    public @com.legend.base.Nullable Integer resultColumnLimit() {
        return 1664;
    }

    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    // ==================================================================
    // Passes and structure
    // ==================================================================

    /** The portable carrier strategies (static pivot, as-of emulation; FULL OUTER
     * stays native), the substring start clamp (Postgres counts empties before 1,
     * like DuckDB), string functions' 64-bit counts as its 32-bit integer, averages delivered as DOUBLE ({@code avg(int)} is numeric here),
     * QUALIFY as a wrapping subselect (Postgres has no QUALIFY), and a constant GROUP BY or
     * ORDER BY key as a typed expression (Postgres reads a bare one as a position, or refuses
     * it). */
    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CAPS),
                new StarExceptToColumns(),
                new WholePartitionOrderedSets(),
                new SubstringClamp(), new StringCountsAsInt(), new AggregatesDeliverDouble(DOUBLE_AGGREGATES),
                new QualifyToSubselect(true),
                new ConstantKeysAsExpressions(), new RootNumericTypes());
    }

    /** Postgres 12+ evaluates a {@code MATERIALIZED} CTE once. */
    @Override
    protected String cteAs(com.legend.sql.SqlWith.Cte c) {
        return c.materialized() ? " AS MATERIALIZED (" : " AS (";
    }

    // ==================================================================
    // Lists: a list of scalars is a NATIVE ARRAY (BIGINT[], VARCHAR[] ...). The IR types every
    // list (SqlType.Array(element)): one level deep and one type, so Postgres's rectangular arrays
    // hold it exactly, and JDBC reads it back as DuckDB's (java.sql.Array). Lambdas, sorts and
    // reductions run over its elements: unnest(xs) WITH ORDINALITY, the lambda's parameter the
    // element's column. A nested list, a struct or a mixed list is the jsonb carrier (the next step):
    // refused by name, never guessed.
    // ==================================================================

    private static @com.legend.base.Nullable SqlType typeOf(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
    }

    /** The element type of a list, or null for a value that is no list. */
    private static @com.legend.base.Nullable SqlType listElement(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof SqlType.Array a
                ? a.element() : null;
    }

    /** A value Postgres holds as itself inside a list: a scalar (a Variant, a struct, a map or a list is
     *  held as jsonb, so every list is flat). */
    private static boolean heldAsItself(SqlType t) {
        return t instanceof SqlType.Decimal || t instanceof SqlType.Scalar sc && sc != SqlType.Scalar.JSON;
    }

    /** The SQL type an element of type {@code t} is held as in a list. */
    private String carrier(SqlType t) {
        return heldAsItself(t) ? castTypeName(t) : "JSONB";
    }

    /** {@code list}, as a piece, when it is a list; refused by name otherwise, at once (an untyped list cannot
     *  be read safely). */
    private SqlWriter.Piece listOf(SqlExpr list, Object what) {
        elementOf(list, what);
        return w -> w.expr(list, 0);
    }

    // ---- maps: a map is its entries in order, a JSONB[] of {"key": k, "value": v} (a list of structs, as BigQuery's
    // ARRAY<STRUCT<key, value>>; the executor decodes it to the map, Executor.unwrap). DuckDB's semantics (BigQuery's
    // notes, measured there on 1.5): entries keep their order; a NULL key raises "Map keys can not be NULL.", a
    // repeated one "Map keys must be unique."; map(ks, vs) of unequal lengths raises; a concatenation keeps each key at
    // its first place with the value of the last map holding it, a NULL map adding nothing; an extraction is the key's
    // value as a list, empty when it is absent; a NULL map's keys, values or extraction are NULL.

    @Override
    protected String mapTypeName(String key, String value) {
        return "JSONB[]";
    }

    private SqlWriter maps(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            case MAP_EMPTY -> writer.append("CAST(ARRAY[] AS JSONB[])");
            // each entry a struct of two fields (a pair's first and second), by position whatever their names
            case MAP_FROM_ENTRIES -> {
                SqlExpr xs = a.get(0);
                if (!(listElement(xs) instanceof SqlType.Struct entry) || entry.fields().size() != 2) {
                    throw wall(c.fn(), "entries of " + listElement(xs));
                }
                String k = stringLit(entry.fields().get(0).name());
                String v = stringLit(entry.fields().get(1).name());
                SqlWriter.Piece entries = w -> w.append("ARRAY(SELECT jsonb_build_object('key', __me -> " + k
                        + ", 'value', __me -> " + v + ") FROM ").piece(unnest(p -> p.expr(xs, 0)))
                        .append(" WITH ORDINALITY AS __mu(__me, __mo) ORDER BY __mo)");
                yield writer.append("(CASE WHEN ").expr(xs, 4).append(" IS NULL THEN NULL ELSE ")
                        .piece(keysChecked(entries)).append(" END)");
            }
            // DuckDB's map(ks, vs): the keys zipped with the values by position
            case MAP_FROM_LISTS -> {
                SqlExpr ks = a.get(0);
                SqlExpr vs = a.get(1);
                SqlType kt = elementOf(ks, c.fn());
                SqlType vt = elementOf(vs, c.fn());
                SqlWriter.Piece entries = w -> w.append("ARRAY(SELECT jsonb_build_object('key', ")
                        .piece(asJsonb(v -> v.append("__mk"), kt)).append(", 'value', ")
                        .piece(asJsonb(v -> v.append("(").expr(vs, 0).append(")[__mo]"), vt)).append(") FROM ")
                        .piece(unnest(p -> p.expr(ks, 0))).append(" WITH ORDINALITY AS __mu(__mk, __mo) ORDER BY __mo)");
                yield writer.append("(CASE WHEN ").expr(ks, 4).append(" IS NULL OR ").expr(vs, 4)
                        .append(" IS NULL THEN NULL WHEN cardinality(").expr(ks, 0).append(") <> cardinality(")
                        .expr(vs, 0).append(") THEN ").piece(mapRaise("The map key list does not align with the map"
                                + " value list.")).append(" ELSE ").piece(keysChecked(entries)).append(" END)");
            }
            // the entries of every map, each key at its first place (map, then position) with the last map's value
            case MAP_CONCAT -> {
                writer.append("ARRAY(SELECT jsonb_build_object('key', __mg.k, 'value', (array_agg(__mg.v ORDER BY"
                        + " __mg.s DESC))[1]) FROM (");
                for (int i = 0; i < a.size(); i++) {
                    SqlExpr map = a.get(i);
                    writer.append(i > 0 ? " UNION ALL " : "").append("SELECT " + i + " AS s, __me -> 'key' AS k,"
                            + " __me -> 'value' AS v, __mo AS o FROM ").piece(unnest(p -> p.expr(map, 0)))
                            .append(" WITH ORDINALITY AS __mu" + i + "(__me, __mo)");
                }
                yield writer.append(") AS __mg GROUP BY __mg.k ORDER BY min(__mg.s), (array_agg(__mg.o ORDER BY"
                        + " __mg.s))[1])");
            }
            case MAP_KEYS, MAP_VALUES, MAP_EXTRACT -> {
                SqlExpr m = a.get(0);
                if (!(typeOf(m) instanceof SqlType.Map mt)) {
                    throw wall(c.fn(), "a map of unknown key and value types");
                }
                boolean keys = c.fn() == SqlFn.MAP_KEYS;
                writer.append("(CASE WHEN ").expr(m, 4).append(" IS NULL THEN NULL ELSE ARRAY(SELECT ")
                        .piece(held(w -> w.append(keys ? "__me -> 'key'" : "__me -> 'value'"), keys ? mt.key()
                                : mt.value(), 0)).append(" FROM ").piece(unnest(p -> p.expr(m, 0)))
                        .append(" WITH ORDINALITY AS __mu(__me, __mo)");
                if (c.fn() == SqlFn.MAP_EXTRACT) {
                    writer.append(" WHERE __me -> 'key' = ").piece(asJsonb(w -> w.expr(a.get(1), 0), mt.key()));
                }
                yield writer.append(" ORDER BY __mo) END)");
            }
            default -> throw new IllegalStateException("not a map call: " + c.fn());
        };
    }

    /** A value of type {@code t} as jsonb: a struct or a Variant is jsonb already, anything else converted, as its
     *  type (an untyped literal is refused by to_jsonb: "could not determine polymorphic type", measured). */
    private SqlWriter.Piece asJsonb(SqlWriter.Piece v, SqlType t) {
        if (t instanceof SqlType.Struct || t == SqlType.Scalar.JSON) {
            return v;
        }
        String type = t instanceof SqlType.Array arr ? carrier(arr.element()) + "[]" : castTypeName(t);
        return w -> w.append("to_jsonb(CAST(").piece(v).append(" AS " + type + "))");
    }

    /** {@code entries} when their keys are each non-NULL and distinct; DuckDB's error otherwise. */
    private SqlWriter.Piece keysChecked(SqlWriter.Piece entries) {
        return w -> w.append("(CASE WHEN (SELECT count(*) FROM ").piece(unnest(entries))
                .append(" AS __mk(e) WHERE jsonb_typeof(e -> 'key') = 'null') > 0 THEN ")
                .piece(mapRaise("Map keys can not be NULL.")).append(" WHEN (SELECT count(DISTINCT e -> 'key') FROM ")
                        .piece(unnest(entries)).append(" AS __mk(e)) < cardinality(").piece(entries)
                .append(") THEN ").piece(mapRaise("Map keys must be unique.")).append(" ELSE ").piece(entries)
                .append(" END)");
    }

    /** DuckDB's map error, raised as a map (the CASE's type). */
    private SqlWriter.Piece mapRaise(String message) {
        SqlWriter.Piece text = raise(List.of(new SqlExpr.StringLit(message)), null, true);
        return w -> w.append("CAST(").piece(text).append(" AS JSONB[])");
    }

    /** Pure's splitPart: the non-empty tokens, 1-based (the lowering's index), NULL past either end, as an array
     *  index is (measured); the empty delimiter is the lowering's. */
    @Override
    protected SqlWriter splitPartCall(SqlWriter writer, List<SqlExpr> a) {
        return writer.append("(array_remove(string_to_array(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                .append("), ''))[").expr(a.get(2), 0).append("]");
    }

    /** The calls over the empty collection whose answer is the empty collection (DuckDB's over a NULL list): a
     *  reduction, an element, or the list rearranged. */
    private static final java.util.Set<SqlFn> EMPTY_IN_EMPTY_OUT = java.util.Set.of(SqlFn.LIST_MIN, SqlFn.LIST_MAX,
            SqlFn.LIST_SUM, SqlFn.LIST_AVG, SqlFn.LIST_MEDIAN, SqlFn.LIST_MODE, SqlFn.LIST_GET, SqlFn.LIST_SLICE,
            SqlFn.LIST_TAIL, SqlFn.LIST_INIT, SqlFn.LIST_SORT, SqlFn.LIST_SORT_DESC, SqlFn.LIST_REVERSE,
            SqlFn.LIST_DISTINCT);

    /** The empty collection where a list goes: a NULL of no type (the IR's Bottom), compacted or not, or one of the
     *  calls above over it, which the IR types Bottom too. */
    /** The empty list literal, typed or not. */
    private static boolean emptyListLiteral(SqlExpr e) {
        SqlExpr v = e instanceof SqlExpr.Cast c && c.target() instanceof SqlType.Array ? c.value() : e;
        return v instanceof SqlExpr.ArrayLit a && a.elements().isEmpty();
    }

    private static boolean emptyCollection(SqlExpr e) {
        if (e instanceof SqlExpr.CompactList k) {
            return emptyCollection(k.list());
        }
        if (e instanceof SqlExpr.Call c) {
            return c.type() instanceof com.legend.sql.TypeFact.Bottom && EMPTY_IN_EMPTY_OUT.contains(c.fn())
                    && !c.args().isEmpty() && emptyCollection(c.args().get(0));
        }
        return e instanceof SqlExpr.NullLit && e.type() instanceof com.legend.sql.TypeFact.Bottom;
    }

    /** The element type of {@code list}; refused by name when it is no list of a known element type. */
    private static SqlType elementOf(SqlExpr list, Object what) {
        SqlType element = listElement(list);
        if (element == null) {
            throw new DialectCapability(what + " over " + list.type() + " reached Postgres: a list of unknown"
                    + " element type");
        }
        return element;
    }

    /** A value of type {@code t} read out of jsonb {@code json} (a list element held as jsonb, a struct
     *  field): a scalar by its text, a list rebuilt as a native list, anything else the jsonb itself. */
    private SqlWriter.Piece decode(SqlWriter.Piece json, SqlType t, int depth) {
        if (heldAsItself(t)) {
            return w -> w.append("CAST((").piece(json).append(" #>> '{}') AS ").append(castTypeName(t)).append(")");
        }
        // a map is its entries, a jsonb[] (mapTypeName): rebuilt as a list of jsonb
        if (t instanceof SqlType.Map) {
            return decode(json, new SqlType.Array(SqlType.Scalar.JSON), depth);
        }
        if (t instanceof SqlType.Array a) {
            String e = "__e" + depth;
            String o = "__o" + depth;
            // NULL for SQL's NULL or jsonb's null; any other value that is not an array is jsonb_array_elements's
            // error, as Pure's toMany raises one and DuckDB's cast does (it was an empty list here: 2026-10-12, the
            // #SQL{}# line)
            return w -> w.append("(CASE WHEN ").piece(json).append(" IS NULL OR jsonb_typeof(").piece(json)
                    .append(") = 'null' THEN NULL ELSE ARRAY(SELECT ")
                    .piece(held(v -> v.append(e), a.element(), depth + 1)).append(" FROM jsonb_array_elements(")
                    .piece(json).append(") WITH ORDINALITY AS __j" + depth + "(" + e + ", " + o + ") ORDER BY " + o)
                    .append(") END)");
        }
        // an instance: NULL for jsonb's null too, as a list's is (an absent property crossed as JSON's null, and a
        // field read of it was a present, empty instance; step 9, 2026-10-19)
        if (t instanceof SqlType.Struct) {
            return w -> w.append("NULLIF(").piece(json).append(", 'null'::jsonb)");
        }
        return json;
    }

    /** An element of type {@code t} read from jsonb as a list holds it: a scalar decoded, anything else
     *  kept as jsonb. */
    private SqlWriter.Piece held(SqlWriter.Piece json, SqlType t, int depth) {
        return heldAsItself(t) ? decode(json, t, depth) : json;
    }

    /** The type a list's elements are held as: their own when held as themselves, jsonb otherwise. */
    private static SqlType held(SqlExpr list) {
        SqlType e = java.util.Objects.requireNonNull(listElement(list));
        return heldAsItself(e) ? e : SqlType.Scalar.JSON;
    }

    /** A value of type {@code t} as a list holds it: a list or a map (its entries, a jsonb[]) as jsonb, anything
     *  else as it is. */
    private static SqlWriter.Piece encode(SqlWriter.Piece value, @com.legend.base.Nullable SqlType t) {
        return rebuilt(t) ? w -> w.append("to_jsonb(").piece(value).append(")") : value;
    }

    /** Whether a list holds a value of type {@code t} as jsonb and rebuilds it when read: a list, or a map, which
     *  is a jsonb[] of its entries (a list of maps had been an array of arrays: PR #34's review, 2026-10-13). */
    private static boolean rebuilt(@com.legend.base.Nullable SqlType t) {
        return t instanceof SqlType.Array || t instanceof SqlType.Map;
    }

    /** {@code unnest(list)}, the list held behind a scalar subquery, at every unnest the dialect writes: Postgres
     *  estimates the function's rows while planning, and the estimate evaluates the argument's constant
     *  sub-expressions (estimate_expression_value runs STABLE functions: a {@code toOne} guard's raise, the cast of
     *  a constant text to TIMESTAMPTZ, in a CASE arm never taken, fired "invalid input syntax for type timestamp
     *  with time zone") after pulling simple subqueries up, so the argument holds by then the expression of every
     *  column it reads (the Cypher island's UNWIND of a CASE over a Variant's member, whose guard raises; measured
     *  on 16.15, 2026-10-17: the raise was in the subquery's projection, not in the argument as written). A scalar
     *  subquery reaches the estimate as a parameter, which it does not open, and the executor reads it per row. The
     *  raise's own volatile term ({@link #raise}) covers only a raise typed as a list or a map, the kind that IS an
     *  argument, not the one a column carries. */
    private static SqlWriter.Piece unnest(SqlWriter.Piece list) {
        return w -> w.append("unnest((SELECT ").piece(list).append("))");
    }

    /** {@code ... FROM} the elements of {@code xs}: each bound to {@code elem} as its type reads (a list
     *  element held as jsonb rebuilt as a list), its 1-based position to {@code idx}. */
    private SqlWriter.Piece elementsFrom(SqlWriter.Piece xs, SqlType element, String elem, String idx) {
        if (!rebuilt(element)) {
            return w -> w.piece(unnest(xs)).append(" WITH ORDINALITY AS __u(" + elem + ", " + idx + ")");
        }
        return w -> w.append("(SELECT ").piece(decode(v -> v.append("__ue"), element, 0))
                .append(" AS " + elem + ", __uo AS " + idx + " FROM ").piece(unnest(xs))
                .append(" WITH ORDINALITY AS __w(__ue, __uo)) AS __u");
    }

    /** A lambda body over elements of {@code element} type: a field read off a struct parameter
     *  ({@code p.first}, as DuckDB spells it) becomes a field read of the jsonb object. */
    private static SqlExpr structFields(SqlExpr body, java.util.Map<String, SqlType> structParams) {
        if (body instanceof SqlExpr.Column c && c.table() != null && structParams.containsKey(c.table())) {
            // the parameter typed as the list's element: its struct type declares each field's
            return new SqlExpr.StructGet(SqlExpr.Column.of(null, c.table(), structParams.get(c.table()), true,
                    com.legend.sql.OutputCol.Origin.DERIVED), c.name(), c.type());
        }
        List<SqlExpr> kids = body.children();
        if (kids.isEmpty()) {
            return body;
        }
        List<SqlExpr> mapped = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (SqlExpr k : kids) {
            SqlExpr m = structFields(k, structParams);
            changed |= m != k;
            mapped.add(m);
        }
        return changed ? body.withChildren(mapped) : body;
    }

    /** A lambda's body, its struct parameter's fields read as jsonb fields. */
    private SqlExpr.Lambda bodyOver(SqlExpr.Lambda l, SqlType element) {
        if (!(element instanceof SqlType.Struct)) {
            return l;
        }
        return new SqlExpr.Lambda(l.params(), structFields(l.body(), java.util.Map.of(l.params().get(0), element)));
    }

    @Override
    protected SqlWriter structLit(SqlWriter writer, SqlExpr.StructLit st) {
        return writer.append("jsonb_build_object(")
                .join(st.fields(), ", ", (w, f) -> w.append(stringLit(f.name())).append(", ").expr(f.value(), 0))
                .append(")");
    }

    @Override
    protected SqlWriter structGet(SqlWriter writer, SqlExpr.StructGet g) {
        // the field's type: the read's own, else the one the struct declares for it
        SqlType type = g.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
        if (type == null && g.source().type() instanceof com.legend.sql.TypeFact.Typed st
                && st.type() instanceof SqlType.Struct struct) {
            for (SqlType.Struct.Field f : struct.fields()) {
                if (f.name().equals(g.field())) {
                    type = f.type();
                }
            }
        }
        if (type == null) {
            throw new DialectCapability("a field '" + g.field() + "' of unknown type reached Postgres");
        }
        return writer.piece(decode(w -> w.append("(").expr(g.source(), 8).append(" -> ").append(stringLit(g.field()))
                .append(")"), type, 0));
    }

    /** A reference to a lambda parameter (or the element/index of an unnest), spelled as the body
     *  spells it -- so the alias that binds it and every use agree. */
    private String param(String name) {
        return columnRef(SqlExpr.Column.derived(null, name));
    }

    /** {@code ARRAY(SELECT select FROM unnest(xs) WITH ORDINALITY AS __u(elem, idx) [WHERE where] ORDER BY
     *  order)}, NULL for a NULL list (DuckDB's list functions answer NULL there). */
    private SqlWriter.Piece overElements(SqlWriter.Piece xs, SqlType element, String elem, String idx,
            SqlWriter.Piece select, SqlWriter.@com.legend.base.Nullable Piece where, SqlWriter.Piece order) {
        return w -> {
            w.append("(CASE WHEN ").piece(xs).append(" IS NULL THEN NULL ELSE ARRAY(SELECT ").piece(select)
                    .append(" FROM ").piece(elementsFrom(xs, element, elem, idx));
            if (where != null) {
                w.append(" WHERE ").piece(where);
            }
            w.append(" ORDER BY ").piece(order).append(") END)");
        };
    }

    /** The ORDER BY of a list's elements, each {@code x} of type {@code t}: a struct orders by its fields
     *  in their declared order, as DuckDB's does -- jsonb's own order compares an object's keys in its
     *  storage order (shorter keys first), which sorted {@code {k, i, v}} by {@code i}; anything else
     *  by itself. */
    private SqlWriter.Piece sortKey(SqlWriter.Piece x, SqlType t, String direction) {
        if (t instanceof SqlType.Struct st) {
            return w -> w.join(st.fields(), ", ", (v, f) -> {
                SqlWriter.Piece field = u -> u.append("(").piece(x).append(" -> ").append(stringLit(f.name()))
                        .append(")");
                v.piece(sortKey(f.type() instanceof SqlType.Struct ? field : decode(field, f.type(), 0), f.type(),
                        direction));
            });
        }
        return w -> w.piece(x).append(direction);
    }

    /** A lambda's element and index parameters, spelled; the index is a fresh name when the lambda has one. */
    private String[] lambdaParams(SqlExpr.Lambda l) {
        return new String[] {param(l.params().get(0)), l.params().size() > 1 ? param(l.params().get(1)) : "__o"};
    }

    @Override
    protected SqlWriter listCall(SqlWriter writer, SqlFn fnName, List<SqlExpr> args) {
        SqlType first = elementOf(args.get(0), fnName);
        SqlWriter.Piece xs = w -> w.expr(args.get(0), 0);
        String x = param("__x");
        SqlWriter.Piece xp = w -> w.append(x);
        return switch (fnName) {
            case LIST_FILTER, LIST_TRANSFORM -> {
                SqlExpr.Lambda l = bodyOver((SqlExpr.Lambda) args.get(1), first);
                String[] p = lambdaParams(l);
                if (fnName == SqlFn.LIST_FILTER) {
                    // a kept element is held again as the list holds it
                    yield writer.piece(overElements(xs, first, p[0], p[1], encode(w -> w.append(p[0]), first),
                            w -> w.expr(l.body(), 0), w -> w.append(p[1])));
                }
                throw new IllegalStateException("LIST_TRANSFORM is rendered with its result type (call)");
            }
            // NULL || xs is xs, as DuckDB's list_concat
            case LIST_CONCAT -> writer.append("(").join(args, " || ", (w, e) -> w.piece(listOf(e, fnName))).append(")");
            // 1-based, negative from the end, out of range NULL (DuckDB's list_extract)
            case LIST_GET -> {
                SqlWriter.Piece at = w -> w.append("(CASE WHEN ").expr(args.get(1), 0).append(" < 0 THEN (").piece(xs)
                        .append(")[cardinality(").piece(xs).append(") + 1 + ").expr(args.get(1), 6).append("] ELSE (")
                        .piece(xs).append(")[").expr(args.get(1), 0).append("] END)");
                // an inner list is held as jsonb: read back as a list
                yield writer.piece(rebuilt(first) ? decode(at, first, 0) : at);
            }
            case LIST_POSITION -> writer.append("array_position(").piece(xs).append(", ").expr(args.get(1), 0)
                    .append(")");
            // DuckDB's list_distinct drops NULLs; first occurrence order
            case LIST_DISTINCT -> writer.append("(CASE WHEN ").piece(xs).append(" IS NULL THEN NULL ELSE ARRAY(SELECT ")
                    .append(x).append(" FROM ").piece(unnest(xs))
                    .append(" WITH ORDINALITY AS __u(" + x + ", __o) WHERE ")
                    .append(x + " IS NOT NULL GROUP BY " + x + " ORDER BY min(__o)) END)");
            case LIST_APPEND -> writer.append("array_append(").piece(xs).append(", ")
                    .piece(encode(w -> w.expr(args.get(1), 0), first)).append(")");
            // reorderings move elements as the list holds them (an inner list stays jsonb)
            case LIST_SORT -> writer.piece(overElements(xs, held(args.get(0)), x, "__o", xp, null,
                    sortKey(xp, first, "")));
            case LIST_SORT_DESC -> writer.piece(overElements(xs, held(args.get(0)), x, "__o", xp, null,
                    sortKey(xp, first, " DESC")));
            case LIST_REVERSE -> writer.piece(overElements(xs, held(args.get(0)), x, "__o", xp, null,
                    w -> w.append("__o DESC")));
            case LIST_TAIL -> writer.append("(").piece(xs).append(")[2:]");
            case LIST_INIT -> writer.append("(").piece(xs).append(")[:cardinality(").piece(xs).append(") - 1]");
            // DuckDB's array_slice: 1-based, both ends inclusive
            case LIST_SLICE -> writer.append("(").piece(xs).append(")[").expr(args.get(1), 0).append(":")
                    .expr(args.get(2), 0).append("]");
            case LIST_SUM -> writer.piece(overReduce(xs, "sum(" + x + ")"));
            // a Boolean list's extremes: false < true, and Postgres has no max(boolean) (measured)
            case LIST_MIN -> writer.piece(overReduce(xs, (first == SqlType.Scalar.BOOLEAN ? "bool_and(" : "min(") + x
                    + ")"));
            case LIST_MAX -> writer.piece(overReduce(xs, (first == SqlType.Scalar.BOOLEAN ? "bool_or(" : "max(") + x
                    + ")"));
            case LIST_AVG -> writer.piece(overReduce(xs, "CAST(avg(" + x + ") AS DOUBLE PRECISION)"));
            case LIST_MEDIAN -> writer.piece(overReduce(xs, "percentile_cont(0.5) WITHIN GROUP (ORDER BY " + x + ")"));
            case LIST_MODE -> writer.piece(overReduce(xs, "mode() WITHIN GROUP (ORDER BY " + x + ")"));
            case LIST_BOOL_AND -> writer.piece(overReduce(xs, "bool_and(" + x + ")"));
            case LIST_BOOL_OR -> writer.piece(overReduce(xs, "bool_or(" + x + ")"));
            default -> throw new IllegalStateException("not a list call: " + fnName);
        };
    }

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        // the empty collection as a list: a reduction, an element or a rearrangement of it is the empty collection
        // itself, NULL, as DuckDB answers over a NULL list (Postgres types every array: a reduction over an untyped
        // NULL is refused, "could not determine polymorphic type", measured); a concatenation is its other lists
        // (listConcat)
        if (emptyCollection(c)) {
            return writer.append("NULL");
        }
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
            case IN -> inList(writer, c.args(), parentPrec);
            case IS_DISTINCT_FROM, NULL_SAFE_NOT_EQUAL -> distinctFrom(writer, c.args());
            case NULL_SAFE_EQUAL -> notDistinctFrom(writer, c.args());
            case XOR -> xorAsAndOr(writer, c.args(), parentPrec);
            // NULL-skipping flat concat (the database's concat skips NULL arguments)
            case CONCAT -> concatFlat(writer, c.args());
            // joinStrings' interleaved concat, never flattened
            case CONCAT_JOIN -> writer.function("concat", c.args());
            // 64-bit parse (PCT Long.MIN/MAX round-trips)
            case PARSE_INT -> castAs(writer, typedText(c.args().get(0)), "BIGINT");
            // parseDate(text): the ISO text as a timestamp
            case PARSE_DATE -> castAs(writer, typedText(c.args().get(0)), "TIMESTAMP");
            case PI -> writer.append("pi()");
            case CEILING -> bigintOf(writer, "ceil", c.args());
            case FLOOR -> bigintOf(writer, "floor", c.args());
            case SIGN -> bigintOf(writer, "sign", c.args());
            case LPAD -> padWithSpaceToInteger(writer, "lpad", c.args());
            case RPAD -> padWithSpaceToInteger(writer, "rpad", c.args());
            case UC_FIRST -> firstCharCased(writer, "upper", c.args(), parentPrec);
            case LC_FIRST -> firstCharCased(writer, "lower", c.args(), parentPrec);
            case TODAY -> writer.append("current_date");
            case DATE_TRUNC_DAY -> castAs(writer, c.args().get(0), "DATE");
            case BOOL_TO_TEXT -> castAs(writer, c.args().get(0), "VARCHAR");
            case FLOAT_CANON -> floatCanonByText(writer, c.args(), parentPrec);
            case FLOAT_PRINT, FLOAT_PRINT_ENGINE -> floatPrintByText(writer, c, parentPrec);
            case CURRENT_USER_FN -> writer.append("current_user");
            // the hooks: this dialect's own (or the base's refusal)
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(writer, c.fn(), c.args());
            case ROUND -> roundHalfEven(writer, c.args());
            case HASH -> hashSigned(writer, c.args());
            // this database's plain spellings: name(args)
            case ABS -> writer.function("abs", c.args());
            case ASCII_CODE -> writer.function("ascii", c.args());
            case ATAN -> writer.function("atan", c.args());
            case ATAN2 -> writer.function("atan2", c.args());
            case CBRT -> writer.function("cbrt", c.args());
            // chr takes a 32-bit code; a computed Integer is a BIGINT here (IQ island, 2026-10-17: CHAR(65 + MOD(id, 26)))
            case CHR -> writer.append("chr(CAST(").expr(c.args().get(0), 0).append(" AS INTEGER))");
            case COALESCE -> coalesceArm(writer, c);
            case COS -> writer.function("cos", c.args());
            case COSH -> writer.function("cosh", c.args());
            case COT -> writer.function("cot", c.args());
            case DEGREES -> writer.function("degrees", c.args());
            case FLOOR_RAW -> writer.function("floor", c.args());
            case GREATEST -> writer.function("greatest", c.args());
            case LEAST -> writer.function("least", c.args());
            case LEFT -> writer.function("left", c.args());
            case LOWER -> writer.function("lower", c.args());
            case LTRIM -> writer.function("ltrim", c.args());
            case MD5 -> writer.function("md5", c.args());
            case RADIANS -> writer.function("radians", c.args());
            case REPEAT_STR -> writer.function("repeat", c.args());
            case REPLACE -> writer.function("replace", c.args());
            case TRANSLATE -> writer.function("translate", c.args());
            case REVERSE_STRING -> writer.function("reverse", c.args());
            case RIGHT -> writer.function("right", c.args());
            case RTRIM -> writer.function("rtrim", c.args());
            case SIN -> writer.function("sin", c.args());
            case SINH -> writer.function("sinh", c.args());
            case SPLIT_PART -> writer.function("split_part", c.args());
            case STARTS_WITH -> writer.function("starts_with", c.args());
            case STRPOS -> writer.function("strpos", c.args());
            case SUBSTRING -> writer.function("substr", c.args());
            case TAN -> writer.function("tan", c.args());
            case TANH -> writer.function("tanh", c.args());
            case TIMEZONE -> writer.function("timezone", c.args());
            // a date plus a time of day is the timestamp at it
            case DATE_AT_TIME -> op(writer, parentPrec, () -> writer.expr(a.get(0), 5).append(" + ")
                    .expr(a.get(1), 6));
            case TRIM -> writer.function("trim", c.args());
            case UPPER -> writer.function("upper", c.args());
            // a Float, as DuckDB's answers; each of these has a numeric overload on Postgres, which a
            // numeric argument would pick (sqrt(9.0) is 3.000000000000000)
            case SQRT -> writer.append("sqrt(CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION))");
            case EXP -> writer.append("exp(CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION))");
            case LN -> writer.append("ln(CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION))");
            case LOG10 -> writer.append("log10(CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION))");

            // ---- arithmetic
            // Pure's pow is a Float, as DuckDB's power answers; Postgres's power over numeric is numeric
            // (9.0 delivered as 9.0000000000000000, found by the Postgres PCT lane, 2026-10-02)
            case POW -> writer.append("power(CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION), CAST(")
                    .expr(a.get(1), 0).append(" AS DOUBLE PRECISION))");
            // Pure's divide is a Float division: both operands DOUBLE PRECISION
            case DIVIDE -> writer.append("(CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION) / CAST(")
                    .expr(a.get(1), 0).append(" AS DOUBLE PRECISION))");
            // Postgres has no % or mod() over double precision; integers and
            // decimals take the base's MOD forms
            case MOD, REM -> modArm(writer, c);
            // DuckDB's // truncates toward zero; so does div() (numeric), and a
            // double operand is refused by Postgres itself (no implicit cast)
            // ~x on BIGINT (the base's xor() is DuckDB's)
            case BIT_NOT -> writer.append("(~ CAST(").expr(a.get(0), 0).append(" AS BIGINT))");
            case INT_DIVIDE -> writer.append("CAST(div(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                    .append(") AS BIGINT)");
            // Pure's divide-with-scale is HALF_UP; Postgres rounds double precision
            // half-EVEN and numeric half away from zero (POSTGRES_BACKEND.md §4.3),
            // and has no round(double, int) at all
            case ROUND_HALF_UP -> roundHalfUp(writer, c);
            // the engine's out-of-domain answer is NaN; Postgres raises
            case ACOS, ASIN -> acos(writer, c);

            // ---- strings
            // the engine coerces a non-text argument (DuckDb's arm)
            case LENGTH -> a.get(0) instanceof SqlExpr.StringLit
                    ? writer.append("length(").list(a).append(")")
                    : writer.append("length(CAST(").expr(a.get(0), 0).append(" AS VARCHAR))");
            case ENDS_WITH -> writer.append("(right(").expr(a.get(0), 0).append(", length(").expr(a.get(1), 0)
                    .append(")) = ").expr(a.get(1), 0).append(")");
            // MATCHES is the PARTIAL test, Postgres' ~ (never regexp_matches: set-
            // returning, it deletes rows in a projection, POSTGRES_BACKEND.md §4.1)
            case MATCHES -> writer.append("(").expr(a.get(0), 7).append(" ~ ").piece(pattern(a.get(1), "", ""))
                    .append(")");
            // full match: ~ is partial on Postgres (§4.2), so the pattern anchors (after its options)
            case REGEXP_FULL_MATCH -> writer.append("(").expr(a.get(0), 7).append(" ~ ")
                    .piece(pattern(a.get(1), "^(?:", ")$")).append(")");
            // regexp_extract(s, p[, g]) is '' on a miss; regexp_substr is NULL
            case REGEXP_EXTRACT -> regexpExtract(writer, c);
            // regexp_replace(s, p, r[, options]): DuckDB's 'g' is Postgres's
            case REGEXP_REPLACE -> regexpReplace(writer, c);
            // Postgres' base64 wraps lines at 76 characters
            case ENCODE_BASE64 -> writer.append("replace(encode(convert_to(").expr(a.get(0), 0)
                    .append(", 'UTF8'), 'base64'), chr(10), '')");
            case DECODE_BASE64 -> writer.append("convert_from(decode(").expr(a.get(0), 0)
                    .append(", 'base64'), 'UTF8')");
            // sha256 is bytea here; the reference is lowercase hex text
            case SHA256 -> writer.append("encode(sha256(convert_to(").expr(a.get(0), 0).append(", 'UTF8')), 'hex')");
            case GUID -> writer.append("CAST(gen_random_uuid() AS VARCHAR)");
            case RANDOM -> writer.append("random()");

            // ---- temporal
            case NOW -> writer.append("(now() AT TIME ZONE 'UTC')");
            case STRFTIME -> strftime(writer, c);
            case DAYNAME -> writer.append("to_char(").piece(naive(a.get(0))).append(", 'FMDay')");
            case MONTHNAME -> writer.append("to_char(").piece(naive(a.get(0))).append(", 'FMMonth')");
            // DuckDB's date_part is an integer (seconds truncated); extract is numeric
            case EXTRACT -> extractArm(writer, c);
            case DATE_TRUNC -> writer.piece(dateTrunc(a));
            // Postgres' make_date/make_timestamp take INTEGER, never BIGINT
            case MAKE_DATE -> writer.append("make_date(")
                    .join(a, ", ", (w, x) -> w.append("CAST(").expr(x, 0).append(" AS INTEGER)")).append(")");
            case MAKE_TIMESTAMP -> makeTimestamp(writer, c);
            // (unitFn, amount, date): amount × a one-unit interval — exact for every
            // unit and any BIGINT amount (make_interval takes INTEGER)
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> op(writer, parentPrec, () -> writer.expr(a.get(2), 5)
                    .append(" + ").expr(a.get(1), 6).append(" * INTERVAL '1 ")
                    .append(intervalUnit(part(a.get(0)))).append("'"));
            case DATE_DIFF -> writer.piece(dateDiff(a));
            // the origins are the base's (weeks align to the Monday 1969-12-29, everything else to
            // 1970); date_bin bins fixed-length intervals, so months and years count whole calendar
            // months from 1970-01 and floor to the bucket's multiple, as DuckDB's time_bucket does
            // (before 1970 too: floor, not truncation)
            case TIME_BUCKET -> timeBucket(writer, c);
            // DuckDB's epoch(ts) is DOUBLE seconds; epoch_ms truncates toward zero
            case EPOCH_SECONDS -> writer.append("CAST(extract(epoch FROM ").expr(a.get(0), 0)
                    .append(") AS DOUBLE PRECISION)");
            case EPOCH_NS, FROM_EPOCH_NS -> throw new DialectCapability("a nanosecond epoch: Postgres keeps"
                    + " microseconds (the Cypher island's nanosecond leg is DuckDB's)");
            case EPOCH_MS -> writer.append("CAST(trunc(extract(epoch FROM ").expr(a.get(0), 0)
                    .append(") * 1000) AS BIGINT)");
            // epoch arithmetic on a naive timestamp: no session zone involved
            case FROM_EPOCH_SECONDS -> op(writer, parentPrec, () -> writer.append("TIMESTAMP '1970-01-01 00:00:00' + ")
                    .expr(a.get(0), 6).append(" * INTERVAL '1 second'"));
            case FROM_EPOCH_MS -> op(writer, parentPrec, () -> writer.append("TIMESTAMP '1970-01-01 00:00:00' + CAST(")
                    .expr(a.get(0), 0).append(" AS BIGINT) * INTERVAL '1 millisecond'"));

            // error() outside a CASE branch: the raise, VARCHAR-typed (see raise)
            case ERROR -> writer.piece(raise(a, null, c.type() instanceof com.legend.sql.TypeFact.Typed t
                    && (t.type() instanceof SqlType.Array || t.type() instanceof SqlType.Map)));

            // ---- walls
            case STRPTIME -> throw wall(c.fn(), "to_timestamp(text, fmt) is lenient and"
                    + " zone-bound; not yet probed against the reference");
            case FORMAT -> writer.piece(format(a));
            // SHA-1 is pgcrypto's digest(): no release has a core sha1 (through 18 and devel, 2026-10-12). The
            // engine's Postgres spells it so (its PCT passes testSHA1Hash there); a database without pgcrypto
            // answers its own "function digest(bytea, unknown) does not exist" (2026-10-20, the parity phase)
            case SHA1 -> writer.append("encode(digest(convert_to(").expr(a.get(0), 0).append(", 'UTF8'), 'sha1'), 'hex')");
            case LEVENSHTEIN -> writer.piece(levenshtein(a.get(0), a.get(1)));
            case JARO_WINKLER -> writer.piece(jaroWinkler(a.get(0), a.get(1)));
            // ---- variant (jsonb)
            // the hooks: this dialect's own (or the base's refusal)
            case TO_VARIANT -> variantConstruct(writer, c.args());
            case VARIANT_GET -> variantGet(writer, c.args());
            // in DuckDB's json_type vocabulary, which the lowering compares against (NULL, VARCHAR,
            // BIGINT, DOUBLE, BOOLEAN, ARRAY, OBJECT): a whole number is a BIGINT, any other a DOUBLE
            case JSON_TYPE -> jsonType(writer, c);
            case JSON_ARRAY_LENGTH -> writer.append("jsonb_array_length(").expr(a.get(0), 0).append(")");
            case JSON_PRETTY -> writer.append("jsonb_pretty(").expr(a.get(0), 0).append(")");
            // a Variant array's elements, each a Variant (DuckDB: CAST(x AS JSON[]))
            // (idempotent, as DuckDB's cast: a value already a list of Variants passes through)
            case VARIANT_ELEMENTS -> listElement(a.get(0)) != null
                    ? writer.expr(a.get(0), 0)
                    : writer.piece(decode(w -> w.expr(a.get(0), 0), new SqlType.Array(SqlType.Scalar.JSON), 0));
            case JSON_MERGE_PATCH -> throw wall(c.fn(), "variant over jsonb is leg P4");
            // ---- lists: a list of scalars is a native array (listCall and its siblings below)
            // the hooks: this dialect's own (or the base's refusal)
            case LIST_FILTER, LIST_GET, LIST_POSITION, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX,
                 LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT, LIST_SORT_DESC, LIST_TAIL, LIST_INIT, LIST_SLICE,
                 LIST_BOOL_AND, LIST_BOOL_OR, LIST_REVERSE -> listCall(writer, c.fn(), c.args());
            case LIST_CONCAT -> listConcat(writer, c, parentPrec);
            // each result held as the RESULT list holds it (its type is the call's)
            case LIST_TRANSFORM -> listTransform(writer, c);
            // the list-making calls whose first argument is no list
            case RANGE_FN -> range(writer, c);
            case REPEAT_VALUE -> writer.append("array_fill(").expr(a.get(0), 0).append(", ARRAY[CAST(")
                    .expr(a.get(1), 0).append(" AS INTEGER)])");
            case LIST_EXISTS -> listExists(writer, c.args());
            case LIST_FOR_ALL -> listForAll(writer, c.args());
            case UNNEST -> unnestProjection(writer, c.args());
            case ALL_DISTINCT -> allDistinct(writer, c.args());
            case LIST_LENGTH -> writer.append("cardinality(").piece(listOf(a.get(0), c.fn())).append(")");
            case SPLIT -> split(writer, c);
            // DuckDB's regexp_extract_all(s, p[, g]): group g (0, the whole match, by default) of every
            // match -- the n-th match's group by regexp_substr, as REGEXP_EXTRACT reads one (regexp_matches
            // answers the capture groups alone once a pattern has any: its m[1] is group 1)
            case REGEXP_EXTRACT_ALL -> regexpExtractAll(writer, c);
            case MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT, MAP_CONCAT, MAP_KEYS, MAP_VALUES ->
                    maps(writer, c);
            // the jsonb carrier for nested lists, structs and mixed lists is the next step
            // a struct with one field set (struct_insert): the jsonb object, the field replaced or added
            case STRUCT_INSERT -> structInsert(writer, c);
            case PURE_SPLIT_PART -> splitPartCall(writer, a);
            case LIST_REDUCE -> listReduce(writer, c);
            case LIST_FLATTEN, LIST_ZIP, LIST_PRODUCT, TYPEOF ->
                    throw wall(c.fn(), "collections over the jsonb carrier are leg P4");
        };
    }

    /** COALESCE: an empty list arm takes another arm's type ('{}', which Postgres resolves against them): a list's type
     *  is no SQL type alone here, json[] and jsonb[] both a list of JSON (an equality canon's key texts are json, a list
     *  of JSON values jsonb: 2026-10-20, testEqualNonPrimitive). Out of {@link #call} since 2026-10-22, its length. */
    private SqlWriter coalesceArm(SqlWriter writer, SqlExpr.Call c) {
        if (c.args().stream().noneMatch(x -> !emptyListLiteral(x))) {
            return writer.function("coalesce", c.args());
        }
        return writer.append("coalesce(").join(c.args(), ", ", (w, x) -> {
            if (emptyListLiteral(x)) {
                w.append("'{}'");
            } else {
                w.expr(x, 0);
            }
        }).append(")");
    }

    /** SPLIT: DuckDB's string_split('', d) is [''], Postgres's string_to_array is {}. */
    private SqlWriter split(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        return writer.append("(CASE WHEN ").expr(a.get(0), 0)
                .append(" = '' THEN ARRAY[''] ELSE string_to_array(").expr(a.get(0), 0).append(", ")
                .expr(a.get(1), 0).append(") END)");
    }

    /** STRUCT_INSERT: a struct with one field set, the jsonb object with the field replaced or added. */
    private SqlWriter structInsert(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        return writer.append("(").expr(a.get(0), 0).append(" || jsonb_build_object(")
                .expr(a.get(1), 0).append(", ").expr(a.get(2), 0).append("))");
    }

    /** The {@code LIST_CONCAT} arm of {@link #call}: a concatenation with the empty collection is its other lists. */
    private SqlWriter listConcat(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        if (c.args().stream().noneMatch(Postgres::emptyCollection)) {
            return listCall(writer, c.fn(), c.args());
        }
        List<SqlExpr> lists = c.args().stream().filter(e -> !emptyCollection(e)).toList();
        if (lists.isEmpty()) {
            if (!(c.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() instanceof SqlType.Array arr)) {
                return writer.append("NULL");
            }
            return writer.append("CAST(NULL AS " + carrier(arr.element()) + "[])");
        }
        return lists.size() == 1 ? writer.expr(lists.get(0), parentPrec)
                : call(writer, new SqlExpr.Call(SqlFn.LIST_CONCAT, lists), parentPrec);
    }

    /** The {@code RANGE_FN} arm of {@link #call}: DuckDB's range, [start, stop) by step (start 0, step 1 by
     * default). */
    private SqlWriter range(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlWriter.Piece start = a.size() == 1 ? w -> w.append("0") : w -> w.expr(a.get(0), 0);
        SqlWriter.Piece stop = w -> w.expr(a.get(a.size() == 1 ? 0 : 1), 0);
        SqlWriter.Piece step = a.size() > 2 ? w -> w.expr(a.get(2), 0) : w -> w.append("1");
        // (generate_series's arguments: Postgres's planner estimates its rows from them, so a raise within them is
        // written as no planner evaluates, SqlWriter.planned)
        return writer.append("ARRAY(SELECT g FROM generate_series(CAST(").planned(start)
                .append(" AS BIGINT), CAST(").planned(stop).append(" AS BIGINT), CAST(").planned(step)
                .append(" AS BIGINT)) AS g WHERE CASE WHEN ").piece(step).append(" > 0 THEN g < ").piece(stop)
                .append(" ELSE g > ").piece(stop).append(" END)");
    }

    /** The {@code LIST_TRANSFORM} arm of {@link #call}: each result held as the RESULT list holds it (its type is
     * the call's). */
    private SqlWriter listTransform(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlType element = elementOf(a.get(0), c.fn());
        SqlWriter.Piece xs = w -> w.expr(a.get(0), 0);
        SqlType result = listElement(c);
        if (result == null) {
            throw new DialectCapability("a list transform of unknown result type reached Postgres");
        }
        SqlExpr.Lambda l = bodyOver((SqlExpr.Lambda) a.get(1), element);
        String[] p = lambdaParams(l);
        return writer.piece(overElements(xs, element, p[0], p[1], encode(w -> w.expr(l.body(), 0), result), null,
                w -> w.append(p[1])));
    }

    /** A date-time from its parts (the call arm's comment says how). */
    private SqlWriter makeTimestamp(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.size() != 6) {
            throw new DialectCapability("make_timestamp of " + a.size()
                    + " arguments has no Postgres spelling");
        }
        return writer.append("make_timestamp(")
                .join(a.subList(0, 5), ", ", (w, x) -> w.append("CAST(").expr(x, 0).append(" AS INTEGER)"))
                .append(", CAST(").expr(a.get(5), 0).append(" AS DOUBLE PRECISION))");
    }

    /** Pure's HALF_UP rounding: round over NUMERIC (half away from zero), a double cast back. */
    private SqlWriter roundHalfUp(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlWriter.Piece r = w -> {
            w.append("round(CAST(").expr(a.get(0), 0).append(" AS NUMERIC)");
            if (a.size() > 1) {
                w.append(", CAST(").expr(a.get(1), 0).append(" AS INTEGER)");
            }
            w.append(")");
        };
        return isDouble(a.get(0))
                ? writer.append("CAST(").piece(r).append(" AS DOUBLE PRECISION)")
                : writer.piece(r);
    }

    /** DuckDB's time_bucket on Postgres: date_bin for fixed-length units, the month index for calendar ones. */
    private SqlWriter timeBucket(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String unit = intervalUnit(part(a.get(0)));
        String origin = BUCKET_ORIGINS.get(unit);
        if (origin != null) {
            return writer.append("date_bin(").expr(a.get(1), 6).append(" * INTERVAL '1 ").append(unit)
                    .append("', ").piece(naive(a.get(2))).append(", ").append(origin).append(")");
        }
        Integer monthsPerUnit = CALENDAR_UNITS.get(unit);
        if (monthsPerUnit == null) {
            throw new DialectCapability("a " + unit + " time bucket has no Postgres spelling");
        }
        // the month index since 1970-01, floored to the bucket's size in months: n months, or
        // 12n for n years (a year bucket of the month index is the year's own, before 1970 too)
        SqlWriter.Piece t = naive(a.get(2));
        SqlWriter.Piece size = w -> w.append("(CAST(").expr(a.get(1), 0)
                .append(" AS INTEGER) * " + monthsPerUnit
                + ")");
        return writer.append("(TIMESTAMP '1970-01-01 00:00:00' + CAST(floor(((extract(year FROM ").piece(t)
                .append(") - 1970) * 12 + extract(month FROM ").piece(t).append(") - 1) / ").piece(size)
                .append(") AS INTEGER) * ").piece(size).append(" * INTERVAL '1 month')");
    }

    /** exists([]) is false, forAll([]) is true: Pure's empty-collection semantics. */
    @Override
    protected SqlWriter listExists(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "bool_or", "FALSE");
    }

    @Override
    protected SqlWriter listForAll(SqlWriter writer, List<SqlExpr> args) {
        return listPredicate(writer, args, "bool_and", "TRUE");
    }

    private SqlWriter listPredicate(SqlWriter writer, List<SqlExpr> args, String agg, String empty) {
        if (emptyCollection(args.get(0))) {
            return writer.append(empty);   // no element: the empty answer, as the coalesce below gives a NULL list
        }
        SqlType element = elementOf(args.get(0), "exists/forAll");
        SqlWriter.Piece xs = w -> w.expr(args.get(0), 0);
        SqlExpr.Lambda l = bodyOver((SqlExpr.Lambda) args.get(1), element);
        String[] p = lambdaParams(l);
        return writer.append("coalesce((SELECT " + agg + "(").expr(l.body(), 0).append(") FROM ")
                .piece(elementsFrom(xs, element, p[0], p[1])).append("), " + empty + ")");
    }

    /** No duplicates iff the distinct count is the count (a NULL element is a duplicate-free miss, as
     *  DuckDB's len(list_distinct(x)) = len(x)); an empty or NULL list is distinct. */
    @Override
    protected SqlWriter allDistinct(SqlWriter writer, List<SqlExpr> args) {
        SqlWriter.Piece xs = listOf(args.get(0), "isDistinct");
        String x = param("__x");
        return writer.append("coalesce((SELECT count(*) = count(DISTINCT " + x + ") FROM ").piece(unnest(xs))
                .append(" AS __u(" + x + ")), TRUE)");
    }

    /** DuckDB's list_contains: NULL for a NULL list; found by IS NOT DISTINCT FROM, as array_position. */
    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        SqlType element = elementOf(m.collection(), "membership");
        SqlWriter.Piece xs = w -> w.expr(m.collection(), 0);
        return writer.append("(CASE WHEN ").piece(xs).append(" IS NULL THEN NULL ELSE array_position(").piece(xs)
                .append(", ").piece(encode(w -> w.expr(m.needle(), 0), element))
                .append(") IS NOT NULL END)");
    }

    /** A named aggregate over a list's elements, in list order. */
    @Override
    protected SqlWriter reduceCollection(SqlWriter writer, SqlExpr.ReduceCollection rc) {
        SqlType element = elementOf(rc.collection(), rc.reducer());
        SqlWriter.Piece xs = w -> w.expr(rc.collection(), 0);
        SqlExpr x = SqlExpr.Column.of(null, "__x", element, true, com.legend.sql.OutputCol.Origin.DERIVED);
        List<SqlExpr> args = new java.util.ArrayList<>();
        args.add(x);
        args.addAll(rc.extras());
        SqlAgg.Reducer agg = new SqlAgg.Reducer(rc.reducer(), args, false,
                rc.reducer() == SqlAgg.Fn.STRING_AGG ? List.of(new com.legend.sql.SqlSelect.SortKey(
                        SqlExpr.Column.derived(null, "__o"), true, null, null)) : List.of());
        writer.append("(SELECT ");
        // avg and the moments over a list are a Float too, cast over their exact result as the grouped ones are
        if (DOUBLE_AGGREGATES.contains(rc.reducer())) {
            reducer(writer.append("CAST("), agg).append(" AS DOUBLE PRECISION)");
        } else {
            reducer(writer, agg);
        }
        return writer.append(" FROM ").piece(unnest(xs)).append(" WITH ORDINALITY AS __u(" + param("__x") + ", "
                + param("__o") + "))");
    }

    /** A list literal, typed (so an empty one has a type), each element held as the list holds it. */
    private SqlWriter arrayLiteral(SqlWriter writer, SqlExpr.ArrayLit a) {
        SqlType element = listElement(a);
        if (element == null) {
            throw new DialectCapability("a list literal of " + a.type() + " reached Postgres: a list of unknown"
                    + " element type");
        }
        return writer.append("CAST(ARRAY[")
                .join(a.elements(), ", ", (w, e) -> w.piece(encode(v -> v.expr(e, 0), element)))
                .append("] AS " + carrier(element) + "[])");
    }

    /** A list exploded to rows in the select list: unnest keeps the list's order. */
    @Override
    protected SqlWriter unnestProjection(SqlWriter writer, List<SqlExpr> args) {
        if (emptyCollection(args.get(0))) {
            // the empty collection exploded: no row, so no value carries its column's type (TEXT: every scalar
            // casts from it); Postgres refuses to unnest an untyped NULL
            return writer.append("unnest(CAST(NULL AS TEXT[]))");
        }
        return writer.piece(unnest(listOf(args.get(0), "UNNEST")));
    }

    /** fold(xs, {element, accumulator | body}, init): a correlated recursive walk over the elements, the
     *  accumulator typed as the fold's result (Postgres types a recursive column by its first term): a scalar, or a
     *  list of scalars, a native array (a list of anything else is jsonb, refused by name). */
    @Override
    protected SqlWriter foldCall(SqlWriter writer, SqlExpr.FoldCall f) {
        if (!(f.type() instanceof com.legend.sql.TypeFact.Typed t)
                || !(t.type() instanceof SqlType.Scalar || t.type() instanceof SqlType.Decimal
                        || t.type() instanceof SqlType.Array arr && heldAsItself(arr.element()))) {
            throw new DialectCapability("a fold into a " + f.type() + " reached Postgres: a list accumulator of"
                    + " anything but scalars is the jsonb carrier");
        }
        SqlWriter.Piece xs = listOf(f.source(), "fold");
        String type = castTypeName(t.type());
        String elem = param(f.lambda().params().get(0));
        String acc = param(f.lambda().params().get(1));
        return writer.append("(WITH RECURSIVE __f(__i, __acc) AS (SELECT 0, CAST(").expr(f.init(), 0)
                .append(" AS " + type + ")").append(" UNION ALL SELECT __f.__i + 1, CAST(").expr(f.lambda().body(), 0)
                .append(" AS " + type + ")").append(" FROM __f CROSS JOIN LATERAL (SELECT (").piece(xs)
                .append(")[__f.__i + 1] AS " + elem + ", __f.__acc AS " + acc + ") AS __b WHERE __f.__i <"
                        + " coalesce(cardinality(").piece(xs).append("), 0))")
                .append(" SELECT __acc FROM __f ORDER BY __i DESC LIMIT 1)");
    }

    /** DuckDB's list_reduce(xs, (acc, x) -> body): the first element starts the accumulator, the lambda walks the rest
     *  -- a correlated recursive walk, as a fold's, typed as the reduction (a scalar, or a list of scalars, a native
     *  array; each element read as the list holds it, a list from its jsonb). The empty list is the lowering's. */
    private SqlWriter listReduce(SqlWriter writer, SqlExpr.Call c) {
        SqlExpr xs = c.args().get(0);
        SqlType element = elementOf(xs, c.fn());
        SqlType acc = typeOf(c) instanceof SqlType t ? t : element;
        if (!(acc instanceof SqlType.Scalar || acc instanceof SqlType.Decimal
                || acc instanceof SqlType.Array arr && heldAsItself(arr.element()))) {
            throw new DialectCapability("a list reduction into a " + acc + " reached Postgres");
        }
        SqlExpr.Lambda l = (SqlExpr.Lambda) c.args().get(1);
        String type = castTypeName(acc);
        String a = param(l.params().get(0));
        String x = param(l.params().get(1));
        SqlWriter.Piece at1 = elementAt(xs, "1", element);
        SqlWriter.Piece next = elementAt(xs, "__r.__i + 1", element);
        return writer.append("(WITH RECURSIVE __r(__i, __acc) AS (SELECT 1, CAST(").piece(at1).append(" AS " + type
                + ") UNION ALL SELECT __r.__i + 1, CAST(").expr(l.body(), 0).append(" AS " + type + ") FROM __r CROSS"
                + " JOIN LATERAL (SELECT ").piece(next).append(" AS " + x + ", __r.__acc AS " + a + ") AS __b WHERE"
                + " __r.__i < coalesce(cardinality(").expr(xs, 0).append("), 0)) SELECT __acc FROM __r ORDER BY __i DESC"
                + " LIMIT 1)");
    }

    /** The element of {@code xs} at a 1-based index, as its type reads: a scalar itself, a list from its jsonb. */
    private SqlWriter.Piece elementAt(SqlExpr xs, String index, SqlType element) {
        SqlWriter.Piece at = w -> w.append("(").expr(xs, 0).append(")[" + index + "]");
        return heldAsItself(element) ? at : decode(at, element, 0);
    }

    /** A reduction of a list's elements: {@code (SELECT agg FROM unnest(xs) AS __u(__x))}. */
    private SqlWriter.Piece overReduce(SqlWriter.Piece xs, String agg) {
        return w -> w.append("(SELECT " + agg + " FROM ").piece(unnest(xs)).append(" AS __u(" + param("__x") + "))");
    }

    /** Postgres also reads a nested value as {@code jsonb}, the one nested type it can group,
     *  compare and navigate alike: a {@code json} column has no equality operator ({@code GROUP BY}
     *  refuses it), and an array or a composite is not JSON at all (measured on Postgres 17,
     *  docs/STORE_TYPES_HOMEWORK_2026_10_02.md, section 3). */
    @Override
    protected boolean readsStored(com.legend.sql.SqlDdl.ColumnType t) {
        return readsAsText(t) || jsonbRead(t) != JsonbRead.NONE;
    }

    @Override
    protected SqlWriter storedRead(SqlWriter writer, com.legend.sql.SqlExpr.StoredRead r) {
        return switch (jsonbRead(r.stored())) {
            case CAST -> writer.append("CAST(").expr(r.column(), 0).append(" AS JSONB)");
            case CONVERT -> writer.append("to_jsonb(").expr(r.column(), 0).append(")");
            case NONE -> super.storedRead(writer, r);
        };
    }

    /** How a stored type reads as {@code jsonb}: {@code json} is cast; an array or a composite
     *  cannot be cast, and is converted ({@code to_jsonb}); any other type is not nested. */
    private enum JsonbRead { CAST, CONVERT, NONE }

    private static JsonbRead jsonbRead(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case JSON -> JsonbRead.CAST;
                case ARRAY, OBJECT -> JsonbRead.CONVERT;
                case BIGINT, SMALLINT, TINYINT, INTEGER, FLOAT, DOUBLE, REAL, BIT, TIMESTAMP, DATE, TIME,
                        TIMESTAMP_NS, VARCHAR, OTHER, DISTINCT -> JsonbRead.NONE;
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized ignored -> JsonbRead.NONE;
            case com.legend.sql.SqlDdl.ColumnType.Scaled ignored -> JsonbRead.NONE;
        };
    }

    /** Alias-less projections label EXPLICITLY from the declared output: Postgres
     * labels an expression {@code ?column?}, and the product wrapper selects
     * columns by name. */
    @Override
    protected @com.legend.base.Nullable String implicitLabel(com.legend.sql.SqlSelect.Projection p) {
        return p.out() != null ? aliasIdent(p.out().name()) : super.implicitLabel(p);
    }

    /** A NULL projected under a LIST slot is typed as the list (a bare NULL is text to Postgres, and
     *  unnest(text) does not exist); the base types the scalar slots. */
    @Override
    protected SqlWriter projection(SqlWriter writer, com.legend.sql.SqlSelect.Projection p) {
        if (p.expr() instanceof SqlExpr.NullLit && p.out() != null && p.out().type() instanceof SqlType.Array arr) {
            writer.append("CAST(NULL AS " + carrier(arr.element()) + "[])");
            String label = p.alias() != null ? aliasIdent(p.alias()) : implicitLabel(p);
            return label == null ? writer : writer.append(" AS ").append(label);
        }
        // error() projected under a slot is typed as the slot, as a CASE branch that raises is: text there met the
        // slot's readers (to_char of a Date slot: "function to_char(character varying, unknown) does not exist",
        // the planner refusing before the raise could; step 9, 2026-10-19)
        if (p.out() != null && p.expr() instanceof SqlExpr.Call c && c.fn() == SqlFn.ERROR) {
            writer.piece(raise(c.args(), p.out().type()));
            String label = p.alias() != null ? aliasIdent(p.alias()) : implicitLabel(p);
            return label == null ? writer : writer.append(" AS ").append(label);
        }
        // the empty collection in a scalar slot is written as an untyped NULL, which Postgres types as text: the
        // slot's own type, so the column is what the plan declares; a struct slot's is its carrier, jsonb (a field
        // read off a text NULL was "operator does not exist: text -> unknown", zip of empty lists; step 9, 2026-10-19)
        if (p.out() != null && p.expr().type() instanceof com.legend.sql.TypeFact.Bottom
                && (p.out().type() instanceof SqlType.Scalar || p.out().type() instanceof SqlType.Decimal
                    || p.out().type() instanceof SqlType.Struct)) {
            writer.append("CAST(").expr(p.expr(), 0).append(" AS " + (p.out().type() instanceof SqlType.Struct
                    ? carrier(p.out().type()) : castTypeName(p.out().type())) + ")");
            String label = p.alias() != null ? aliasIdent(p.alias()) : implicitLabel(p);
            return label == null ? writer : writer.append(" AS ").append(label);
        }
        return super.projection(writer, p);
    }

    /** A name the query itself writes (an alias, a derived label) quoted, so it keeps the case it is written in
     * (Postgres folds a bare one to lower case); a quote-bearing declaration ({@code "date"}) is already its own
     * spelling. A STORE's name is spelled by {@link #physicalName}. */
    @Override
    protected String ident(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")
                && !name.substring(1, name.length() - 1).replace("\"\"", "").contains("\"")) {
            return name;
        }
        return delimited(name);
    }

    /**
     * A STORE's name as Postgres folds it (decision D8, the user's, 2026-10-13: fold, as legend-engine does). A name a
     * store declares bare is case-insensitive -- the engine's convention, H2's -- and Postgres folds a bare name to
     * lower case, so it is spelled in lower case: bare where it may be, delimited where it must be (a keyword). It then
     * names the table this dialect's own DDL made, the table a bare {@code CREATE TABLE} made (the engine's unquoted
     * SQL, the corpus's setup text), and a catalog's lower-case name. A name the store declared quoted
     * ({@code "firstName"}) keeps its case, as does a catalog name that is not its own fold: CatalogModel declares it
     * quoted ({@link #bareNameReaches}). Until 2026-10-14 every name was delimited as declared, which reached only a
     * table made with that exact case.
     */
    @Override
    public String physicalName(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")
                && !name.substring(1, name.length() - 1).replace("\"\"", "").contains("\"")) {
            return name;
        }
        String folded = name.toLowerCase(java.util.Locale.ROOT);
        return BARE.matcher(folded).matches() && !reservedWords().contains(folded) ? folded : delimited(folded);
    }

    private static final java.util.regex.Pattern BARE = java.util.regex.Pattern.compile("[a-z_][a-z0-9_]*");

    /** A catalog name a bare declaration reaches: one that is its own fold (a table made bare, or quoted in lower
     *  case); {@code "Orders"} is not, and is declared quoted. */
    @Override
    public boolean bareNameReaches(String catalogName) {
        return catalogName.equals(catalogName.toLowerCase(java.util.Locale.ROOT));
    }

    /** A DDL column the store declared bare is a {@link #physicalName} (the queries' spelling of it). */
    @Override
    protected String ddlIdentifier(String name, boolean declaredQuoted) {
        return declaredQuoted ? super.ddlIdentifier(name, true) : physicalName(name);
    }

    /** A stored table, each part a {@link #storedName}: folded where the store declared it bare, its case kept where
     *  it declared it quoted. */
    @Override
    protected String tableName(String name, com.legend.sql.NameQuoting quoting) {
        int dot = name.indexOf('.');
        return dot <= 0 ? storedName(name, quoting.table())
                : storedName(name.substring(0, dot), quoting.schema()) + "."
                        + storedName(name.substring(dot + 1), quoting.table());
    }

    /** A stored column the store declared bare is a {@link #physicalName}; a derived one (the query's own label) and one
     *  declared quoted keep their spelling. A stored column read with no declared output is labelled as it is
     *  referenced (the base's implicit label), so an outer query's reference to it names the same column. */
    @Override
    protected String columnName(SqlExpr.Column c) {
        return c.origin() == com.legend.sql.OutputCol.Origin.PHYSICAL ? physicalName(c.name()) : super.columnName(c);
    }

    @Override
    protected String rowOrderColumn() {
        throw new DialectCapability("row order reached Postgres, which has no stable"
                + " row-order column (ctid moves on update)");
    }

    @Override
    protected String starExceptKeyword() {
        throw new DialectCapability("SELECT * EXCLUDE reached Postgres, which has no"
                + " star exclusion; the column list must be expanded upstream");
    }

    @Override
    protected SqlWriter expr(SqlWriter writer, SqlExpr e, int parentPrec) {
        if (e instanceof SqlExpr.ArrayLit a) {
            return arrayLiteral(writer, a);
        }
        if (e instanceof SqlExpr.OrderedListAgg ola) {
            // DuckDB's list(value ORDER BY key), as the LIST aggregate: NULL over no row, a list value held as jsonb
            SqlExpr v = ola.value();
            return writer.append("array_agg(").piece(encode(w -> w.expr(v, 0), typeOf(v)))
                    .append(" ORDER BY ").expr(ola.orderBy(), 0).append(")");
        }
        return super.expr(writer, e, parentPrec);
    }

    /** A date-time finer than a microsecond is a {@code TIMESTAMP} of its first six digits, the finer ones cut, as its
     *  literal is ({@link #timestampLit}). */
    @Override
    protected RenderedStatement.TypeSpelling holeType(com.legend.sql.ValueKind kind) {
        return kind == com.legend.sql.ValueKind.DATE_TIME_NANOS
                ? new RenderedStatement.TypeSpelling("TIMESTAMP", RenderedStatement.Digits.NONE, 6)
                : super.holeType(kind);
    }

    /** Postgres keeps microseconds and ROUNDS finer digits (59.9999999 becomes the next
     * minute; the 9999-12-31 23:59:59.999999999 sentinel would become year 10000):
     * finer digits are TRUNCATED, as DuckDB's TIMESTAMP does. */
    @Override
    protected String timestampLit(String iso) {
        int dot = iso.lastIndexOf('.');
        if (dot >= 0) {
            int end = dot + 1;
            while (end < iso.length() && Character.isDigit(iso.charAt(end))) {
                end++;
            }
            if (end - dot - 1 > 6) {
                iso = iso.substring(0, dot + 7) + iso.substring(end);
            }
        }
        return super.timestampLit(iso);
    }

    /** Postgres text cannot hold NUL ({@code chr(0)} raises). */
    @Override
    protected String stringLit(String value) {
        if (value.indexOf(0) >= 0) {   // NUL, by code point
            throw new DialectCapability("a string holding NUL reached Postgres, whose text"
                    + " type cannot hold it");
        }
        return super.stringLit(value);
    }

    /** Composite casts other than DECIMAL are the collection carrier's (leg P4). */
    @Override
    protected SqlWriter variantAwareCast(SqlWriter writer, SqlExpr.Cast c) {
        if (c.target() instanceof SqlType.Array arr) {
            return listCast(writer, c, arr);
        }
        // a Variant as a map: its members as entries, in jsonb's own key order. NULL for SQL's NULL or jsonb's null;
        // any other value that is not an object is jsonb_each's error, as DuckDB's cast to a MAP raises one (it was
        // NULL here: 2026-10-12, the #SQL{}# line)
        if (c.target() instanceof SqlType.Map && isJson(c.value())) {
            return writer.append("(CASE WHEN ").expr(c.value(), 0).append(" IS NULL OR jsonb_typeof(")
                    .expr(c.value(), 0).append(") = 'null' THEN NULL ELSE ARRAY(SELECT jsonb_build_object('key',"
                    + " to_jsonb(__mk), 'value', __mv) FROM jsonb_each(").expr(c.value(), 0)
                    .append(") WITH ORDINALITY AS __mj(__mk, __mv, __mo) ORDER BY __mo) END)");
        }
        // a map to another map's key and value types: its entries are jsonb, so it is itself (a NULL or the empty map
        // typed as the carrier)
        if (c.target() instanceof SqlType.Map) {
            if (c.value() instanceof SqlExpr.NullLit) {
                return writer.append("CAST(NULL AS JSONB[])");
            }
            if (!(typeOf(c.value()) instanceof SqlType.Map) && !(c.value() instanceof SqlExpr.Call call
                    && (call.fn() == SqlFn.MAP_EMPTY || call.fn() == SqlFn.MAP_FROM_LISTS
                            || call.fn() == SqlFn.MAP_FROM_ENTRIES || call.fn() == SqlFn.MAP_CONCAT))) {
                throw new DialectCapability("a cast of " + c.value().type() + " to " + c.target() + " reached Postgres");
            }
            return writer.expr(c.value(), 0);
        }
        // A VARIANT is jsonb: text parses into it; any other value converts (no cast from a number
        // or a boolean to jsonb exists)
        if (c.target() == SqlType.Scalar.JSON) {
            // (a NULL typed as jsonb: to_jsonb(NULL) has no type to go on, and jsonb's operators over an untyped
            // NULL are ambiguous: 2026-10-17)
            if (c.value() instanceof SqlExpr.NullLit) {
                return writer.append("CAST(NULL AS JSONB)");
            }
            return isText(c.value()) ? writer.append("CAST(").expr(c.value(), 0).append(" AS JSONB)")
                    : writer.append("to_jsonb(").expr(c.value(), 0).append(")");
        }
        // a value out of a variant: its TEXT (->> strips JSON quoting; #>> '{}' for the whole value),
        // then the cast -- the swap lives in rendering, as DuckDB's
        if (c.target() != SqlType.Scalar.TEMPORAL_TEXT && c.target() != SqlType.Scalar.DECIMAL_TEXT) {
            if (c.value() instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET) {
                boolean root = call.args().get(1) instanceof SqlExpr.StringLit k && "$".equals(k.value());
                SqlExpr key = call.args().get(1);
                writer.append("CAST((").expr(call.args().get(0), 8);
                if (root) {
                    writer.append(" #>> '{}'");
                } else if (!(key instanceof SqlExpr.IntLit) && isNumeric(key)) {
                    // (a computed index, BIGINT: ->> takes an int or a text, never a bigint, as -> in variantGet;
                    // 2026-10-17, the Cypher island's path text reading its relationships' directions by place)
                    writer.append(" ->> CAST(").expr(key, 0).append(" AS INTEGER)");
                } else {
                    writer.append(" ->> ").expr(key, 8);
                }
                return writer.append(") AS ").append(castTypeName(c.target())).append(")");
            }
            // a whole value to a scalar reads its text; to TEXT it is its JSON text, as DuckDB's cast
            if (isJson(c.value()) && c.target() != SqlType.Scalar.VARCHAR) {
                return writer.append("CAST((").expr(c.value(), 8).append(" #>> '{}') AS ")
                        .append(castTypeName(c.target()))
                        .append(")");
            }
            if (isJson(c.value())) {
                return writer.piece(compactJson(w -> w.append("CAST(").expr(c.value(), 0).append(" AS VARCHAR)")));
            }
        }
        // DuckDB's Float text keeps an integral value's '.0' (41.0, where Postgres writes 41: measured 2026-10-11 by
        // channel B on Postgres, an average's TDS cell printed in the query), as Snowflake's
        if (c.target() == SqlType.Scalar.VARCHAR && (isDouble(c.value()) || c.value() instanceof SqlExpr.FloatLit)) {
            writer.append("regexp_replace(");
            super.variantAwareCast(writer, c);
            return writer.append(", '^(-?[0-9]+)$', '\\1.0')");
        }
        if (c.value() instanceof SqlExpr.StringLit && c.target() != SqlType.Scalar.VARCHAR
                && c.target() != SqlType.Scalar.TEMPORAL_TEXT && c.target() != SqlType.Scalar.DECIMAL_TEXT) {
            return super.variantAwareCast(writer, new SqlExpr.Cast(typedText(c.value()), c.target()));
        }
        return super.variantAwareCast(writer, c);
    }

    /**
     * A text literal under a cast, spelled as a text first: Postgres reads a bare literal under a cast as a
     * constant of the cast's type WHEN IT PARSES the statement, before any CASE around it is weighed, so
     * {@code CASE WHEN 'x' ~ '[0-9]+' THEN CAST('x' AS BIGINT) END} is the parse's error, not NULL (measured
     * 2026-10-16 on Postgres 16, the Cypher island's toInteger of a text, docs/CYPHER_STUDY_2026_10_15.md §12.23).
     * A typed text's cast is the planner's, which drops a CASE arm whose test is false before it folds the arm.
     * Any other value is itself.
     */
    private static SqlExpr typedText(SqlExpr x) {
        return x instanceof SqlExpr.StringLit ? new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR) : x;
    }

    /** A value to a list of scalars: a Variant array's elements read as the element type, in order (a
     *  JSON null or a non-array is a NULL list, as DuckDB's cast); a list of another element type cast
     *  element-wise. */
    private SqlWriter listCast(SqlWriter writer, SqlExpr.Cast c, SqlType.Array target) {
        if (emptyCollection(c.value())) {
            return writer.append("CAST(NULL AS " + carrier(target.element()) + "[])");
        }
        // the empty list literal typed: a list of no elements, never NULL (an equality canon's empty key collection)
        if (c.value() instanceof SqlExpr.ArrayLit a && a.elements().isEmpty()) {
            return writer.append("CAST(ARRAY[] AS " + carrier(target.element()) + "[])");
        }
        if (isJson(c.value())) {
            // a Variant array: its elements held as the list holds them
            return writer.piece(decode(w -> w.expr(c.value(), 0), target, 0));
        }
        SqlType from = listElement(c.value());
        if (from != null && heldAsItself(from) && heldAsItself(target.element())) {
            return writer.append("CAST(").expr(c.value(), 0).append(" AS " + carrier(target.element()) + "[])");
        }
        if (from != null && !heldAsItself(from) && !heldAsItself(target.element())) {
            return writer.expr(c.value(), 0);   // both held as jsonb
        }
        throw new DialectCapability("a cast of " + c.value().type() + " to " + target + " reached Postgres");
    }

    /** jsonb's text, compact as Pure and DuckDB print JSON: jsonb puts a space after every ',' and ':'
     *  outside a string, and only there -- so a string-aware replace drops exactly those (a string, with
     *  its escapes, is matched whole and kept). Keys stay in jsonb's own order. */
    private static SqlWriter.Piece compactJson(SqlWriter.Piece text) {
        return w -> w.append("regexp_replace(").piece(text)
                .append(", '(\"(?:[^\"\\\\]|\\\\.)*\")|([,:]) ', '\\1\\2', 'g')");
    }

    /** VARIANT navigation over jsonb: a key or a 0-based index (-1 the last, past the end NULL --
     *  DuckDB's JSON arrow agrees). */
    @Override
    protected SqlWriter variantGet(SqlWriter writer, List<SqlExpr> args) {
        // the JSON path '$' (the lowering's root read) is the value itself; any other key is a member
        if (args.get(1) instanceof SqlExpr.StringLit root && "$".equals(root.value())) {
            return writer.expr(args.get(0), 0);
        }
        if (!(args.get(1) instanceof SqlExpr.IntLit) && isNumeric(args.get(1))) {
            // a computed index (an integer the row holds, which the island types as BIGINT): Postgres's -> takes an
            // int or a text, never a bigint; a key the row holds is text and passes as is (2026-10-17, the Cypher
            // island's W11: a spelled list indexed by a value of the row)
            return writer.append("(").expr(args.get(0), 8).append(" -> CAST(").expr(args.get(1), 0)
                    .append(" AS INTEGER))");
        }
        return writer.append("(").expr(args.get(0), 8).append(" -> ").expr(args.get(1), 8).append(")");
    }

    @Override
    protected SqlWriter variantConstruct(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() != 1) {
            throw new DialectCapability("toVariant of " + a.size() + " arguments reached Postgres");
        }
        if (a.get(0) instanceof SqlExpr.StringLit) {
            // a quoted literal is of no type yet, which to_jsonb refuses (asJsonb)
            return writer.piece(asJsonb(w -> w.expr(a.get(0), 0), SqlType.Scalar.VARCHAR));
        }
        return writer.append("to_jsonb(").expr(a.get(0), 0).append(")");
    }

    // ==================================================================
    // Scalar functions — the named arms of call, every SqlFn's table
    // ==================================================================

    /** The {@code MOD}, {@code REM} arm of {@link #call}. */
    private SqlWriter modArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (a.stream().anyMatch(Postgres::isDouble)) {
            if (c.fn() == SqlFn.MOD) {
                throw new DialectCapability("MOD over a Float reached Postgres, which has no double-precision"
                        + " floored remainder");
            }
            return exactRemainder(writer, a);
        }
        return c.fn() == SqlFn.MOD ? modFloored(writer, a) : modTruncated(writer, a);
    }

    /**
     * Pure's {@code rem} of two Floats, exact as Java's {@code %} (C's {@code fmod}): Postgres has no remainder of
     * doubles, and its double-to-numeric cast is the shortest decimal, not the double's value. Each operand is
     * written as an integer times a power of two (the power from its logarithm, kept 60 bits above the mantissa so
     * an estimate off by one still leaves an integer: the scaling by powers of two and the cast to BIGINT are exact),
     * both are scaled to the one power, the numeric remainder of the two integers is taken (exact), and the result
     * scaled back, a double exactly (the numeric quotient to a thousand places, far below the least double's unit).
     * Null where either operand is (the arms after test values, and an arm true of the one would answer for a null
     * other: 2026-10-18, the independent review). The sign is the dividend's (a zero too); NaN for a NaN, an
     * infinite dividend or a zero divisor; the dividend for an infinite divisor, a zero dividend or one smaller than
     * the divisor. Each operand is written where it is read (about a hundred times; a remainder of a remainder its
     * square): a scalar subquery reading it once cannot stand in a grouped select whose key is an expression, since
     * Postgres matches only a grouped column inside a subquery ("subquery uses ungrouped column", measured
     * 2026-10-18 over the Cypher island's {@code WITH a * 1.5 AS g, count(*) AS c RETURN g % 2.0}). An operand that
     * draws afresh at each reading (random(), a uuid) is refused by name, since it would draw again.
     */
    private SqlWriter exactRemainder(SqlWriter writer, List<SqlExpr> a) {
        if (a.stream().anyMatch(Postgres::drawsAfresh)) {
            throw new DialectCapability("REM over a Float of random() or a uuid reached Postgres: its exact remainder"
                    + " writes each operand where it reads it, which would draw it again");
        }
        SqlWriter.Piece x = w -> w.append("CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION)");
        SqlWriter.Piece y = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS DOUBLE PRECISION)");
        SqlWriter.Piece kx = exponent(x);
        SqlWriter.Piece ky = exponent(y);
        // (the common power never below zero: a larger one scales the integers up, exact in NUMERIC, and the result
        // is then one division, written once)
        SqlWriter.Piece k = w -> w.append("greatest(").piece(kx).append(", ").piece(ky).append(", 0)");
        SqlWriter.Piece scaledX = scaled(mantissa(x, kx), k, kx);
        SqlWriter.Piece scaledY = scaled(mantissa(y, ky), k, ky);
        SqlWriter.Piece r = w -> w.append("CAST(round(mod(").piece(scaledX).append(", ").piece(scaledY)
                .append("), 1000) / power(CAST(2 AS NUMERIC), ").piece(k).append(") AS DOUBLE PRECISION)");
        return writer.append("(CASE WHEN ").piece(x).append(" IS NULL OR ").piece(y).append(" IS NULL THEN CAST(NULL"
                + " AS DOUBLE PRECISION) WHEN ").piece(x).append(" = 'NaN' OR ").piece(y).append(" = 'NaN' OR ")
                .piece(y).append(" = 0 OR abs(").piece(x).append(") = 'Infinity' THEN CAST('NaN' AS DOUBLE PRECISION)"
                + " WHEN abs(").piece(y).append(") = 'Infinity' OR ").piece(x).append(" = 0 OR abs(").piece(x)
                .append(") < abs(").piece(y).append(") THEN ").piece(x).append(" ELSE CASE WHEN ").piece(x)
                .append(" < 0 THEN CAST(-1 AS DOUBLE PRECISION) ELSE CAST(1 AS DOUBLE PRECISION) END * ").piece(r)
                .append(" END)");
    }

    /** The power of two {@link #exactRemainder} scales a finite non-zero double {@code v} by: 60 less its
     *  logarithm's floor (null for a zero, an infinity or a NaN, which take no part in it). */
    private static SqlWriter.Piece exponent(SqlWriter.Piece v) {
        return w -> w.append("CASE WHEN abs(").piece(v).append(") > 0 AND abs(").piece(v).append(") < 'Infinity' THEN"
                + " 60 - CAST(floor(ln(abs(").piece(v).append(")) / ln(2)) AS INTEGER) END");
    }

    /** {@code |v|} times two to the {@code k} (two steps, so neither overflows), an integer, as a NUMERIC. */
    private static SqlWriter.Piece mantissa(SqlWriter.Piece v, SqlWriter.Piece k) {
        return w -> w.append("CAST(CAST(abs(").piece(v).append(") * power(CAST(2 AS DOUBLE PRECISION), div(").piece(k)
                .append(", 2)) * power(CAST(2 AS DOUBLE PRECISION), ").piece(k).append(" - div(").piece(k)
                .append(", 2)) AS BIGINT) AS NUMERIC)");
    }

    /** An operand's integer {@code m} scaled from its own power {@code kv} to the common one {@code k}, at least
     *  its own (exact: a numeric power of two of a non-negative exponent). */
    private static SqlWriter.Piece scaled(SqlWriter.Piece m, SqlWriter.Piece k, SqlWriter.Piece kv) {
        return w -> w.piece(m).append(" * power(CAST(2 AS NUMERIC), ").piece(k).append(" - ").piece(kv).append(")");
    }

    /** Whether {@code e} holds a call that draws afresh at each reading (the database's random, a uuid). */
    private static boolean drawsAfresh(SqlExpr e) {
        if (e instanceof SqlExpr.Call c && (c.fn() == SqlFn.RANDOM || c.fn() == SqlFn.GUID)) {
            return true;
        }
        for (SqlExpr k : e.children()) {
            if (drawsAfresh(k)) {
                return true;
            }
        }
        return false;
    }

    /** The {@code ACOS}, {@code ASIN} arm of {@link #call}. */
    private SqlWriter acos(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String f = c.fn() == SqlFn.ACOS ? "acos" : "asin";
        return writer.append("(CASE WHEN (").expr(a.get(0), 0).append(") BETWEEN -1 AND 1 THEN ").append(f)
                .append("(").expr(a.get(0), 0).append(") ELSE CAST('NaN' AS DOUBLE PRECISION) END)");
    }

    /** The {@code REGEXP_EXTRACT} arm of {@link #call}. */
    private SqlWriter regexpExtract(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        writer.append("coalesce(regexp_substr(").expr(a.get(0), 0).append(", ")
                .piece(pattern(a.get(1), "", "")).append(", 1, 1, '', ");
        return (a.size() > 2 ? writer.expr(a.get(2), 0) : writer.append("0")).append("), '')");
    }

    /** The {@code REGEXP_REPLACE} arm of {@link #call}. */
    private SqlWriter regexpReplace(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        writer.append("regexp_replace(").expr(a.get(0), 0).append(", ").piece(pattern(a.get(1), "", ""));
        for (SqlExpr x : a.subList(2, a.size())) {
            writer.append(", ").expr(x, 0);
        }
        return writer.append(")");
    }

    /** The {@code STRFTIME} arm of {@link #call}. */
    private SqlWriter strftime(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        if (!(a.get(1) instanceof SqlExpr.FormatLit)) {
            throw new DialectCapability("a date format that is not a format literal"
                    + " reached Postgres: its codes are DuckDB's");
        }
        return writer.append("to_char(").piece(naive(a.get(0))).append(", ").expr(a.get(1), 0).append(")");
    }

    /** The {@code EXTRACT} arm of {@link #call}. */
    private SqlWriter extractArm(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        String part = datePart(part(a.get(0)));
        if (!List.of("year", "quarter", "month", "week", "day", "doy", "dow",
                "isodow", "hour", "minute", "second").contains(part)) {
            throw new DialectCapability("date part '" + part
                    + "' has no probed Postgres spelling");
        }
        return writer.append("CAST(floor(extract(").append(part).append(" FROM ").expr(a.get(1), 0)
                .append(")) AS BIGINT)");
    }

    /** The {@code JSON_TYPE} arm of {@link #call}. */
    private SqlWriter jsonType(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        return writer.append("(CASE jsonb_typeof(").expr(a.get(0), 0)
                .append(") WHEN 'null' THEN 'NULL' WHEN 'string' THEN 'VARCHAR'")
                .append(" WHEN 'boolean' THEN 'BOOLEAN' WHEN 'array' THEN 'ARRAY' WHEN 'object' THEN 'OBJECT'")
                .append(" WHEN 'number' THEN CASE WHEN (").expr(a.get(0), 0)
                .append(" #>> '{}') ~ '^-?[0-9]+$' THEN 'BIGINT'").append(" ELSE 'DOUBLE' END END)");
    }

    /** The {@code REGEXP_EXTRACT_ALL} arm of {@link #call}. */
    private SqlWriter regexpExtractAll(SqlWriter writer, SqlExpr.Call c) {
        List<SqlExpr> a = c.args();
        SqlWriter.Piece pat = pattern(a.get(1), "", "");
        writer.append("ARRAY(SELECT regexp_substr(").expr(a.get(0), 0).append(", ").piece(pat)
                .append(", 1, __n, '', ");
        return (a.size() > 2 ? writer.expr(a.get(2), 0) : writer.append("0"))
                .append(") FROM generate_series(1, regexp_count(").expr(a.get(0), 0).append(", ").piece(pat)
                .append(")) AS __g(__n) ORDER BY __n)");
    }

    /**
     * error(msg[, 'line:col']) without a UDF (the route H2_BACKEND.md bans) and without
     * a raise function (Postgres has none in SQL): the sentinel-wrapped message CAST to
     * TIMESTAMPTZ, which cannot parse it ({@code invalid input syntax for type timestamp
     * with time zone: "␟msg␟"} — RaisedErrors reads between the U+001F sentinels, the
     * B7 envelope). TIMESTAMPTZ input is STABLE, so the planner never folds the raise
     * of a constant message (an immutable cast would raise at PLAN time inside a CASE
     * arm never taken); the CASE stays lazy per row. Then text, then the slot's own
     * type where a CASE branch IS the raise, so the branches unify. Probed on 17:
     * constant-false and row guards do not fire, a taken guard raises the message.
     * But a STABLE expression IS evaluated at plan time where the planner estimates (an
     * argument of unnest, whose row estimate pre-evaluates it: an untaken raise under
     * unnest(CASE ...) fired, measured on 16.15, 2026-10-12), so a raise whose value is a
     * list or a map, the only kind unnest takes, carries an empty VOLATILE term
     * ({@code left(random()::text, 0)}), which no estimate evaluates (and every unnest now
     * holds its argument behind a scalar subquery, {@link #unnest}: the estimate reaches a
     * column's raise through the planner's pull-up, which no term at the raise's own site
     * could carry, 2026-10-17). Only that kind: a
     * volatile term keeps the planner from flattening a subquery and from aggregating in
     * parallel, and every Integer division's zero guard is a raise (PR #34's review,
     * 2026-10-13, measured on 16.15: a join on a subquery's id went from an index nested
     * loop to a hash join over a full scan).
     */
    private SqlWriter.Piece raise(List<SqlExpr> a, @com.legend.base.Nullable SqlType slot) {
        return raise(a, slot, slot instanceof SqlType.Array || slot instanceof SqlType.Map);
    }

    private SqlWriter.Piece raise(List<SqlExpr> a, @com.legend.base.Nullable SqlType slot, boolean unnestable) {
        SqlWriter.Piece text = w -> {
            w.append("CAST(CAST(chr(31) || ");
            if (a.size() > 1) {
                w.expr(a.get(1), 0).append(" || chr(30) || ");
            }
            w.append("(").expr(a.get(0), 0).append(") || chr(31)")
                    .append(unnestable || w.inPlanned() ? " || left(CAST(random() AS VARCHAR), 0)" : "")
                    .append(" AS TIMESTAMPTZ) AS VARCHAR)");
        };
        // typed as its slot (never evaluated: the TIMESTAMPTZ cast raises first) -- a list of scalars too
        boolean typed = slot instanceof SqlType.Scalar || slot instanceof SqlType.Decimal
                || slot instanceof SqlType.Array arr && (arr.element() instanceof SqlType.Scalar
                        || arr.element() instanceof SqlType.Decimal);
        return typed && slot != null
                ? w -> w.append("CAST(").piece(text).append(" AS ").append(castTypeName(slot)).append(")") : text;
    }

    /** The base CASE, with a branch that IS error() raised in the CASE's own type. */
    @Override
    protected SqlWriter caseExpr(SqlWriter writer, SqlExpr.Case c) {
        SqlType slot = c.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
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

    private static DialectCapability wall(SqlFn fn, String why) {
        return new DialectCapability(fn + " reached Postgres: " + why);
    }

    // ---- format

    /** A printf directive: {@code %[0][width][.precision](s|d|f)}, or {@code %%}. */
    private static final java.util.regex.Pattern DIRECTIVE =
            java.util.regex.Pattern.compile("%(%|(0?)([0-9]*)(?:\\.([0-9]+))?([sdf]))");

    /**
     * DuckDB's printf over a LITERAL format, unrolled (Postgres' {@code format()} has {@code %s} and {@code %I}/{@code
     * %L} only): its text and each directive's argument concatenated -- {@code %s} the argument's text (this
     * dialect's cast to text: a Float keeps its {@code .0}), {@code %d} an integer's digits (padded to a width,
     * {@code 0} with zeros after the sign), {@code %.Nf} a number at N decimals (6 by default), rounded half away
     * from zero as printf's ({@code round(numeric, N)}, measured), a negative value that rounds to zero keeping its
     * sign. The lowering spells %t and %r as %s already. Any other directive is refused by name.
     */
    private SqlWriter.Piece format(List<SqlExpr> a) {
        if (!(a.get(0) instanceof SqlExpr.StringLit f)) {
            throw new DialectCapability("a computed format reached Postgres, which has no printf: only a literal one"
                    + " is unrolled");
        }
        if (DIRECTIVE.matcher(f.value()).replaceAll("").contains("%")) {
            throw new DialectCapability("a format directive other than %s, %d and %f reached Postgres: " + f.value());
        }
        List<SqlWriter.Piece> parts = new java.util.ArrayList<>();
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
            if (text.length() > 0) {
                String t = stringLit(text.toString());
                parts.add(w -> w.append(t));
                text.setLength(0);
            }
            if (arg >= a.size()) {
                throw new DialectCapability("a format with more directives than arguments reached Postgres");
            }
            SqlExpr x = a.get(arg++);
            boolean zero = !m.group(2).isEmpty();
            int width = m.group(3).isEmpty() ? 0 : Integer.parseInt(m.group(3));
            char conversion = m.group(5).charAt(0);
            SqlExpr asText = new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR);
            SqlWriter.Piece value = switch (conversion) {
                case 's', 'd' -> isText(x) ? w -> w.expr(x, 0) : w -> w.expr(asText, 0);
                case 'f' -> {
                    int precision = m.group(4) == null ? 6 : Integer.parseInt(m.group(4));
                    // Pure rounds the value's own decimal text half-EVEN (FormatTools, RoundingMode.HALF_EVEN): the
                    // text is the shortest one that reads back (a float8's text; its cast to numeric keeps 15 digits),
                    // and numeric rounds half away from zero, so an exact half of the scaled value steps to its even
                    // neighbour (PR #34's review, 2026-10-13: %.2f of 1.125 is 1.12, %.0f of 2.5 is 2)
                    String scale = java.math.BigDecimal.ONE.movePointRight(precision).toPlainString();
                    SqlWriter.Piece rounded = w -> w.append("(SELECT round(CASE WHEN __hv - floor(__hv) = 0.5 THEN (CASE"
                            + " WHEN mod(floor(__hv), 2) = 0 THEN floor(__hv) ELSE floor(__hv) + 1 END) ELSE round(__hv)"
                            + " END / " + scale + ", " + precision + ") FROM (SELECT CAST(CAST(").expr(x, 0)
                            .append(" AS TEXT) AS NUMERIC) * " + scale + " AS __hv) AS __he)");
                    yield w -> w.append("(CASE WHEN ").expr(x, 0).append(" < 0 AND ").piece(rounded)
                            .append(" = 0 THEN '-' ELSE '' END || CAST(").piece(rounded).append(" AS VARCHAR))");
                }
                default -> throw new IllegalStateException("the directive pattern admits s, d, f: " + conversion);
            };
            if (width > 0 && conversion == 'd' && zero) {
                SqlWriter.Piece digits = w -> w.append("CAST(abs(").expr(x, 0).append(") AS VARCHAR)");
                parts.add(w -> w.append("(CASE WHEN ").expr(x, 0).append(" < 0 THEN '-' || ").piece(padded(digits,
                        width - 1, '0')).append(" ELSE ").piece(padded(digits, width, '0')).append(" END)"));
            } else if (width > 0) {
                parts.add(padded(value, width, ' '));
            } else {
                parts.add(value);
            }
        }
        if (arg != a.size()) {
            throw new DialectCapability("a format with more arguments than directives reached Postgres");
        }
        text.append(f.value().substring(at));
        if (text.length() > 0 || parts.isEmpty()) {
            String t = stringLit(text.toString());
            parts.add(w -> w.append(t));
        }
        return w -> w.append("(").join(parts, " || ", (v, part) -> v.piece(part)).append(")");
    }

    /** {@code v} padded on the left to {@code width} with {@code fill}, never cut: Postgres' lpad truncates a longer
     *  text to the width (measured), printf never does. */
    private static SqlWriter.Piece padded(SqlWriter.Piece v, int width, char fill) {
        return w -> w.append("lpad(").piece(v).append(", greatest(" + width + ", length(").piece(v)
                .append(")), '" + fill + "')");
    }

    // ---- string distances (fuzzystrmatch has a Levenshtein of at most 255 characters, no extension a Jaro-Winkler)

    /** The Levenshtein distance of two texts, by characters (SQLite's walk, with arrays): the edit-distance table a
     *  row at a time, {@code p} the row above, {@code c} the row being built, one cell a step. Equal to DuckDB's on
     *  1,585 ASCII pairs (2026-10-12); on other text DuckDB counts bytes (2 for 'ä€x' and 'a€x', measured), this
     *  characters, as the reference and SQLite, MariaDB and Snowflake do. */
    private static SqlWriter.Piece levenshtein(SqlExpr x, SqlExpr y) {
        String more = "__lv.j < length(__lv.b)";
        String cell = "least(__lv.p[__lv.j + 2] + 1, __lv.c[__lv.j + 1] + 1, __lv.p[__lv.j + 1] + CASE WHEN"
                + " substr(__lv.a, __lv.i, 1) = substr(__lv.b, __lv.j + 1, 1) THEN 0 ELSE 1 END)";
        return w -> w.append("(WITH RECURSIVE __lv(a, b, i, j, p, c) AS (SELECT __s.a, __s.b, 0, 0, CAST(ARRAY[] AS"
                + " BIGINT[]), CAST(ARRAY[0] AS BIGINT[]) FROM (SELECT ").expr(x, 0).append(" AS a, ").expr(y, 0)
                .append(" AS b) AS __s UNION ALL SELECT __lv.a, __lv.b, CASE WHEN " + more + " THEN __lv.i ELSE"
                + " __lv.i + 1 END, CASE WHEN " + more + " THEN __lv.j + 1 ELSE 0 END, CASE WHEN " + more + " THEN"
                + " __lv.p ELSE __lv.c END, CASE WHEN " + more + " THEN __lv.c || CASE WHEN __lv.i = 0 THEN"
                + " CAST(__lv.j + 1 AS BIGINT) ELSE " + cell + " END ELSE ARRAY[CAST(__lv.i + 1 AS BIGINT)] END FROM"
                + " __lv WHERE NOT (__lv.j = length(__lv.b) AND __lv.i = length(__lv.a))) SELECT c[j + 1] FROM __lv"
                + " WHERE i = length(a) AND j = length(b))");
    }

    /**
     * Jaro-Winkler similarity, exactly DuckDB's (the algorithm and the order of operations are spelled out on
     * Snowflake's): each character of {@code t} in turn takes the first unmatched equal character of {@code p} in its
     * window, a recursive walk along {@code t} ({@code f} flags {@code p}'s matched characters, {@code tm} is {@code
     * t}'s matched ones in order); the transpositions are {@code p}'s matched characters unequal to {@code tm}'s in the
     * same place; the score in DOUBLE PRECISION, Jaro's division guarded where no character matched (that scores 0).
     * Bit-equal to DuckDB's on the 1,585 ASCII pairs Snowflake's was measured on (2026-10-12); by characters, where
     * DuckDB's counts bytes (as {@link #levenshtein}).
     */
    private static SqlWriter.Piece jaroWinkler(SqlExpr p, SqlExpr t) {
        String m = "CAST(length(__jw.tm) AS DOUBLE PRECISION)";
        String jaro = "((" + m + " / CAST(length(__jw.p) AS DOUBLE PRECISION) + " + m + " / CAST(length(__jw.t) AS"
                + " DOUBLE PRECISION) + (" + m + " - CAST(__x.n / 2 AS DOUBLE PRECISION)) / NULLIF(" + m + ", 0))"
                + " / CAST(3 AS DOUBLE PRECISION))";
        List<String> prefix = new java.util.ArrayList<>();
        for (int k = 1; k <= 4; k++) {
            prefix.add("CASE WHEN " + k + " <= least(length(__jw.p), length(__jw.t)) AND left(__jw.p, " + k
                    + ") = left(__jw.t, " + k + ") THEN 1 ELSE 0 END");
        }
        String at = "__jw.j + 1";
        return w -> w.append("(WITH RECURSIVE __jw(p, t, b, j, f, tm) AS (SELECT __s.p, __s.t, greatest(0,"
                + " greatest(length(__s.p), length(__s.t)) / 2 - 1), 0, repeat('0', length(__s.p)), CAST('' AS TEXT)"
                + " FROM (SELECT ").expr(p, 0).append(" AS p, ").expr(t, 0).append(" AS t) AS __s UNION ALL SELECT"
                + " __jw.p, __jw.t, __jw.b, " + at + ", CASE WHEN __m.i IS NULL THEN __jw.f ELSE overlay(__jw.f placing"
                + " '1' from __m.i for 1) END, CASE WHEN __m.i IS NULL THEN __jw.tm ELSE __jw.tm || substr(__jw.t, "
                + at + ", 1) END FROM __jw CROSS JOIN LATERAL (SELECT min(__g.i) AS i FROM generate_series(greatest(1, "
                + at + " - __jw.b), " + at + " + __jw.b) AS __g(i) WHERE substr(__jw.f, __g.i, 1) = '0' AND"
                + " substr(__jw.p, __g.i, 1) = substr(__jw.t, " + at + ", 1)) AS __m WHERE __jw.j < length(__jw.t))"
                + " SELECT CASE WHEN length(__jw.tm) = 0 THEN CAST(0 AS DOUBLE PRECISION) WHEN " + jaro + " > CAST(0.7"
                + " AS DOUBLE PRECISION) THEN " + jaro + " + CAST(" + String.join(" + ", prefix) + " AS DOUBLE"
                + " PRECISION) * CAST(0.1 AS DOUBLE PRECISION) * (CAST(1 AS DOUBLE PRECISION) - " + jaro + ") ELSE "
                + jaro + " END FROM __jw CROSS JOIN LATERAL (SELECT count(*) AS n FROM (SELECT substr(__jw.p, __g.i,"
                + " 1) AS ch, row_number() OVER (ORDER BY __g.i) AS k FROM generate_series(1, length(__jw.p)) AS"
                + " __g(i) WHERE substr(__jw.f, __g.i, 1) = '1') AS __o WHERE __o.ch <> substr(__jw.tm, CAST(__o.k AS"
                + " INTEGER), 1)) AS __x WHERE __jw.j = length(__jw.t) AND __jw.p IS NOT NULL)");
    }

    private static boolean isDouble(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t
                && t.type() == SqlType.Scalar.DOUBLE;
    }

    /** A temporal operand as a NAIVE timestamp: a DATE implicitly casts to
     * timestamptz in Postgres' function resolution (date_trunc, to_char, date_bin),
     * which the session zone would then shift; a declared TIMESTAMPTZ stays one. */
    private SqlWriter.Piece naive(SqlExpr e) {
        if (e.type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == SqlType.Scalar.TIMESTAMP || t.type() == SqlType.Scalar.TIMESTAMPTZ)) {
            return w -> w.expr(e, 0);
        }
        return w -> w.append("CAST(").expr(e, 0).append(" AS TIMESTAMP)");
    }

    /** The base's contract: Date-grained parts (year/quarter/month/week) deliver a
     * DATE, finer parts a TIMESTAMP (DuckDb casts 'day' back to one). */
    private SqlWriter.Piece dateTrunc(List<SqlExpr> a) {
        String part = datePart(part(a.get(0)));
        SqlWriter.Piece trunc = w -> w.append("date_trunc(").append(stringLit(part)).append(", ").piece(naive(a.get(1)))
                .append(")");
        if (DATE_GRAINED.contains(part)) {
            return w -> w.append("CAST(").piece(trunc).append(" AS DATE)");
        }
        if (TIME_GRAINED.contains(part)) {
            return trunc;
        }
        // century/millennium/decade: Postgres counts from year 1 (2001), DuckDB from
        // 2000 (POSTGRES_BACKEND.md §4.4)
        throw new DialectCapability("date_trunc part '" + part + "' has no probed Postgres spelling");
    }

    private static final java.util.Set<String> DATE_GRAINED = java.util.Set.of("year", "quarter", "month", "week");
    private static final java.util.Set<String> TIME_GRAINED = java.util.Set.of("day", "hour", "minute", "second");

    /** DuckDB's date_diff counts BOUNDARIES crossed, never elapsed time — and never
     * {@code age()}, wrong on 9 of 15 edge cases (POSTGRES_BACKEND.md §7). Probed
     * against DuckDB 1.5: day over timestamps (23:00 → 01:00 is 1), month
     * (01-31 → 02-01 is 1), year backwards (-1), quarter. */
    private SqlWriter.Piece dateDiff(List<SqlExpr> a) {
        String part = datePart(part(a.get(0)));
        SqlWriter.Piece from = w -> w.expr(a.get(1), 0);
        SqlWriter.Piece to = w -> w.expr(a.get(2), 0);
        SqlWriter.Piece y = w -> w.append("(extract(year FROM ").piece(to).append(") - extract(year FROM ").piece(from)
                .append("))");
        Integer perYear = PARTS_PER_YEAR.get(part);
        String scale = EPOCH_SCALE.get(part);
        SqlWriter.Piece body;
        if (perYear != null) {
            // year/quarter/month: whole years in the unit, plus the in-year part's step
            body = perYear == 1 ? y : w -> w.piece(y).append(" * " + perYear + " + (extract(" + part + " FROM ")
                    .piece(to).append(") - extract(" + part + " FROM ").piece(from).append("))");
        } else if (scale != null) {
            body = epochBoundaries(from, to, scale);
        } else if (DAY_PART.equals(part)) {
            body = w -> w.append("CAST(").piece(to).append(" AS DATE) - CAST(").piece(from).append(" AS DATE)");
        } else {
            // week is DuckDB's plain day count / 7 (Saturday -> Monday is 0), not a
            // boundary count: unprobed beyond that, so it walls
            throw new DialectCapability("date_diff part '" + part + "' has no probed Postgres spelling");
        }
        return w -> w.append("CAST(").piece(body).append(" AS BIGINT)");
    }

    private static final String DAY_PART = "day";
    private static final java.util.Map<String, Integer> PARTS_PER_YEAR =
            java.util.Map.of("year", 1, "quarter", 4, "month", 12);
    private static final java.util.Map<String, String> EPOCH_SCALE = java.util.Map.of(
            "hour", " / 3600", "minute", " / 60", "second", "",
            "millisecond", " * 1000", "microsecond", " * 1000000");

    /** Sub-day parts FLOOR the epoch in their unit, before 1970 too (probed on DuckDB
     * 1.5: 1969-12-31 23:59:59.9995 -> 00:00 is 1 millisecond, 22:30 -> 23:10 in 1969
     * is 1 hour); Postgres' extract(epoch) is exact numeric. */
    private static SqlWriter.Piece epochBoundaries(SqlWriter.Piece from, SqlWriter.Piece to, String scale) {
        return w -> w.append("floor(extract(epoch FROM ").piece(to)
                .append(")" + scale + ") - floor(extract(epoch FROM ")
                .piece(from).append(")" + scale + ")");
    }

    /** A date part as Postgres names it: {@code extract}'s field, {@code date_trunc}'s unit. */
    private static String datePart(com.legend.sql.DatePart p) {
        return switch (p) {
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
            case NANOSECOND -> throw new DialectCapability("a nanosecond part: Postgres keeps microseconds (the"
                    + " Cypher island's nanosecond leg is DuckDB's)");
            case DAY_OF_YEAR -> "doy";
            case ISO_DAY_OF_WEEK -> "isodow";
        };
    }

    /** An interval's unit as Postgres names it ({@code INTERVAL '1 day'}). */
    private static String intervalUnit(com.legend.sql.DatePart unit) {
        return switch (unit) {
            case YEAR -> "year";
            case MONTH -> "month";
            case WEEK -> "week";
            case DAY -> "day";
            case HOUR -> "hour";
            case MINUTE -> "minute";
            case SECOND -> "second";
            case MILLISECOND -> "millisecond";
            case MICROSECOND -> "microsecond";
            case QUARTER, NANOSECOND, DAY_OF_YEAR, ISO_DAY_OF_WEEK -> throw new DialectCapability("interval unit '" + unit
                    + "' has no Postgres spelling");
        };
    }

    /** The calendar units date_bin cannot bin, by their length in months. */
    private static final java.util.Map<String, Integer> CALENDAR_UNITS = java.util.Map.of("month", 1, "year", 12);

    /** date_bin's fixed-length units and their origins (month and year are absent). */
    private static final java.util.Map<String, String> BUCKET_ORIGINS = bucketOrigins();

    private static java.util.Map<String, String> bucketOrigins() {
        java.util.Map<String, String> m = new java.util.HashMap<>();
        for (String u : List.of("day", "hour", "minute", "second", "millisecond", "microsecond")) {
            m.put(u, "TIMESTAMP '1970-01-01 00:00:00'");
        }
        m.put("week", "TIMESTAMP '1969-12-29 00:00:00'");
        return java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(m));
    }

    /** Pure's hashCode, a signed 64-bit integer: the first 64 bits of the MD5 of the values' text as a JSON array (a
     *  NULL is JSON's null, so the empty collection hashes too), read signed through {@code bit(64)} (measured:
     *  {@code 'x' || 'ffffffffffffffff'} is -1) -- MariaDB's construction. */
    @Override
    protected SqlWriter hashSigned(SqlWriter writer, List<SqlExpr> a) {
        writer.append("CAST(CAST('x' || left(md5(CAST(json_build_array(");
        writer.join(a, ", ", (w, x) -> w.expr(isText(x) || x instanceof SqlExpr.NullLit ? x
                : new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR), 0));
        return writer.append(") AS TEXT)), 16) AS BIT(64)) AS BIGINT)");
    }

    /** Pure round is half-EVEN: Postgres' round(double precision) is rint (probed:
     * 2.5 → 2, 3.5 → 4, -2.5 → -2). There is no round(double, int); a scaled
     * half-even round is unprobed, so it walls. */
    @Override
    protected SqlWriter roundHalfEven(SqlWriter writer, List<SqlExpr> a) {
        if (a.size() == 1) {
            return writer.append("round(CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION))");
        }
        SqlWriter.Piece scale = w -> w.append("CAST(").expr(a.get(1), 0).append(" AS INTEGER)");
        if (isDouble(a.get(0))) {
            // a Float to a scale: scaled, rounded half-even (round(double precision) is rint), scaled
            // back -- DuckDB's ROUND_EVEN(x, s) and H2's form, in double precision
            SqlWriter.Piece p = w -> w.append("power(CAST(10 AS DOUBLE PRECISION), ").piece(scale).append(")");
            return writer.append("(round(CAST(").expr(a.get(0), 0).append(" AS DOUBLE PRECISION) * ").piece(p)
                    .append(") / ").piece(p).append(")");
        }
        // an exact decimal: numeric rounds half AWAY from zero, so an exact .5 of the scaled value
        // steps to its even neighbour (H2's banker's form, in numeric: exact)
        SqlWriter.Piece p = w -> w.append("power(CAST(10 AS NUMERIC), ").piece(scale).append(")");
        SqlWriter.Piece v = w -> w.append("(CAST(").expr(a.get(0), 0).append(" AS NUMERIC) * ").piece(p).append(")");
        return writer.append("(CASE WHEN ").piece(v).append(" - floor(").piece(v)
                .append(") = 0.5 THEN (CASE WHEN mod(floor(")
                .piece(v).append("), 2) = 0 THEN floor(").piece(v).append(") ELSE floor(").piece(v)
                .append(") + 1 END) ELSE round(").piece(v).append(") END / ").piece(p).append(")");
    }

    /** BIGINT bit operators: an INTEGER shift is masked to 32 bits ({@code 1 << 40}
     * is 256 — POSTGRES_BACKEND.md §4.4), so operands widen first; xor is {@code #}. */
    @Override
    protected SqlWriter bitOp(SqlWriter writer, SqlFn fnName, List<SqlExpr> a) {
        SqlWriter.Piece x = w -> w.append("CAST(").expr(a.get(0), 0).append(" AS BIGINT)");
        SqlWriter.Piece y = w -> w.expr(a.get(1), 0);
        return switch (fnName) {
            case BIT_AND -> writer.append("(").piece(x).append(" & CAST(").piece(y).append(" AS BIGINT))");
            case BIT_OR -> writer.append("(").piece(x).append(" | CAST(").piece(y).append(" AS BIGINT))");
            case BIT_XOR -> writer.append("(").piece(x).append(" # CAST(").piece(y).append(" AS BIGINT))");
            case BIT_SHIFT_LEFT -> writer.append("(").piece(x).append(" << CAST(").piece(y).append(" AS INTEGER))");
            case BIT_SHIFT_RIGHT -> writer.append("(").piece(x).append(" >> CAST(").piece(y).append(" AS INTEGER))");
            default -> throw new IllegalStateException("not a bit op: " + fnName);
        };
    }

    /** RE2's inline flags, as the lowering prefixes a pattern with them (RegexpRules.inlineFlags). */
    private static final java.util.regex.Pattern RE2_FLAGS = java.util.regex.Pattern.compile("\\(\\?([ims]+)\\)");

    /**
     * A pattern in Postgres's own flavour (ARE), wrapped in {@code open}/{@code close}: the platform's
     * patterns are RE2's (DuckDB's), whose inline flags Postgres reads otherwise -- its {@code m} is
     * NEWLINE-SENSITIVE, and its default lets {@code .} match a newline, which RE2's never does. So the
     * RE2 flags become ARE options, always spelled, at the very front (where ARE takes them): RE2's
     * default (. stops at a newline, ^ and $ at the ends) is {@code p}, MULTILINE {@code n},
     * NON_NEWLINE_SENSITIVE {@code s}, both {@code w}; CASE_INSENSITIVE {@code i} (probed on 16.15,
     * 2026-10-02).
     */
    private SqlWriter.Piece pattern(SqlExpr p, String open, String close) {
        String flags = "";
        String literal = p instanceof SqlExpr.StringLit lit ? lit.value() : null;   // the body, when literal
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
        boolean multiline = flags.contains("m");
        boolean dotAll = flags.contains("s");
        String options = "(?" + (flags.contains("i") ? "i" : "")
                + (multiline ? (dotAll ? "w" : "n") : (dotAll ? "s" : "p")) + ")";
        if (literal != null) {
            String spelled = stringLit(options + open + literal + close);
            return w -> w.append(spelled);
        }
        SqlExpr dynamic = body;
        return w -> w.append("('" + options + open + "' || ").expr(dynamic, 0)
                .append((close.isEmpty() ? "" : " || '" + close + "'") + ")");
    }

    // ==================================================================
    // Aggregates and windows
    // ==================================================================

    @Override
    protected SqlWriter reducer(SqlWriter writer, SqlAgg.Reducer r) {
        return switch (r.fn()) {
            // ANY_VALUE is Postgres 16+; ordered aggregates keep the plain spelling — Postgres' default null
            // placement (ASC last, DESC first) IS the reference's NULL-largest
            // (avg and the moments are a Float: AggregatesDeliverDouble casts them, windowCall in a window)
            case SUM -> aggregate(writer, "SUM", r);
            case COUNT -> aggregate(writer, "COUNT", r);
            case AVG -> aggregate(writer, "AVG", r);
            case MIN -> extreme(writer, "MIN", "bool_and", " ASC", r);
            case MAX -> extreme(writer, "MAX", "bool_or", " DESC", r);
            case ANY_VALUE -> aggregate(writer, "ANY_VALUE", r);
            case STDDEV_SAMP -> aggregate(writer, "STDDEV_SAMP", r);
            case STDDEV_POP -> aggregate(writer, "STDDEV_POP", r);
            case VAR_SAMP -> aggregate(writer, "VAR_SAMP", r);
            case VAR_POP -> aggregate(writer, "VAR_POP", r);
            case STRING_AGG -> aggregate(writer, "STRING_AGG", r);
            case CORR -> aggregate(writer, "CORR", r);
            case COVAR_SAMP -> aggregate(writer, "COVAR_SAMP", r);
            case COVAR_POP -> aggregate(writer, "COVAR_POP", r);
            case BOOL_AND -> aggregate(writer, "BOOL_AND", r);
            case BOOL_OR -> aggregate(writer, "BOOL_OR", r);
            case VARIANCE -> aggregate(writer, "VARIANCE", r);
            case STDDEV -> aggregate(writer, "STDDEV", r);
            // DuckDB's median interpolates numbers; a median of anything else walls
            case MEDIAN -> median(writer, r);
            case MODE -> mode(writer, r);
            // the reducer's single order key IS the within-group order (H2's arm)
            case QUANTILE_CONT -> percentile(writer, "percentile_cont", r);
            case QUANTILE_DISC -> percentile(writer, "percentile_disc", r);
            // DuckDB's list(x): the values in order, NULLs kept, NULL over no row (array_agg's own), each value
            // held as a list holds it (a list as jsonb, a struct jsonb already)
            case LIST -> listOfValues(writer, r);
            // no arg_max/arg_min: the value at the first row in the key's order, rows of a NULL key
            // ignored, as DuckDB's arg_max/arg_min (a flat, internal array: never a carrier)
            case ARG_MAX -> argExtreme(writer, " DESC", r);
            case ARG_MIN -> argExtreme(writer, " ASC", r);
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY -> throw loweringMarker(r);
            // a window function is a ranking or value function, never an aggregate (windowCall writes it)
            case ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST, NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE,
                 NTH_VALUE -> throw windowFunction(r);
        };
    }

    /** The {@code MAX} and {@code MIN} arms of {@link #reducer}. Postgres has no max/min over BOOLEAN (measured:
     *  "function max(boolean) does not exist"); over false < true they ARE bool_or/bool_and. DataCube's "the group's
     *  one value" columns (CASE WHEN COUNT(DISTINCT b) = 1 THEN MAX(b) END) reach this on every boolean column. Nor
     *  over jsonb (measured: "function max(jsonb) does not exist"), though jsonb is ordered: the first of the values in
     *  that order, NULLs last, is max/min exactly -- NULL only when all are. A Variant column (json read as jsonb,
     *  StoredReads) reaches this in DataCube's one-value columns. */
    private SqlWriter extreme(SqlWriter writer, String name, String overBoolean, String direction, SqlAgg.Reducer r) {
        if (r.args().size() == 1 && r.orderBy().isEmpty() && isBoolean(r.args().get(0))) {
            return writer.append(overBoolean).append("(").expr(r.args().get(0), 0).append(")");
        }
        if (r.args().size() == 1 && r.orderBy().isEmpty() && isJson(r.args().get(0))) {
            return writer.append("(array_agg(").expr(r.args().get(0), 0).append(" ORDER BY ").expr(r.args().get(0), 0)
                    .append(direction).append(" NULLS LAST))[1]");
        }
        return aggregate(writer, name, r);
    }

    /** The {@code MEDIAN} arm of {@link #reducer}. */
    private SqlWriter median(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1 || r.distinct() || !r.orderBy().isEmpty()
                || !isNumeric(r.args().get(0))) {
            throw new DialectCapability("median of a non-number reached Postgres,"
                    + " whose percentile_cont interpolates numbers only");
        }
        return writer.append("percentile_cont(0.5) WITHIN GROUP (ORDER BY ").expr(r.args().get(0), 0)
                .append(")");
    }

    /** The {@code MODE} arm of {@link #reducer}. */
    private SqlWriter mode(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1 || r.distinct() || !r.orderBy().isEmpty()) {
            throw new DialectCapability("a distinct or ordered mode reached Postgres");
        }
        return writer.append("mode() WITHIN GROUP (ORDER BY ").expr(r.args().get(0), 0).append(")");
    }

    /** {@code name(level) WITHIN GROUP (ORDER BY value[ DESC])}. */
    private SqlWriter percentile(SqlWriter writer, String name, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct() || r.orderBy().size() > 1) {
            throw new DialectCapability(r.fn() + " of this shape reached Postgres");
        }
        boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
        return writer.append(name).append("(")
                .expr(r.args().get(1), 0).append(") WITHIN GROUP (ORDER BY ").expr(r.args().get(0), 0)
                .append(desc ? " DESC" : "").append(")");
    }

    /** The {@code LIST} arm of {@link #reducer}. */
    private SqlWriter listOfValues(SqlWriter writer, SqlAgg.Reducer r) {
        if (r.args().size() != 1 || r.distinct() && !r.orderBy().isEmpty()) {
            throw new DialectCapability("a LIST aggregate of this shape reached Postgres");
        }
        SqlExpr x = r.args().get(0);
        writer.append("array_agg(").append(r.distinct() ? "DISTINCT " : "")
                .piece(encode(w -> w.expr(x, 0), typeOf(x)));
        if (!r.orderBy().isEmpty()) {
            writer.append(" ORDER BY ").join(r.orderBy(), ", ", this::aggregateOrderKey);
        }
        return writer.append(")");
    }

    /** The value at the first row in the key's {@code direction}, rows of a NULL key ignored. */
    private SqlWriter argExtreme(SqlWriter writer, String direction, SqlAgg.Reducer r) {
        if (r.args().size() != 2 || r.distinct() || !r.orderBy().isEmpty()) {
            throw new DialectCapability(r.fn() + " of this shape reached Postgres");
        }
        return writer.append("(array_agg(").expr(r.args().get(0), 0).append(" ORDER BY ")
                .expr(r.args().get(1), 0)
                .append(direction).append(") FILTER (WHERE ")
                .expr(r.args().get(1), 0).append(" IS NOT NULL))[1]");
    }

    private static boolean isBoolean(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() == SqlType.Scalar.BOOLEAN;
    }

    private static boolean isText(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() == SqlType.Scalar.VARCHAR;
    }

    private static boolean isJson(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t && t.type() == SqlType.Scalar.JSON;
    }

    private static boolean isNumeric(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == SqlType.Scalar.INTEGER || t.type() == SqlType.Scalar.BIGINT
                        || t.type() == SqlType.Scalar.HUGEINT || t.type() == SqlType.Scalar.DOUBLE
                        || t.type() instanceof SqlType.Decimal);
    }

    /** Ordered-set aggregates (percentile_*, mode) are not window functions here. */
    @Override
    protected SqlWriter windowCall(SqlWriter writer, SqlExpr.WindowCall w) {
        if (w.fn() instanceof SqlAgg.Reducer r && java.util.EnumSet.of(SqlAgg.Fn.MEDIAN,
                SqlAgg.Fn.MODE, SqlAgg.Fn.QUANTILE_CONT, SqlAgg.Fn.QUANTILE_DISC).contains(r.fn())) {
            throw new DialectCapability(r.fn() + " over a window reached Postgres, where"
                    + " ordered-set aggregates take no OVER clause");
        }
        // a windowed avg or moment is numeric here too, and the pass sees only grouped ones
        return w.fn() instanceof SqlAgg.Reducer r && DOUBLE_AGGREGATES.contains(r.fn())
                ? super.windowCall(writer.append("CAST("), w).append(" AS DOUBLE PRECISION)")
                : super.windowCall(writer, w);
    }

    /** The Float aggregates Postgres answers as numeric over an integer or a numeric: cast over their exact
     *  result (AggregatesDeliverDouble), grouped or windowed. */
    private static final java.util.Set<SqlAgg.Fn> DOUBLE_AGGREGATES = java.util.Set.of(SqlAgg.Fn.AVG,
            SqlAgg.Fn.STDDEV_SAMP, SqlAgg.Fn.STDDEV_POP, SqlAgg.Fn.STDDEV, SqlAgg.Fn.VAR_SAMP, SqlAgg.Fn.VAR_POP,
            SqlAgg.Fn.VARIANCE, SqlAgg.Fn.CORR, SqlAgg.Fn.COVAR_SAMP, SqlAgg.Fn.COVAR_POP);

    /** Interval frame offsets in the SQL-standard quoted SINGULAR form
     * ({@code INTERVAL '3' DAY}; {@code '3' DAYS} and {@code 3 DAY} are syntax errors,
     * POSTGRES_BACKEND.md §7). Weeks have no standard qualifier: 7-day multiples. */
    @Override
    protected String bound(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow cr -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.Following f -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> super.bound(b);
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p ->
                    frameInterval(p.n(), p.unit()) + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f ->
                    frameInterval(f.n(), f.unit()) + " FOLLOWING";
        };
    }

    private static String frameInterval(long n, String unit) {
        String u = unit.toUpperCase(java.util.Locale.ROOT);
        String qualifier = FRAME_QUALIFIERS.get(u);
        if (qualifier == null) {
            throw new DialectCapability("a " + unit
                    + " window frame offset has no Postgres interval qualifier");
        }
        return "INTERVAL '" + Math.multiplyExact(n, FRAME_DAYS_PER.getOrDefault(u, 1)) + "' "
                + qualifier;
    }

    /** DurationUnit name → SQL-standard interval qualifier; a week is 7 days. */
    private static final java.util.Map<String, String> FRAME_QUALIFIERS = java.util.Map.of(
            "YEARS", "YEAR", "MONTHS", "MONTH", "WEEKS", "DAY", "DAYS", "DAY",
            "HOURS", "HOUR", "MINUTES", "MINUTE", "SECONDS", "SECOND");
    private static final java.util.Map<String, Integer> FRAME_DAYS_PER = java.util.Map.of("WEEKS", 7);

    // ==================================================================
    // JSON constructors (json, not jsonb: json keeps key order)
    // ==================================================================

    @Override
    protected SqlWriter jsonObject(SqlWriter writer, SqlExpr.JsonObject j) {
        return writer.function("json_build_object", j.kv());
    }

    @Override
    protected SqlWriter jsonArray(SqlWriter writer, SqlExpr.JsonArray j) {
        return writer.function("json_build_array", j.elements());
    }

    /** json_agg keeps NULL elements (DuckDB's json_group_array does too); zero rows
     * aggregate to NULL, so the empty array is coalesced in. */
    @Override
    protected SqlWriter jsonArrayAgg(SqlWriter writer, SqlExpr.JsonArrayAgg j) {
        writer.append("coalesce(json_agg(").expr(j.value(), 0);
        if (!j.orderKeys().isEmpty()) {
            writer.append(" ORDER BY ").join(j.orderKeys(), ", ", (w, k) -> w.expr(k.expr(), 0)
                    .append(k.desc() ? " DESC" : " ASC").append(" NULLS LAST"));
        }
        return writer.append("), '[]')");
    }

    // ==================================================================
    // Date format codes (to_char)
    // ==================================================================

    /** to_char codes; literal text is always double-quoted (a bare letter would
     * read as a code). DuckDB's %n (nine digits) is the microseconds and three
     * zeros: a Postgres timestamp holds microseconds. */
    @Override
    protected String formatText(SqlExpr.FormatLit fl) {
        StringBuilder out = new StringBuilder();
        for (com.legend.sql.DateFmt p : fl.parts()) {
            out.append(switch (p) {
                case com.legend.sql.DateFmt.Text t ->
                        "\"" + t.s().replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
                case com.legend.sql.DateFmt.Part part -> switch (part) {
                    case YEAR4 -> "YYYY";
                    case MONTH2 -> "MM";
                    case DAY2 -> "DD";
                    case HOUR2 -> "HH24";
                    case MIN2 -> "MI";
                    case SEC2 -> "SS";
                    case SUBSEC_MICRO -> "US";
                    case SUBSEC_NANO -> "US\"000\"";
                    // DuckDB's %g: the milliseconds, three digits (the part's engine convention, as Snowflake's FF3)
                    case SUBSEC_MIN -> "MS";
                    case MONTH_ABBREV -> "Mon";
                    case MONTH_NAME -> "FMMonth";
                    case WEEKDAY_NAME -> "FMDay";
                    case HOUR12 -> "HH12";
                    case HOUR12_NOPAD -> "FMHH12";
                    case AMPM -> "AM";
                };
            });
        }
        return out.toString();
    }

    // ==================================================================
    // Values: what the driver hands back
    // ==================================================================

    /** A struct is jsonb here, and the driver hands a jsonb cell back as its own object, whose text is the JSON
     *  (PGobject's toString): decoded to the platform's map, a struct's fields in its declared order (nested ones
     *  too), a Float field a Double, a Decimal one a BigDecimal. A jsonb null (an absent optional instance read off
     *  its owner, {@code x -> 'f'}) is NULL. (Still raw: a date-time field arrives as its text, and a map field as its
     *  entries -- the executor's unwrap would type them, and sql/ cannot reach it: PR #34's review, 2026-10-13.) */
    @Override
    public @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            @com.legend.base.Nullable SqlType type) {
        if (jdbcValue != null && type instanceof SqlType.Struct st && !(jdbcValue instanceof java.util.Map)) {
            String text = jdbcValue.toString();
            if (text.startsWith("{")) {
                return declared(com.legend.sql.Json.parse(text), st);
            }
            if (com.legend.sql.Json.parse(text) == null) {
                return null;
            }
        }
        return jdbcValue;
    }

    /** A parsed JSON value as the type declares it: a struct's fields in declared order, a list's elements, a Float a
     *  Double; anything else as parsed. */
    private static @com.legend.base.Nullable Object declared(@com.legend.base.Nullable Object v, SqlType t) {
        if (t instanceof SqlType.Struct st && v instanceof java.util.Map<?, ?> m) {
            java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
            for (SqlType.Struct.Field f : st.fields()) {
                out.put(f.name(), declared(m.get(f.name()), f.type()));
            }
            return out;
        }
        if (t instanceof SqlType.Array arr && v instanceof List<?> l) {
            List<Object> out = new java.util.ArrayList<>(l.size());
            for (Object e : l) {
                out.add(declared(e, arr.element()));
            }
            return out;
        }
        if (t == SqlType.Scalar.DOUBLE && v instanceof Number n) {
            return n.doubleValue();
        }
        if (t instanceof SqlType.Decimal && v instanceof Number n && !(v instanceof java.math.BigDecimal)) {
            return new java.math.BigDecimal(n.toString());
        }
        return v;
    }

    // ==================================================================
    // DDL (the JVM lane: test fixtures; never on the product path)
    // ==================================================================

    /** Postgres has no TINYINT, BIT or bare DOUBLE; FLOAT is DOUBLE PRECISION as on H2. */
    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> switch (p.kind()) {
                case BIGINT -> "BIGINT";
                case SMALLINT, TINYINT -> "SMALLINT";
                case INTEGER -> "INTEGER";
                case FLOAT, DOUBLE -> "DOUBLE PRECISION";
                case REAL -> "REAL";
                case BIT -> "BOOLEAN";
                case TIMESTAMP -> "TIMESTAMP";
                case DATE -> "DATE";
                case TIME -> "TIME";
                // a nanosecond timestamp column: Postgres's timestamps keep microseconds (the nanosecond leg's wall)
                case TIMESTAMP_NS -> throw new DialectCapability("a TIMESTAMP_NS column: Postgres keeps microseconds");
                case JSON -> "JSONB";
                case VARCHAR -> "VARCHAR";
                case OTHER, DISTINCT, ARRAY, OBJECT -> throw DdlSpelling.unspelled(p.kind());
            };
            case com.legend.sql.SqlDdl.ColumnType.Sized z -> DdlSpelling.sized(z);
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> DdlSpelling.scaled(s);
        };
    }

    /** {@inheritDoc} Its JSON_TYPE answers DuckDB's names (a CASE over jsonb_typeof). */
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

    /** {@inheritDoc} Refused, as TYPEOF is here (its type names are Postgres's). */
    @Override
    protected String typeofName(com.legend.sql.SqlType.Scalar type) {
        throw new DialectCapability("typeof reached Postgres: its type names are Postgres's");
    }

    /** {@inheritDoc} A map is carried as its entries, a jsonb[] of {"key", "value"} (2026-10-12): an ARRAY at the
     *  wire. */
    @Override
    public boolean deliversOwn(SqlType label, String wire) {
        return label instanceof SqlType.Map && wire.equals("ARRAY");
    }
}
