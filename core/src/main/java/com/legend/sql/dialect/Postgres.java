// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;
import java.util.stream.Collectors;

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
 * a portable base arm, a Postgres arm, or a {@link DialectCapability} wall. A wall is
 * honest; a guessed spelling is a silent wrong answer. Collections, structs, lambdas,
 * folds and variant navigation are walls until the jsonb carrier lands (leg P4;
 * POSTGRES_BACKEND.md §5: never Postgres' native ARRAY).
 */
public final class Postgres extends AnsiSqlRenderer {

    public Postgres() {
        super(Lexicon.POSTGRES, TypeNames.POSTGRES, Spellings.POSTGRES);
    }

    // ==================================================================
    // The catalog: a Postgres table's model, from Postgres's own catalog
    // ==================================================================

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

    /** No native PIVOT: static pivots are emulated by the carrier pass; a dynamic
     * pivot needs a key-discovery round trip the browser planner cannot make. */
    @Override
    public boolean needsStaticPivot() {
        return true;
    }

    // ==================================================================
    // Passes and structure
    // ==================================================================

    /** The portable carrier strategies (static pivot, as-of emulation; FULL OUTER
     * stays native), the substring start clamp (Postgres counts empties before 1,
     * like DuckDB), averages delivered as DOUBLE ({@code avg(int)} is numeric here),
     * QUALIFY as a wrapping subselect (Postgres has no QUALIFY), and a constant GROUP BY or
     * ORDER BY key as a typed expression (Postgres reads a bare one as a position, or refuses
     * it). */
    @Override
    protected List<com.legend.sql.SqlRewriter> passes() {
        return List.of(new CarrierStrategies(CarrierStrategies.Caps.POSTGRES),
                new StarExceptToColumns(),
                new WholePartitionOrderedSets(),
                new SubstringClamp(), new AggregatesDeliverDouble(DOUBLE_AGGREGATES), new QualifyToSubselect(true),
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

    /** {@code list}, rendered, when it is a list; refused by name otherwise (an untyped list cannot be
     *  read safely). */
    private String listOf(SqlExpr list, Object what) {
        if (listElement(list) == null) {
            throw new DialectCapability(what + " over " + list.type() + " reached Postgres: a list of unknown"
                    + " element type");
        }
        return expr(list, 0);
    }

    /** A value of type {@code t} read out of jsonb {@code json} (a list element held as jsonb, a struct
     *  field): a scalar by its text, a list rebuilt as a native list, anything else the jsonb itself. */
    private String decode(String json, SqlType t, int depth) {
        if (heldAsItself(t)) {
            return "CAST((" + json + " #>> '{}') AS " + castTypeName(t) + ")";
        }
        if (t instanceof SqlType.Array a) {
            String e = "__e" + depth;
            String o = "__o" + depth;
            return "(CASE WHEN jsonb_typeof(" + json + ") = 'array' THEN ARRAY(SELECT " + held(e, a.element(), depth + 1)
                    + " FROM jsonb_array_elements(" + json + ") WITH ORDINALITY AS __j" + depth + "(" + e + ", " + o
                    + ") ORDER BY " + o + ") END)";
        }
        return json;
    }

    /** An element of type {@code t} read from jsonb as a list holds it: a scalar decoded, anything else
     *  kept as jsonb. */
    private String held(String json, SqlType t, int depth) {
        return heldAsItself(t) ? decode(json, t, depth) : json;
    }

    /** The type a list's elements are held as: their own when held as themselves, jsonb otherwise. */
    private static SqlType held(SqlExpr list) {
        SqlType e = java.util.Objects.requireNonNull(listElement(list));
        return heldAsItself(e) ? e : SqlType.Scalar.JSON;
    }

    /** A value of type {@code t} as a list holds it: a list as jsonb, anything else as it is. */
    private static String encode(String value, SqlType t) {
        return t instanceof SqlType.Array ? "to_jsonb(" + value + ")" : value;
    }

    /** {@code ... FROM} the elements of {@code xs}: each bound to {@code elem} as its type reads (a list
     *  element held as jsonb rebuilt as a list), its 1-based position to {@code idx}. */
    private String elementsFrom(String xs, SqlType element, String elem, String idx) {
        if (!(element instanceof SqlType.Array)) {
            return "unnest(" + xs + ") WITH ORDINALITY AS __u(" + elem + ", " + idx + ")";
        }
        return "(SELECT " + decode("__ue", element, 0) + " AS " + elem + ", __uo AS " + idx + " FROM unnest(" + xs
                + ") WITH ORDINALITY AS __w(__ue, __uo)) AS __u";
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
    protected String structLit(SqlExpr.StructLit st) {
        return "jsonb_build_object(" + st.fields().stream()
                .map(f -> stringLit(f.name()) + ", " + expr(f.value(), 0)).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    protected String structGet(SqlExpr.StructGet g) {
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
        return decode("(" + expr(g.source(), 8) + " -> " + stringLit(g.field()) + ")", type, 0);
    }

    /** A reference to a lambda parameter (or the element/index of an unnest), spelled as the body
     *  spells it -- so the alias that binds it and every use agree. */
    private String param(String name) {
        return expr(SqlExpr.Column.derived(null, name), 0);
    }

    /** {@code ARRAY(SELECT select FROM unnest(xs) WITH ORDINALITY AS __u(elem, idx) [WHERE where] ORDER BY
     *  order)}, NULL for a NULL list (DuckDB's list functions answer NULL there). */
    private String overElements(String xs, SqlType element, String elem, String idx, String select,
            @com.legend.base.Nullable String where, String order) {
        return "(CASE WHEN " + xs + " IS NULL THEN NULL ELSE ARRAY(SELECT " + select + " FROM "
                + elementsFrom(xs, element, elem, idx) + (where == null ? "" : " WHERE " + where)
                + " ORDER BY " + order + ") END)";
    }

    /** The ORDER BY of a list's elements, each {@code x} of type {@code t}: a struct orders by its fields
     *  in their declared order, as DuckDB's does -- jsonb's own order compares an object's keys in its
     *  storage order (shorter keys first), which sorted {@code {k, i, v}} by {@code i}; anything else
     *  by itself. */
    private String sortKey(String x, SqlType t, String direction) {
        if (t instanceof SqlType.Struct st) {
            return st.fields().stream().map(f -> sortKey(f.type() instanceof SqlType.Struct
                    ? "(" + x + " -> " + stringLit(f.name()) + ")"
                    : decode("(" + x + " -> " + stringLit(f.name()) + ")", f.type(), 0), f.type(), direction))
                    .collect(java.util.stream.Collectors.joining(", "));
        }
        return x + direction;
    }

    /** A lambda's element and index parameters, spelled; the index is a fresh name when the lambda has one. */
    private String[] lambdaParams(SqlExpr.Lambda l) {
        return new String[] {param(l.params().get(0)), l.params().size() > 1 ? param(l.params().get(1)) : "__o"};
    }

    @Override
    protected String listCall(SqlFn fnName, List<SqlExpr> args) {
        String xs = listOf(args.get(0), fnName);
        String x = param("__x");
        return switch (fnName) {
            case LIST_FILTER, LIST_TRANSFORM -> {
                SqlType element = java.util.Objects.requireNonNull(listElement(args.get(0)));
                SqlExpr.Lambda l = bodyOver((SqlExpr.Lambda) args.get(1), element);
                String[] p = lambdaParams(l);
                if (fnName == SqlFn.LIST_FILTER) {
                    // a kept element is held again as the list holds it
                    yield overElements(xs, element, p[0], p[1], encode(p[0], element), expr(l.body(), 0), p[1]);
                }
                throw new IllegalStateException("LIST_TRANSFORM is rendered with its result type (call)");
            }
            // NULL || xs is xs, as DuckDB's list_concat
            case LIST_CONCAT -> "(" + String.join(" || ", args.stream().map(e -> listOf(e, fnName)).toList()) + ")";
            // 1-based, negative from the end, out of range NULL (DuckDB's list_extract)
            case LIST_GET -> {
                SqlType element = java.util.Objects.requireNonNull(listElement(args.get(0)));
                String at = "(CASE WHEN " + expr(args.get(1), 0) + " < 0 THEN (" + xs + ")[cardinality(" + xs
                        + ") + 1 + " + expr(args.get(1), 6) + "] ELSE (" + xs + ")[" + expr(args.get(1), 0) + "] END)";
                // an inner list is held as jsonb: read back as a list
                yield element instanceof SqlType.Array ? decode(at, element, 0) : at;
            }
            case LIST_POSITION -> "array_position(" + xs + ", " + expr(args.get(1), 0) + ")";
            // DuckDB's list_distinct drops NULLs; first occurrence order
            case LIST_DISTINCT -> "(CASE WHEN " + xs + " IS NULL THEN NULL ELSE ARRAY(SELECT " + x + " FROM unnest("
                    + xs + ") WITH ORDINALITY AS __u(" + x + ", __o) WHERE " + x + " IS NOT NULL GROUP BY " + x
                    + " ORDER BY min(__o)) END)";
            case LIST_APPEND -> "array_append(" + xs + ", " + encode(expr(args.get(1), 0),
                    java.util.Objects.requireNonNull(listElement(args.get(0)))) + ")";
            // reorderings move elements as the list holds them (an inner list stays jsonb)
            case LIST_SORT -> overElements(xs, held(args.get(0)), x, "__o", x, null,
                    sortKey(x, java.util.Objects.requireNonNull(listElement(args.get(0))), ""));
            case LIST_SORT_DESC -> overElements(xs, held(args.get(0)), x, "__o", x, null,
                    sortKey(x, java.util.Objects.requireNonNull(listElement(args.get(0))), " DESC"));
            case LIST_REVERSE -> overElements(xs, held(args.get(0)), x, "__o", x, null, "__o DESC");
            case LIST_TAIL -> "(" + xs + ")[2:]";
            case LIST_INIT -> "(" + xs + ")[:cardinality(" + xs + ") - 1]";
            // DuckDB's array_slice: 1-based, both ends inclusive
            case LIST_SLICE -> "(" + xs + ")[" + expr(args.get(1), 0) + ":" + expr(args.get(2), 0) + "]";
            case LIST_SUM -> overReduce(xs, "sum(" + x + ")");
            case LIST_MIN -> overReduce(xs, "min(" + x + ")");
            case LIST_MAX -> overReduce(xs, "max(" + x + ")");
            case LIST_AVG -> overReduce(xs, "CAST(avg(" + x + ") AS DOUBLE PRECISION)");
            case LIST_MEDIAN -> overReduce(xs, "percentile_cont(0.5) WITHIN GROUP (ORDER BY " + x + ")");
            case LIST_MODE -> overReduce(xs, "mode() WITHIN GROUP (ORDER BY " + x + ")");
            case LIST_BOOL_AND -> overReduce(xs, "bool_and(" + x + ")");
            case LIST_BOOL_OR -> overReduce(xs, "bool_or(" + x + ")");
            default -> throw new IllegalStateException("not a list call: " + fnName);
        };
    }

    @Override
    protected SqlWriter call(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        // the list-making calls whose first argument is no list
        return switch (c.fn()) {
            // DuckDB's range: [start, stop) by step (start 0, step 1 by default)
            case RANGE_FN -> {
                List<SqlExpr> a = c.args();
                String start = a.size() == 1 ? "0" : expr(a.get(0), 0);
                String stop = expr(a.get(a.size() == 1 ? 0 : 1), 0);
                String step = a.size() > 2 ? expr(a.get(2), 0) : "1";
                yield writer.append("ARRAY(SELECT g FROM generate_series(CAST(").append(start)
                        .append(" AS BIGINT), CAST(").append(stop).append(" AS BIGINT), CAST(").append(step)
                        .append(" AS BIGINT)) AS g WHERE CASE WHEN ").append(step).append(" > 0 THEN g < ").append(stop)
                        .append(" ELSE g > ").append(stop).append(" END)");
            }
            case REPEAT_VALUE -> writer.append("array_fill(").expr(c.args().get(0), 0).append(", ARRAY[CAST(")
                    .expr(c.args().get(1), 0).append(" AS INTEGER)])");
            // each result held as the RESULT list holds it (its type is the call's)
            case LIST_TRANSFORM -> {
                List<SqlExpr> a = c.args();
                String xs = listOf(a.get(0), c.fn());
                SqlType element = java.util.Objects.requireNonNull(listElement(a.get(0)));
                SqlType result = listElement(c);
                if (result == null) {
                    throw new DialectCapability("a list transform of unknown result type reached Postgres");
                }
                SqlExpr.Lambda l = bodyOver((SqlExpr.Lambda) a.get(1), element);
                String[] p = lambdaParams(l);
                yield writer.append(
                        overElements(xs, element, p[0], p[1], encode(expr(l.body(), 0), result), null, p[1]));
            }
            // a struct with one field set (struct_insert): the jsonb object, the field replaced or added
            case STRUCT_INSERT -> writer.append("(").expr(c.args().get(0), 0).append(" || jsonb_build_object(")
                    .expr(c.args().get(1), 0).append(", ").expr(c.args().get(2), 0).append("))");
            default -> postgresCall(writer, c, parentPrec);
        };
    }

    /** exists([]) is false, forAll([]) is true: Pure's empty-collection semantics. */
    @Override
    protected String listExists(List<SqlExpr> args) {
        return listPredicate(args, "bool_or", "FALSE");
    }

    @Override
    protected String listForAll(List<SqlExpr> args) {
        return listPredicate(args, "bool_and", "TRUE");
    }

    private String listPredicate(List<SqlExpr> args, String agg, String empty) {
        String xs = listOf(args.get(0), "exists/forAll");
        SqlType element = java.util.Objects.requireNonNull(listElement(args.get(0)));
        SqlExpr.Lambda l = bodyOver((SqlExpr.Lambda) args.get(1), element);
        String[] p = lambdaParams(l);
        return "coalesce((SELECT " + agg + "(" + expr(l.body(), 0) + ") FROM " + elementsFrom(xs, element, p[0], p[1])
                + "), " + empty + ")";
    }

    /** No duplicates iff the distinct count is the count (a NULL element is a duplicate-free miss, as
     *  DuckDB's len(list_distinct(x)) = len(x)); an empty or NULL list is distinct. */
    @Override
    protected String allDistinct(List<SqlExpr> args) {
        String xs = listOf(args.get(0), "isDistinct");
        String x = param("__x");
        return "coalesce((SELECT count(*) = count(DISTINCT " + x + ") FROM unnest(" + xs + ") AS __u(" + x
                + ")), TRUE)";
    }

    /** DuckDB's list_contains: NULL for a NULL list; found by IS NOT DISTINCT FROM, as array_position. */
    @Override
    protected SqlWriter membership(SqlWriter writer, SqlExpr.Membership m) {
        String xs = listOf(m.collection(), "membership");
        return writer.append("(CASE WHEN ").append(xs).append(" IS NULL THEN NULL ELSE array_position(").append(xs)
                .append(", ")
                .append(encode(expr(m.needle(), 0), java.util.Objects.requireNonNull(listElement(m.collection()))))
                .append(") IS NOT NULL END)");
    }

    /** A named aggregate over a list's elements, in list order. */
    @Override
    protected String reduceCollection(SqlExpr.ReduceCollection rc) {
        String xs = listOf(rc.collection(), rc.reducer());
        SqlType element = java.util.Objects.requireNonNull(listElement(rc.collection()));
        SqlExpr x = SqlExpr.Column.of(null, "__x", element, true, com.legend.sql.OutputCol.Origin.DERIVED);
        List<SqlExpr> args = new java.util.ArrayList<>();
        args.add(x);
        args.addAll(rc.extras());
        SqlAgg.Reducer agg = new SqlAgg.Reducer(rc.reducer(), args, false,
                rc.reducer() == SqlAgg.Fn.STRING_AGG ? List.of(new com.legend.sql.SqlSelect.SortKey(
                        SqlExpr.Column.derived(null, "__o"), true, null, null)) : List.of());
        // avg and the moments over a list are a Float too, cast over their exact result as the grouped ones are
        String reduced = DOUBLE_AGGREGATES.contains(rc.reducer())
                ? "CAST(" + reducer(agg) + " AS DOUBLE PRECISION)" : reducer(agg);
        return "(SELECT " + reduced + " FROM unnest(" + xs + ") WITH ORDINALITY AS __u(" + param("__x")
                + ", " + param("__o") + "))";
    }

    /** A list literal, typed (so an empty one has a type), each element held as the list holds it. */
    private String arrayLiteral(SqlExpr.ArrayLit a) {
        SqlType element = listElement(a);
        if (element == null) {
            throw new DialectCapability("a list literal of " + a.type() + " reached Postgres: a list of unknown"
                    + " element type");
        }
        return "CAST(ARRAY[" + a.elements().stream().map(e -> encode(expr(e, 0), element)).collect(Collectors.joining(", "))
                + "] AS " + carrier(element) + "[])";
    }

    /** A list exploded to rows in the select list: unnest keeps the list's order. */
    @Override
    protected String unnestProjection(List<SqlExpr> args) {
        return "unnest(" + listOf(args.get(0), "UNNEST") + ")";
    }

    /** fold(xs, {element, accumulator | body}, init): a correlated recursive walk over the elements, the
     *  accumulator typed as the fold's result (Postgres types a recursive column by its first term). */
    @Override
    protected String foldCall(SqlExpr.FoldCall f) {
        if (f.accIsList() || !(f.type() instanceof com.legend.sql.TypeFact.Typed t)
                || !(t.type() instanceof SqlType.Scalar || t.type() instanceof SqlType.Decimal)) {
            throw new DialectCapability("a fold into a " + f.type() + " reached Postgres: a list accumulator"
                    + " is the jsonb carrier (leg P4)");
        }
        String xs = listOf(f.source(), "fold");
        String type = castTypeName(t.type());
        String elem = param(f.lambda().params().get(0));
        String acc = param(f.lambda().params().get(1));
        return "(WITH RECURSIVE __f(__i, __acc) AS (SELECT 0, CAST(" + expr(f.init(), 0) + " AS " + type + ")"
                + " UNION ALL SELECT __f.__i + 1, CAST(" + expr(f.lambda().body(), 0) + " AS " + type + ")"
                + " FROM __f CROSS JOIN LATERAL (SELECT (" + xs + ")[__f.__i + 1] AS " + elem + ", __f.__acc AS "
                + acc + ") AS __b WHERE __f.__i < coalesce(cardinality(" + xs + "), 0))"
                + " SELECT __acc FROM __f ORDER BY __i DESC LIMIT 1)";
    }

    /** A reduction of a list's elements: {@code (SELECT agg FROM unnest(xs) AS __u(__x))}. */
    private String overReduce(String xs, String agg) {
        return "(SELECT " + agg + " FROM unnest(" + xs + ") AS __u(" + param("__x") + "))";
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
    protected String storedRead(com.legend.sql.SqlExpr.StoredRead r) {
        String ref = expr(r.column(), 0);
        return switch (jsonbRead(r.stored())) {
            case CAST -> "CAST(" + ref + " AS JSONB)";
            case CONVERT -> "to_jsonb(" + ref + ")";
            case NONE -> super.storedRead(r);
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
                case BIGINT, SMALLINT, TINYINT, INTEGER, FLOAT, DOUBLE, REAL, BIT, TIMESTAMP, DATE,
                        VARCHAR, OTHER, DISTINCT -> JsonbRead.NONE;
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
    protected String projection(com.legend.sql.SqlSelect.Projection p) {
        if (p.expr() instanceof SqlExpr.NullLit && p.out() != null && p.out().type() instanceof SqlType.Array arr) {
            String typed = "CAST(NULL AS " + carrier(arr.element()) + "[])";
            String label = p.alias() != null ? aliasIdent(p.alias()) : implicitLabel(p);
            return label == null ? typed : typed + " AS " + label;
        }
        return super.projection(p);
    }

    /** Every identifier quoted (Postgres folds a bare one to lowercase); a
     * quote-bearing declaration ({@code "date"}) is already its own spelling. */
    @Override
    protected String ident(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")
                && !name.substring(1, name.length() - 1).replace("\"\"", "").contains("\"")) {
            return name;
        }
        return delimited(name);
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
            return writer.append(arrayLiteral(a));
        }
        if (e instanceof SqlExpr.OrderedListAgg) {
            throw new DialectCapability("an ordered list aggregate reached Postgres before"
                    + " the jsonb collection carrier (leg P4)");
        }
        return super.expr(writer, e, parentPrec);
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
    protected String variantAwareCast(SqlExpr.Cast c) {
        if (c.target() instanceof SqlType.Array arr) {
            return listCast(c, arr);
        }
        // Map<String, ...> of a Variant: the jsonb object itself (keys and values read by MAP_KEYS / MAP_VALUES)
        if (c.target() instanceof SqlType.Map && isJson(c.value())) {
            String v = expr(c.value(), 0);
            return "(CASE WHEN jsonb_typeof(" + v + ") = 'object' THEN " + v + " END)";
        }
        if (c.target() instanceof SqlType.Map) {
            throw new DialectCapability("a cast to " + c.target() + " reached Postgres before"
                    + " the jsonb collection carrier (leg P4)");
        }
        // A VARIANT is jsonb: text parses into it; any other value converts (no cast from a number
        // or a boolean to jsonb exists)
        if (c.target() == SqlType.Scalar.JSON) {
            return isText(c.value()) ? "CAST(" + expr(c.value(), 0) + " AS JSONB)"
                    : "to_jsonb(" + expr(c.value(), 0) + ")";
        }
        // a value out of a variant: its TEXT (->> strips JSON quoting; #>> '{}' for the whole value),
        // then the cast -- the swap lives in rendering, as DuckDB's
        if (c.target() != SqlType.Scalar.TEMPORAL_TEXT && c.target() != SqlType.Scalar.DECIMAL_TEXT) {
            if (c.value() instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET) {
                boolean root = call.args().get(1) instanceof SqlExpr.StringLit k && "$".equals(k.value());
                return "CAST((" + expr(call.args().get(0), 8) + (root ? " #>> '{}'" : " ->> " + expr(call.args().get(1), 8))
                        + ") AS " + castTypeName(c.target()) + ")";
            }
            // a whole value to a scalar reads its text; to TEXT it is its JSON text, as DuckDB's cast
            if (isJson(c.value()) && c.target() != SqlType.Scalar.VARCHAR) {
                return "CAST((" + expr(c.value(), 8) + " #>> '{}') AS " + castTypeName(c.target()) + ")";
            }
            if (isJson(c.value())) {
                return compactJson("CAST(" + expr(c.value(), 0) + " AS VARCHAR)");
            }
        }
        return super.variantAwareCast(c);
    }

    /** A value to a list of scalars: a Variant array's elements read as the element type, in order (a
     *  JSON null or a non-array is a NULL list, as DuckDB's cast); a list of another element type cast
     *  element-wise. */
    private String listCast(SqlExpr.Cast c, SqlType.Array target) {
        if (isJson(c.value())) {
            // a Variant array: its elements held as the list holds them
            return decode(expr(c.value(), 0), target, 0);
        }
        SqlType from = listElement(c.value());
        if (from != null && heldAsItself(from) && heldAsItself(target.element())) {
            return "CAST(" + expr(c.value(), 0) + " AS " + carrier(target.element()) + "[])";
        }
        if (from != null && !heldAsItself(from) && !heldAsItself(target.element())) {
            return expr(c.value(), 0);   // both held as jsonb
        }
        throw new DialectCapability("a cast of " + c.value().type() + " to " + target + " reached Postgres");
    }

    /** jsonb's text, compact as Pure and DuckDB print JSON: jsonb puts a space after every ',' and ':'
     *  outside a string, and only there -- so a string-aware replace drops exactly those (a string, with
     *  its escapes, is matched whole and kept). Keys stay in jsonb's own order. */
    private static String compactJson(String text) {
        return "regexp_replace(" + text + ", '(\"(?:[^\"\\\\]|\\\\.)*\")|([,:]) ', '\\1\\2', 'g')";
    }

    /** VARIANT navigation over jsonb: a key or a 0-based index (-1 the last, past the end NULL --
     *  DuckDB's JSON arrow agrees). */
    @Override
    protected String variantGet(List<SqlExpr> args) {
        // the JSON path '$' (the lowering's root read) is the value itself; any other key is a member
        if (args.get(1) instanceof SqlExpr.StringLit root && "$".equals(root.value())) {
            return expr(args.get(0), 0);
        }
        return "(" + expr(args.get(0), 8) + " -> " + expr(args.get(1), 8) + ")";
    }

    @Override
    protected String variantConstruct(List<SqlExpr> a) {
        if (a.size() != 1) {
            throw new DialectCapability("toVariant of " + a.size() + " arguments reached Postgres");
        }
        return "to_jsonb(" + expr(a.get(0), 0) + ")";
    }

    // ==================================================================
    // Scalar functions — every SqlFn decided here
    // ==================================================================

    /** Every SqlFn but the list-making ones ({@link #call}). */
    private SqlWriter postgresCall(SqlWriter writer, SqlExpr.Call c, int parentPrec) {
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            // ---- portable: the base arm (or a Spellings.POSTGRES row) is Postgres SQL
            case AND, OR, NOT, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL,
                 PLUS, MINUS, TIMES, NEGATE, IS_NULL, IS_NOT_NULL, IN, IS_DISTINCT_FROM,
                 NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL, XOR, CONCAT, CONCAT_JOIN, PARSE_INT,
                 PARSE_DATE, PI, CEILING, FLOOR, SIGN, LPAD, RPAD, UC_FIRST, LC_FIRST, TODAY,
                 DATE_TRUNC_DAY, BOOL_TO_TEXT, CURRENT_USER_FN,
                 // bit operators and rounding ride the base's hooks (overridden below);
                 // HASH's hook walls: Postgres has no signed 64-bit hash of the reference
                 BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT, ROUND, HASH,
                 // Spellings.POSTGRES rows
                 ABS, ASCII_CODE, ATAN, ATAN2, CBRT, CHR, COALESCE, COS, COSH, COT, DEGREES,
                 FLOOR_RAW, GREATEST, LEAST, LEFT, LOWER, LTRIM, MD5,
                 RADIANS, REPEAT_STR, REPLACE, REVERSE_STRING, RIGHT, RTRIM,
                 SIN, SINH, SPLIT_PART, STARTS_WITH, STRPOS, SUBSTRING, TAN, TANH,
                 TIMEZONE, TRIM, UPPER -> super.call(writer, c, parentPrec);
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
            case MOD, REM -> {
                if (a.stream().anyMatch(Postgres::isDouble)) {
                    throw new DialectCapability(c.fn() + " over a Float reached Postgres,"
                            + " which has no double-precision remainder");
                }
                yield super.call(writer, c, parentPrec);
            }
            // DuckDB's // truncates toward zero; so does div() (numeric), and a
            // double operand is refused by Postgres itself (no implicit cast)
            // ~x on BIGINT (the base's xor() is DuckDB's)
            case BIT_NOT -> writer.append("(~ CAST(").expr(a.get(0), 0).append(" AS BIGINT))");
            case INT_DIVIDE -> writer.append("CAST(div(").expr(a.get(0), 0).append(", ").expr(a.get(1), 0)
                    .append(") AS BIGINT)");
            // Pure's divide-with-scale is HALF_UP; Postgres rounds double precision
            // half-EVEN and numeric half away from zero (POSTGRES_BACKEND.md §4.3),
            // and has no round(double, int) at all
            case ROUND_HALF_UP -> {
                String r = "round(CAST(" + expr(a.get(0), 0) + " AS NUMERIC)"
                        + (a.size() > 1 ? ", CAST(" + expr(a.get(1), 0) + " AS INTEGER)" : "")
                        + ")";
                yield isDouble(a.get(0))
                        ? writer.append("CAST(").append(r).append(" AS DOUBLE PRECISION)")
                        : writer.append(r);
            }
            // the engine's out-of-domain answer is NaN; Postgres raises
            case ACOS, ASIN -> {
                String x = expr(a.get(0), 0);
                String f = c.fn() == SqlFn.ACOS ? "acos" : "asin";
                yield writer.append("(CASE WHEN (").append(x).append(") BETWEEN -1 AND 1 THEN ").append(f).append("(")
                        .append(x).append(") ELSE CAST('NaN' AS DOUBLE PRECISION) END)");
            }

            // ---- strings
            // the engine coerces a non-text argument (DuckDb's arm)
            case LENGTH -> a.get(0) instanceof SqlExpr.StringLit
                    ? writer.append("length(").list(a).append(")")
                    : writer.append("length(CAST(").expr(a.get(0), 0).append(" AS VARCHAR))");
            case ENDS_WITH -> writer.append("(right(").expr(a.get(0), 0).append(", length(").expr(a.get(1), 0)
                    .append(")) = ").expr(a.get(1), 0).append(")");
            // MATCHES is the PARTIAL test, Postgres' ~ (never regexp_matches: set-
            // returning, it deletes rows in a projection, POSTGRES_BACKEND.md §4.1)
            case MATCHES -> writer.append("(").expr(a.get(0), 7).append(" ~ ").append(pattern(a.get(1), "", ""))
                    .append(")");
            // full match: ~ is partial on Postgres (§4.2), so the pattern anchors (after its options)
            case REGEXP_FULL_MATCH -> writer.append("(").expr(a.get(0), 7).append(" ~ ")
                    .append(pattern(a.get(1), "^(?:", ")$")).append(")");
            // regexp_extract(s, p[, g]) is '' on a miss; regexp_substr is NULL
            case REGEXP_EXTRACT -> writer.append("coalesce(regexp_substr(").expr(a.get(0), 0).append(", ")
                    .append(pattern(a.get(1), "", "")).append(", 1, 1, '', ")
                    .append((a.size() > 2 ? expr(a.get(2), 0) : "0")).append("), '')");
            // regexp_replace(s, p, r[, options]): DuckDB's 'g' is Postgres's
            case REGEXP_REPLACE -> writer.append("regexp_replace(").expr(a.get(0), 0).append(", ")
                    .append(pattern(a.get(1), "", ""))
                    .append(a.subList(2, a.size()).stream().map(x -> ", " + expr(x, 0)).collect(Collectors.joining()))
                    .append(")");
            // Postgres' base64 wraps lines at 76 characters
            case ENCODE_BASE64 -> writer.append("replace(encode(convert_to(").expr(a.get(0), 0)
                    .append(", 'UTF8'), 'base64'), chr(10), '')");
            case DECODE_BASE64 -> writer.append("convert_from(decode(").expr(a.get(0), 0)
                    .append(", 'base64'), 'UTF8')");
            // sha256 is bytea here; the reference is lowercase hex text
            case SHA256 -> writer.append("encode(sha256(convert_to(").expr(a.get(0), 0).append(", 'UTF8')), 'hex')");
            case GUID -> writer.append("CAST(gen_random_uuid() AS VARCHAR)");

            // ---- temporal
            case NOW -> writer.append("(now() AT TIME ZONE 'UTC')");
            case STRFTIME -> {
                if (!(a.get(1) instanceof SqlExpr.FormatLit)) {
                    throw new DialectCapability("a date format that is not a format literal"
                            + " reached Postgres: its codes are DuckDB's");
                }
                yield writer.append("to_char(").append(naive(a.get(0))).append(", ").expr(a.get(1), 0).append(")");
            }
            case DAYNAME -> writer.append("to_char(").append(naive(a.get(0))).append(", 'FMDay')");
            case MONTHNAME -> writer.append("to_char(").append(naive(a.get(0))).append(", 'FMMonth')");
            // DuckDB's date_part is an integer (seconds truncated); extract is numeric
            case EXTRACT -> {
                String part = literal(a.get(0), "EXTRACT");
                if (!List.of("year", "quarter", "month", "week", "day", "doy", "dow",
                        "isodow", "hour", "minute", "second").contains(part)) {
                    throw new DialectCapability("date part '" + part
                            + "' has no probed Postgres spelling");
                }
                yield writer.append("CAST(floor(extract(").append(part).append(" FROM ").expr(a.get(1), 0)
                        .append(")) AS BIGINT)");
            }
            case DATE_TRUNC -> writer.append(dateTrunc(a));
            // Postgres' make_date/make_timestamp take INTEGER, never BIGINT
            case MAKE_DATE -> writer.append("make_date(")
                    .append(a.stream().map(x -> "CAST(" + expr(x, 0) + " AS INTEGER)")
                            .collect(Collectors.joining(", ")))
                    .append(")");
            case MAKE_TIMESTAMP -> {
                if (a.size() != 6) {
                    throw new DialectCapability("make_timestamp of " + a.size()
                            + " arguments has no Postgres spelling");
                }
                writer.append("make_timestamp(");
                writer.append(a.subList(0, 5).stream()
                        .map(x -> "CAST(" + expr(x, 0) + " AS INTEGER)")
                        .collect(Collectors.joining(", ")));
                yield writer.append(", CAST(").expr(a.get(5), 0).append(" AS DOUBLE PRECISION))");
            }
            // (unitFn, amount, date): amount × a one-unit interval — exact for every
            // unit and any BIGINT amount (make_interval takes INTEGER)
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> op(writer, parentPrec, () -> writer.expr(a.get(2), 5)
                    .append(" + ").expr(a.get(1), 6).append(" * INTERVAL '1 ")
                    .append(intervalUnit(literal(a.get(0), c.fn().name()))).append("'"));
            case DATE_DIFF -> writer.append(dateDiff(a));
            // the origins are the base's (weeks align to the Monday 1969-12-29, everything else to
            // 1970); date_bin bins fixed-length intervals, so months and years count whole calendar
            // months from 1970-01 and floor to the bucket's multiple, as DuckDB's time_bucket does
            // (before 1970 too: floor, not truncation)
            case TIME_BUCKET -> {
                String unit = intervalUnit(literal(a.get(0), "TIME_BUCKET"));
                String origin = BUCKET_ORIGINS.get(unit);
                if (origin != null) {
                    yield writer.append("date_bin(").expr(a.get(1), 6).append(" * INTERVAL '1 ").append(unit)
                            .append("', ").append(naive(a.get(2))).append(", ").append(origin).append(")");
                }
                Integer monthsPerUnit = CALENDAR_UNITS.get(unit);
                if (monthsPerUnit == null) {
                    throw new DialectCapability("a " + unit + " time bucket has no Postgres spelling");
                }
                // the month index since 1970-01, floored to the bucket's size in months: n months, or
                // 12n for n years (a year bucket of the month index is the year's own, before 1970 too)
                String t = naive(a.get(2));
                String size = "(CAST(" + expr(a.get(1), 0) + " AS INTEGER) * " + monthsPerUnit + ")";
                yield writer.append("(TIMESTAMP '1970-01-01 00:00:00' + CAST(floor(((extract(year FROM ").append(t)
                        .append(") - 1970) * 12 + extract(month FROM ").append(t).append(") - 1) / ").append(size)
                        .append(") AS INTEGER) * ").append(size).append(" * INTERVAL '1 month')");
            }
            // DuckDB's epoch(ts) is DOUBLE seconds; epoch_ms truncates toward zero
            case EPOCH_SECONDS -> writer.append("CAST(extract(epoch FROM ").expr(a.get(0), 0)
                    .append(") AS DOUBLE PRECISION)");
            case EPOCH_MS -> writer.append("CAST(trunc(extract(epoch FROM ").expr(a.get(0), 0)
                    .append(") * 1000) AS BIGINT)");
            // epoch arithmetic on a naive timestamp: no session zone involved
            case FROM_EPOCH_SECONDS -> op(writer, parentPrec, () -> writer.append("TIMESTAMP '1970-01-01 00:00:00' + ")
                    .expr(a.get(0), 6).append(" * INTERVAL '1 second'"));
            case FROM_EPOCH_MS -> op(writer, parentPrec, () -> writer.append("TIMESTAMP '1970-01-01 00:00:00' + CAST(")
                    .expr(a.get(0), 0).append(" AS BIGINT) * INTERVAL '1 millisecond'"));

            // error() outside a CASE branch: the raise as text (see raise)
            case ERROR -> writer.append(raise(a, null));

            // ---- walls
            case STRPTIME -> throw wall(c.fn(), "to_timestamp(text, fmt) is lenient and"
                    + " zone-bound; not yet probed against the reference");
            case FORMAT -> throw wall(c.fn(), "Postgres' format() has no %d/%f");
            case SHA1, LEVENSHTEIN, JARO_WINKLER ->
                    throw wall(c.fn(), "an extension function (pgcrypto/fuzzystrmatch)");
            // ---- variant (jsonb)
            case TO_VARIANT, VARIANT_GET -> super.call(writer, c, parentPrec);
            // in DuckDB's json_type vocabulary, which the lowering compares against (NULL, VARCHAR,
            // BIGINT, DOUBLE, BOOLEAN, ARRAY, OBJECT): a whole number is a BIGINT, any other a DOUBLE
            case JSON_TYPE -> {
                String v = expr(a.get(0), 0);
                yield writer.append("(CASE jsonb_typeof(").append(v)
                        .append(") WHEN 'null' THEN 'NULL' WHEN 'string' THEN 'VARCHAR'")
                        .append(" WHEN 'boolean' THEN 'BOOLEAN' WHEN 'array' THEN 'ARRAY' WHEN 'object' THEN 'OBJECT'")
                        .append(" WHEN 'number' THEN CASE WHEN (").append(v)
                        .append(" #>> '{}') ~ '^-?[0-9]+$' THEN 'BIGINT'").append(" ELSE 'DOUBLE' END END)");
            }
            case JSON_ARRAY_LENGTH -> writer.append("jsonb_array_length(").expr(a.get(0), 0).append(")");
            case JSON_PRETTY -> writer.append("jsonb_pretty(").expr(a.get(0), 0).append(")");
            // a Variant array's elements, each a Variant (DuckDB: CAST(x AS JSON[]))
            // (idempotent, as DuckDB's cast: a value already a list of Variants passes through)
            case VARIANT_ELEMENTS -> listElement(a.get(0)) != null
                    ? writer.expr(a.get(0), 0)
                    : writer.append(decode(expr(a.get(0), 0), new SqlType.Array(SqlType.Scalar.JSON), 0));
            case JSON_MERGE_PATCH -> throw wall(c.fn(), "variant over jsonb is leg P4");
            // ---- lists: a list of scalars is a native array (listCall and its siblings below)
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_GET, LIST_POSITION,
                 LIST_EXISTS, LIST_FOR_ALL, UNNEST, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN,
                 LIST_MAX, LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_SORT, LIST_SORT_DESC, LIST_TAIL,
                 LIST_INIT, RANGE_FN, LIST_SLICE, REPEAT_VALUE, LIST_BOOL_AND, LIST_BOOL_OR,
                 ALL_DISTINCT, LIST_REVERSE -> super.call(writer, c, parentPrec);
            case LIST_LENGTH -> writer.append("cardinality(").append(listOf(a.get(0), c.fn())).append(")");
            // DuckDB's string_split('', d) is [''], Postgres's string_to_array is {}
            case SPLIT -> writer.append("(CASE WHEN ").expr(a.get(0), 0)
                    .append(" = '' THEN ARRAY[''] ELSE string_to_array(").expr(a.get(0), 0).append(", ")
                    .expr(a.get(1), 0).append(") END)");
            // DuckDB's regexp_extract_all(s, p[, g]): group g (0, the whole match, by default) of every
            // match -- the n-th match's group by regexp_substr, as REGEXP_EXTRACT reads one (regexp_matches
            // answers the capture groups alone once a pattern has any: its m[1] is group 1)
            case REGEXP_EXTRACT_ALL -> {
                String str = expr(a.get(0), 0);
                String pat = pattern(a.get(1), "", "");
                yield writer.append("ARRAY(SELECT regexp_substr(").append(str).append(", ").append(pat)
                        .append(", 1, __n, '', ").append((a.size() > 2 ? expr(a.get(2), 0) : "0"))
                        .append(") FROM generate_series(1, regexp_count(").append(str).append(", ").append(pat)
                        .append(")) AS __g(__n) ORDER BY __n)");
            }
            // a map is a jsonb object (a Variant read as Map<String, ...>): its keys and values, in
            // jsonb's own key order
            case MAP_KEYS -> writer.append("ARRAY(SELECT k FROM jsonb_object_keys(").expr(a.get(0), 0)
                    .append(") AS k)");
            case MAP_VALUES -> writer.append("ARRAY(SELECT v FROM jsonb_each(").expr(a.get(0), 0)
                    .append(") AS e(k, v))");
            // the jsonb carrier for nested lists, structs and mixed lists is the next step
            case STRUCT_INSERT -> throw new IllegalStateException("STRUCT_INSERT is rendered by call");
            case LIST_FLATTEN, MAP_FROM_LISTS, MAP_FROM_ENTRIES, MAP_EMPTY, MAP_EXTRACT,
                 MAP_CONCAT, PURE_SPLIT_PART, LIST_ZIP, LIST_PRODUCT, LIST_REDUCE, TYPEOF ->
                    throw wall(c.fn(), "collections over the jsonb carrier are leg P4");
        };
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
     */
    private String raise(List<SqlExpr> a, @com.legend.base.Nullable SqlType slot) {
        String position = a.size() > 1 ? expr(a.get(1), 0) + " || chr(30) || " : "";
        String text = "CAST(CAST(chr(31) || " + position + "(" + expr(a.get(0), 0)
                + ") || chr(31) AS TIMESTAMPTZ) AS VARCHAR)";
        // typed as its slot (never evaluated: the TIMESTAMPTZ cast raises first) -- a list of scalars too
        boolean typed = slot instanceof SqlType.Scalar || slot instanceof SqlType.Decimal
                || slot instanceof SqlType.Array arr && (arr.element() instanceof SqlType.Scalar
                        || arr.element() instanceof SqlType.Decimal);
        return typed && slot != null ? "CAST(" + text + " AS " + castTypeName(slot) + ")" : text;
    }

    /** The base CASE, with a branch that IS error() raised in the CASE's own type. */
    @Override
    protected String caseExpr(SqlExpr.Case c) {
        SqlType slot = c.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type() : null;
        StringBuilder sb = new StringBuilder("CASE");
        for (SqlExpr.Case.When w : c.whens()) {
            sb.append(" WHEN ").append(expr(w.condition(), 0))
                    .append(" THEN ").append(branch(w.then(), slot));
        }
        if (c.otherwise() != null) {
            sb.append(" ELSE ").append(branch(c.otherwise(), slot));
        }
        return sb.append(" END").toString();
    }

    private String branch(SqlExpr value, @com.legend.base.Nullable SqlType slot) {
        return value instanceof SqlExpr.Call call && call.fn() == SqlFn.ERROR
                ? raise(call.args(), slot) : expr(value, 0);
    }

    private static DialectCapability wall(SqlFn fn, String why) {
        return new DialectCapability(fn + " reached Postgres: " + why);
    }

    private static boolean isDouble(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t
                && t.type() == SqlType.Scalar.DOUBLE;
    }

    private static String literal(SqlExpr e, String what) {
        if (e instanceof SqlExpr.StringLit s) {
            return s.value();
        }
        throw new DialectCapability(what + " with a non-literal part reached Postgres");
    }

    /** A temporal operand as a NAIVE timestamp: a DATE implicitly casts to
     * timestamptz in Postgres' function resolution (date_trunc, to_char, date_bin),
     * which the session zone would then shift; a declared TIMESTAMPTZ stays one. */
    private String naive(SqlExpr e) {
        if (e.type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == SqlType.Scalar.TIMESTAMP || t.type() == SqlType.Scalar.TIMESTAMPTZ)) {
            return expr(e, 0);
        }
        return "CAST(" + expr(e, 0) + " AS TIMESTAMP)";
    }

    /** The base's contract: Date-grained parts (year/quarter/month/week) deliver a
     * DATE, finer parts a TIMESTAMP (DuckDb casts 'day' back to one). */
    private String dateTrunc(List<SqlExpr> a) {
        String part = literal(a.get(0), "DATE_TRUNC");
        String trunc = "date_trunc(" + stringLit(part) + ", " + naive(a.get(1)) + ")";
        if (DATE_GRAINED.contains(part)) {
            return "CAST(" + trunc + " AS DATE)";
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
    private String dateDiff(List<SqlExpr> a) {
        String part = literal(a.get(0), "DATE_DIFF");
        String from = expr(a.get(1), 0);
        String to = expr(a.get(2), 0);
        String y = "(extract(year FROM " + to + ") - extract(year FROM " + from + "))";
        Integer perYear = PARTS_PER_YEAR.get(part);
        String scale = EPOCH_SCALE.get(part);
        String body;
        if (perYear != null) {
            // year/quarter/month: whole years in the unit, plus the in-year part's step
            body = perYear == 1 ? y : y + " * " + perYear + " + (extract(" + part + " FROM " + to
                    + ") - extract(" + part + " FROM " + from + "))";
        } else if (scale != null) {
            body = epochBoundaries(from, to, scale);
        } else if (DAY_PART.equals(part)) {
            body = "CAST(" + to + " AS DATE) - CAST(" + from + " AS DATE)";
        } else {
            // week is DuckDB's plain day count / 7 (Saturday -> Monday is 0), not a
            // boundary count: unprobed beyond that, so it walls
            throw new DialectCapability("date_diff part '" + part + "' has no probed Postgres spelling");
        }
        return "CAST(" + body + " AS BIGINT)";
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
    private static String epochBoundaries(String from, String to, String scale) {
        return "floor(extract(epoch FROM " + to + ")" + scale + ") - floor(extract(epoch FROM "
                + from + ")" + scale + ")";
    }

    /** DuckDB's interval-function name ({@code to_days}) → a Postgres interval unit. */
    private static String intervalUnit(String unitFn) {
        String unit = INTERVAL_UNITS.get(unitFn);
        if (unit == null) {
            throw new DialectCapability("interval unit '" + unitFn + "' has no Postgres spelling");
        }
        return unit;
    }

    private static final java.util.Map<String, String> INTERVAL_UNITS = java.util.Map.of(
            "to_years", "year", "to_months", "month", "to_weeks", "week", "to_days", "day",
            "to_hours", "hour", "to_minutes", "minute", "to_seconds", "second",
            "to_milliseconds", "millisecond", "to_microseconds", "microsecond");

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

    /** Pure round is half-EVEN: Postgres' round(double precision) is rint (probed:
     * 2.5 → 2, 3.5 → 4, -2.5 → -2). There is no round(double, int); a scaled
     * half-even round is unprobed, so it walls. */
    @Override
    protected String roundHalfEven(List<SqlExpr> a) {
        if (a.size() == 1) {
            return "round(CAST(" + expr(a.get(0), 0) + " AS DOUBLE PRECISION))";
        }
        String scale = "CAST(" + expr(a.get(1), 0) + " AS INTEGER)";
        if (isDouble(a.get(0))) {
            // a Float to a scale: scaled, rounded half-even (round(double precision) is rint), scaled
            // back -- DuckDB's ROUND_EVEN(x, s) and H2's form, in double precision
            String p = "power(CAST(10 AS DOUBLE PRECISION), " + scale + ")";
            return "(round(CAST(" + expr(a.get(0), 0) + " AS DOUBLE PRECISION) * " + p + ") / " + p + ")";
        }
        // an exact decimal: numeric rounds half AWAY from zero, so an exact .5 of the scaled value
        // steps to its even neighbour (H2's banker's form, in numeric: exact)
        String p = "power(CAST(10 AS NUMERIC), " + scale + ")";
        String v = "(CAST(" + expr(a.get(0), 0) + " AS NUMERIC) * " + p + ")";
        return "(CASE WHEN " + v + " - floor(" + v + ") = 0.5 THEN (CASE WHEN mod(floor(" + v
                + "), 2) = 0 THEN floor(" + v + ") ELSE floor(" + v + ") + 1 END) ELSE round(" + v
                + ") END / " + p + ")";
    }

    /** BIGINT bit operators: an INTEGER shift is masked to 32 bits ({@code 1 << 40}
     * is 256 — POSTGRES_BACKEND.md §4.4), so operands widen first; xor is {@code #}. */
    @Override
    protected String bitOp(SqlFn fnName, List<SqlExpr> a) {
        String x = "CAST(" + expr(a.get(0), 0) + " AS BIGINT)";
        String y = expr(a.get(1), 0);
        return switch (fnName) {
            case BIT_AND -> "(" + x + " & CAST(" + y + " AS BIGINT))";
            case BIT_OR -> "(" + x + " | CAST(" + y + " AS BIGINT))";
            case BIT_XOR -> "(" + x + " # CAST(" + y + " AS BIGINT))";
            case BIT_SHIFT_LEFT -> "(" + x + " << CAST(" + y + " AS INTEGER))";
            case BIT_SHIFT_RIGHT -> "(" + x + " >> CAST(" + y + " AS INTEGER))";
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
    private String pattern(SqlExpr p, String open, String close) {
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
        return literal != null
                ? stringLit(options + open + literal + close)
                : "('" + options + open + "' || " + expr(body, 0) + (close.isEmpty() ? "" : " || '" + close + "'") + ")";
    }

    // ==================================================================
    // Aggregates and windows
    // ==================================================================

    @Override
    protected String reducer(SqlAgg.Reducer r) {
        // Postgres has no max/min over BOOLEAN (measured: "function max(boolean) does not exist");
        // over false < true they ARE bool_or/bool_and. DataCube's "the group's one value" columns
        // (CASE WHEN COUNT(DISTINCT b) = 1 THEN MAX(b) END) reach this on every boolean column.
        if ((r.fn() == SqlAgg.Fn.MAX || r.fn() == SqlAgg.Fn.MIN) && r.args().size() == 1
                && r.orderBy().isEmpty() && isBoolean(r.args().get(0))) {
            return (r.fn() == SqlAgg.Fn.MAX ? "bool_or(" : "bool_and(") + expr(r.args().get(0), 0) + ")";
        }
        // nor over jsonb (measured: "function max(jsonb) does not exist"), though jsonb is ordered: the
        // first of the values in that order, NULLs last, is max/min exactly -- NULL only when all are.
        // A Variant column (json read as jsonb, StoredReads) reaches this in DataCube's one-value columns.
        if ((r.fn() == SqlAgg.Fn.MAX || r.fn() == SqlAgg.Fn.MIN) && r.args().size() == 1
                && r.orderBy().isEmpty() && isJson(r.args().get(0))) {
            String x = expr(r.args().get(0), 0);
            return "(array_agg(" + x + " ORDER BY " + x + (r.fn() == SqlAgg.Fn.MAX ? " DESC" : " ASC") + " NULLS LAST))[1]";
        }
        return switch (r.fn()) {
            // ANY_VALUE is Postgres 16+; ordered aggregates keep the base's spelling —
            // Postgres' default null placement (ASC last, DESC first) IS the
            // reference's NULL-largest
            // (avg and the moments are a Float: AggregatesDeliverDouble casts them, windowCall in a window)
            case SUM, COUNT, AVG, MIN, MAX, ANY_VALUE, STDDEV_SAMP, STDDEV_POP, VAR_SAMP,
                 VAR_POP, STRING_AGG, CORR, COVAR_SAMP, COVAR_POP, BOOL_AND, BOOL_OR,
                 VARIANCE, STDDEV, ROW_NUMBER, RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST,
                 NTILE, LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE -> super.reducer(r);
            // DuckDB's median interpolates numbers; a median of anything else walls
            case MEDIAN -> {
                if (r.args().size() != 1 || r.distinct() || !r.orderBy().isEmpty()
                        || !isNumeric(r.args().get(0))) {
                    throw new DialectCapability("median of a non-number reached Postgres,"
                            + " whose percentile_cont interpolates numbers only");
                }
                yield "percentile_cont(0.5) WITHIN GROUP (ORDER BY "
                        + expr(r.args().get(0), 0) + ")";
            }
            case MODE -> {
                if (r.args().size() != 1 || r.distinct() || !r.orderBy().isEmpty()) {
                    throw new DialectCapability("a distinct or ordered mode reached Postgres");
                }
                yield "mode() WITHIN GROUP (ORDER BY " + expr(r.args().get(0), 0) + ")";
            }
            // the reducer's single order key IS the within-group order (H2's arm)
            case QUANTILE_CONT, QUANTILE_DISC -> {
                if (r.args().size() != 2 || r.distinct() || r.orderBy().size() > 1) {
                    throw new DialectCapability(r.fn() + " of this shape reached Postgres");
                }
                boolean desc = !r.orderBy().isEmpty() && !r.orderBy().get(0).ascending();
                yield (r.fn() == SqlAgg.Fn.QUANTILE_CONT ? "percentile_cont(" : "percentile_disc(")
                        + expr(r.args().get(1), 0) + ") WITHIN GROUP (ORDER BY "
                        + expr(r.args().get(0), 0) + (desc ? " DESC" : "") + ")";
            }
            case LIST -> throw new DialectCapability("a LIST aggregate reached Postgres before"
                    + " the jsonb collection carrier (leg P4)");
            // no arg_max/arg_min: the value at the first row in the key's order, rows of a NULL key
            // ignored, as DuckDB's arg_max/arg_min (a flat, internal array: never a carrier)
            case ARG_MAX, ARG_MIN -> {
                if (r.args().size() != 2 || r.distinct() || !r.orderBy().isEmpty()) {
                    throw new DialectCapability(r.fn() + " of this shape reached Postgres");
                }
                String key = expr(r.args().get(1), 0);
                yield "(array_agg(" + expr(r.args().get(0), 0) + " ORDER BY " + key
                        + (r.fn() == SqlAgg.Fn.ARG_MAX ? " DESC" : " ASC") + ") FILTER (WHERE " + key
                        + " IS NOT NULL))[1]";
            }
            case WAVG, HASH_LIST, IS_DISTINCT_MARK, UNIQUE_VALUE_ONLY ->
                    throw new IllegalStateException("lowering marker " + r.fn()
                            + " reached the renderer");
        };
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
    protected String windowCall(SqlExpr.WindowCall w) {
        if (w.fn() instanceof SqlAgg.Reducer r && java.util.EnumSet.of(SqlAgg.Fn.MEDIAN,
                SqlAgg.Fn.MODE, SqlAgg.Fn.QUANTILE_CONT, SqlAgg.Fn.QUANTILE_DISC).contains(r.fn())) {
            throw new DialectCapability(r.fn() + " over a window reached Postgres, where"
                    + " ordered-set aggregates take no OVER clause");
        }
        // a windowed avg or moment is numeric here too, and the pass sees only grouped ones
        return w.fn() instanceof SqlAgg.Reducer r && DOUBLE_AGGREGATES.contains(r.fn())
                ? "CAST(" + super.windowCall(w) + " AS DOUBLE PRECISION)" : super.windowCall(w);
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
    protected String jsonObject(SqlExpr.JsonObject j) {
        return "json_build_object(" + list(j.kv()) + ")";
    }

    @Override
    protected String jsonArray(SqlExpr.JsonArray j) {
        return "json_build_array(" + list(j.elements()) + ")";
    }

    /** json_agg keeps NULL elements (DuckDB's json_group_array does too); zero rows
     * aggregate to NULL, so the empty array is coalesced in. */
    @Override
    protected String jsonArrayAgg(SqlExpr.JsonArrayAgg j) {
        return "coalesce(json_agg(" + expr(j.value(), 0)
                + (j.orderKeys().isEmpty() ? "" : " ORDER BY " + j.orderKeys().stream()
                        .map(k -> expr(k.expr(), 0) + (k.desc() ? " DESC" : " ASC") + " NULLS LAST")
                        .collect(Collectors.joining(", ")))
                + "), '[]')";
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
                    case SUBSEC_MIN -> throw new DialectCapability("a minimal-fraction date"
                            + " format reached Postgres, whose to_char has no trim-zeros code");
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
    // DDL (the JVM lane: test fixtures; never on the product path)
    // ==================================================================

    /** Postgres has no TINYINT, BIT or bare DOUBLE; FLOAT is DOUBLE PRECISION as on H2. */
    @Override
    protected String ddlType(com.legend.sql.SqlDdl.ColumnType t) {
        if (t instanceof com.legend.sql.SqlDdl.ColumnType.Plain p) {
            return switch (p.kind()) {
                case TINYINT -> "SMALLINT";
                case FLOAT, DOUBLE -> "DOUBLE PRECISION";
                case BIT -> "BOOLEAN";
                case JSON -> "JSONB";
                case BIGINT, SMALLINT, INTEGER, REAL, TIMESTAMP, DATE, VARCHAR, OTHER, DISTINCT,
                     ARRAY, OBJECT -> super.ddlType(t);
            };
        }
        return super.ddlType(t);
    }

    /** Table names quote like the query's table references. */
    @Override
    protected String ddlQualified(@com.legend.base.Nullable String schema, String table) {
        return schema == null || schema.isEmpty() || "default".equals(schema)
                ? ident(table) : ident(schema) + "." + ident(table);
    }

    /** Schema names quote like the query's schema-qualified references. */
    @Override
    public String render(com.legend.sql.SqlDdl ddl) {
        if (ddl instanceof com.legend.sql.SqlDdl.CreateSchema cs) {
            return "Create Schema if not exists " + ident(cs.schema()) + ";";
        }
        if (ddl instanceof com.legend.sql.SqlDdl.DropSchema ds) {
            return "Drop schema if exists " + ident(ds.schema()) + " cascade;";
        }
        return super.render(ddl);
    }
}
