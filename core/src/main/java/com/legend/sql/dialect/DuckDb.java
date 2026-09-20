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

    public DuckDb() {
        super(Lexicon.DUCKDB, TypeNames.DUCKDB, Spellings.DUCKDB);
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
        // Still unconditional — but a name may already ARRIVE quoted (a
        // relational identifier keeps its quotes as the wire name), and
        // wrapping that again gave `ON ""製品""`, a zero-length
        // delimited identifier DuckDB rejects. An already-quoted name
        // goes through ident(), which passes a clean one straight back
        // and re-spells a backslash-escaped one; everything else keeps
        // the unconditional wrap the corpus pins.
        sb.append(" ON ").append(p.on().stream()
                .map(e -> e instanceof SqlExpr.Column c
                        ? (c.name().length() > 1
                                && c.name().charAt(0) == quoteChar()
                                && c.name().charAt(c.name().length() - 1)
                                        == quoteChar()
                                ? ident(c.name())
                                : quoteChar() + c.name() + quoteChar())
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
        return fn("to_json", a);
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
        // INTEGER key renders as the array subscript — same extraction,
        // and the corpus greps for it.
        if (args.get(1) instanceof SqlExpr.IntLit i) {
            return "(" + expr(args.get(0), 7) + ")[" + i.value() + "]";
        }
        return "(" + expr(args.get(0), 7) + " -> " + expr(args.get(1), 8) + ")";
    }

    @Override
    protected String variantElements(List<SqlExpr> args) {
        return "CAST(" + expr(args.get(0), 0) + " AS JSON[])";
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
            String text = "(" + expr(call.args().get(0), 7) + " ->> "
                    + expr(call.args().get(1), 8) + ")";
            return "CAST(" + text + " AS " + castTypeName(c.target()) + ")";
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
