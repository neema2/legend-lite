package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * The ANSI-standard renderer — the base every dialect extends
 * (PHASE_HIJ_LOWERING.md "Dialect architecture"). The IR carries MEANING with
 * Pure conventions; this class renders everything standard SQL can express
 * and exposes GROUPED extension points for the rest:
 *
 * <ul>
 *   <li><b>Lexical</b> — {@link #reservedWords()}, {@link #quoteChar()},
 *       literal forms, {@link #castTypeName}.</li>
 *   <li><b>Idioms</b> — list operations ({@link #foldCall},
 *       {@link #listExists}, {@link #listCall}), variant access
 *       ({@link #variantGet}, {@link #variantCast}), {@link #lambda}.
 *       The base THROWS for these: there is no ANSI spelling, and a silent
 *       approximation is forbidden (the no-fallback rule per dialect).</li>
 *   <li><b>Structural</b> — {@link #appendQualify} (native clause or
 *       self-wrap), {@link #pivotSource}, {@link #asOfJoinClause},
 *       {@link #valuesSource}.</li>
 * </ul>
 *
 * <p>Every {@code throw} below is a capability statement, not a TODO: a
 * dialect that cannot express a construct fails LOUDLY at render time.
 */
public abstract class AnsiSqlRenderer implements SqlDialect {

    private static final Pattern PLAIN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Infix operators: semantic entry → (sql, precedence). Higher binds tighter. */
    protected record Infix(String sql, int prec) {
    }

    private static final Map<SqlFn, Infix> INFIX = Map.ofEntries(
            Map.entry(SqlFn.OR, new Infix("OR", 1)),
            Map.entry(SqlFn.AND, new Infix("AND", 2)),
            Map.entry(SqlFn.EQUAL, new Infix("=", 4)),
            Map.entry(SqlFn.NOT_EQUAL, new Infix("<>", 4)),
            Map.entry(SqlFn.LESS, new Infix("<", 4)),
            Map.entry(SqlFn.LESS_EQUAL, new Infix("<=", 4)),
            Map.entry(SqlFn.GREATER, new Infix(">", 4)),
            Map.entry(SqlFn.GREATER_EQUAL, new Infix(">=", 4)),
            Map.entry(SqlFn.PLUS, new Infix("+", 5)),
            Map.entry(SqlFn.MINUS, new Infix("-", 5)),
            Map.entry(SqlFn.CONCAT, new Infix("||", 5)),
            Map.entry(SqlFn.TIMES, new Infix("*", 6)));

    @Override
    public String render(SqlQuery query) {
        StringBuilder sb = new StringBuilder();
        query(sb, query, 0);
        return sb.toString();
    }

    // ==================================================================
    // Queries and clause assembly
    // ==================================================================

    protected void query(StringBuilder sb, SqlQuery q, int depth) {
        switch (q) {
            case SqlSelect s -> select(sb, s, depth);
            case SqlUnion u -> {
                String op = u.all() ? "UNION ALL" : "UNION";
                for (int i = 0; i < u.branches().size(); i++) {
                    if (i > 0) {
                        nl(sb, depth).append(op);
                        nl(sb, depth);
                    }
                    query(sb, u.branches().get(i), depth);
                }
            }
        }
    }

    protected void select(StringBuilder sb, SqlSelect s, int depth) {
        if (s.qualify() != null && !supportsQualify()) {
            // Structural capability: filter-over-window-results without a
            // QUALIFY clause = the select wraps ITSELF.
            SqlSelect inner = s.withQualify(null);
            sb.append("SELECT *");
            nl(sb, depth).append("FROM (");
            nl(sb, depth + 1);
            select(sb, inner, depth + 1);
            nl(sb, depth).append(") AS qualify_src");
            nl(sb, depth).append("WHERE ").append(expr(s.qualify(), 0));
            return;
        }
        sb.append("SELECT ");
        if (s.distinct()) {
            sb.append("DISTINCT ");
        }
        sb.append(s.projections().isEmpty()
                ? "*"
                : s.projections().stream().map(this::projection).collect(Collectors.joining(", ")));
        if (s.from() != null) {
            nl(sb, depth).append("FROM ");
            source(sb, s.from(), depth);
        }
        if (s.where() != null) {
            nl(sb, depth).append("WHERE ").append(expr(s.where(), 0));
        }
        if (!s.groupBy().isEmpty()) {
            nl(sb, depth).append("GROUP BY ")
                    .append(s.groupBy().stream().map(e -> expr(e, 0)).collect(Collectors.joining(", ")));
        }
        if (s.having() != null) {
            nl(sb, depth).append("HAVING ").append(expr(s.having(), 0));
        }
        if (s.qualify() != null) {
            appendQualify(sb, s, depth);
        }
        if (!s.orderBy().isEmpty()) {
            nl(sb, depth).append("ORDER BY ")
                    .append(s.orderBy().stream().map(this::sortKey).collect(Collectors.joining(", ")));
        }
        if (s.limit() != null) {
            nl(sb, depth).append("LIMIT ").append(s.limit());
        }
        if (s.offset() != null) {
            nl(sb, depth).append("OFFSET ").append(s.offset());
        }
    }

    /**
     * Render a {@code sourceUrl} into a complete SELECT (scheme-dispatched).
     * No ANSI spelling exists — the base is a capability statement.
     */
    protected String sourceUrl(String url) {
        throw new IllegalStateException("sourceUrl reached a dialect without"
                + " an external-source encoding: " + url);
    }

    /** Whether this dialect has a native QUALIFY clause. ANSI does not. */
    protected boolean supportsQualify() {
        return false;
    }

    /** Emit the native QUALIFY clause (only called when {@link #supportsQualify()}). */
    protected void appendQualify(StringBuilder sb, SqlSelect s, int depth) {
        throw new IllegalStateException("QUALIFY reached a dialect without native support");
    }

    protected String projection(SqlSelect.Projection p) {
        String e = expr(p.expr(), 0);
        return p.alias() == null ? e : e + " AS " + ident(p.alias());
    }

    protected String sortKey(SqlSelect.SortKey k) {
        String s = expr(k.expr(), 0) + (k.ascending() ? "" : " DESC");
        if (k.nullOrder() != null) {
            s += k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                    ? " NULLS FIRST" : " NULLS LAST";
        }
        return s;
    }

    // ==================================================================
    // Sources
    // ==================================================================

    protected void source(StringBuilder sb, SqlSource src, int depth) {
        switch (src) {
            case SqlSource.Table t -> {
                sb.append(tableName(t.name()));
                if (t.alias() != null) {
                    sb.append(" AS ").append(ident(t.alias()));
                }
            }
            case SqlSource.Subselect sub -> {
                sb.append("(");
                nl(sb, depth + 1);
                query(sb, sub.inner(), depth + 1);
                nl(sb, depth).append(") AS ").append(ident(sub.alias()));
            }
            case SqlSource.Values v -> valuesSource(sb, v);
            case SqlSource.SourceUrl u -> {
                sb.append("(");
                nl(sb, depth + 1).append(sourceUrl(u.url()));
                nl(sb, depth).append(") AS ").append(ident(u.alias()));
            }
            case SqlSource.Pivot p -> pivotSource(sb, p, depth);
            case SqlSource.Join j -> {
                source(sb, j.left(), depth);
                nl(sb, depth);
                if (j.kind() == SqlSource.Join.Kind.ASOF_LEFT) {
                    sb.append(asOfJoinClause());
                } else {
                    sb.append(j.kind().sql);
                }
                sb.append(" ");
                source(sb, j.right(), depth);
                if (j.on() != null) {
                    sb.append(" ON ").append(expr(j.on(), 0));
                }
            }
        }
    }

    /** ANSI row-constructor VALUES with column aliases; SQLite overrides (UNION ALL). */
    protected void valuesSource(StringBuilder sb, SqlSource.Values v) {
        sb.append("(VALUES ")
                .append(v.rows().stream()
                        .map(row -> "(" + row.stream().map(e -> expr(e, 0))
                                .collect(Collectors.joining(", ")) + ")")
                        .collect(Collectors.joining(", ")))
                .append(") AS ").append(ident(v.alias()))
                .append("(")
                .append(v.columns().stream().map(this::ident).collect(Collectors.joining(", ")))
                .append(")");
    }

    /** Native PIVOT or a CASE-WHEN aggregation rewrite — no ANSI form exists. */
    protected void pivotSource(StringBuilder sb, SqlSource.Pivot p, int depth) {
        throw new IllegalStateException("pivot reached a dialect without a PIVOT strategy");
    }

    /** The AS-OF join clause keyword(s); no ANSI form exists. */
    protected String asOfJoinClause() {
        throw new IllegalStateException("asOfJoin reached a dialect without an AS-OF strategy");
    }

    // ==================================================================
    // Expressions
    // ==================================================================

    protected String expr(SqlExpr e, int parentPrec) {
        return switch (e) {
            case SqlExpr.Column c -> c.table() == null
                    ? ident(c.name()) : ident(c.table()) + "." + ident(c.name());
            case SqlExpr.Star s -> s.table() == null ? "*" : ident(s.table()) + ".*";
            // DuckDB's EXCLUDE spelling (the one PIVOT backend); the dropped
            // names quote UNCONDITIONALLY — the corpus pins the quoted form.
            case SqlExpr.StarExcept se -> (se.table() == null ? "*" : ident(se.table()) + ".*")
                    + " EXCLUDE (" + se.except().stream()
                            .map(n -> quoteChar() + n + quoteChar())
                            .collect(java.util.stream.Collectors.joining(", ")) + ")";
            case SqlExpr.StringLit s -> stringLit(s.value());
            case SqlExpr.IntLit i -> String.valueOf(i.value());
            // pure Float IS float8 — a BARE decimal literal types as
            // DECIMAL(p,s) in DuckDB and infects every aggregate over it
            case SqlExpr.FloatLit f -> "CAST(" + f.value() + " AS DOUBLE)";
            case SqlExpr.DecimalLit d -> d.value().toPlainString();
            case SqlExpr.BoolLit b -> boolLit(b.value());
            case SqlExpr.NullLit n -> "NULL";
            case SqlExpr.DateLit d -> dateLit(d.iso());
            case SqlExpr.TimestampLit t -> timestampLit(t.iso());
            case SqlExpr.OrderedListAgg ola -> "list(" + expr(ola.value(), 0)
                    + " ORDER BY " + expr(ola.orderBy(), 0) + ")";
            case SqlExpr.ArrayLit a -> arrayLit(a.elements());
            case SqlExpr.StructLit s -> structLit(s);
            case SqlExpr.StructGet g -> structGet(g);
            case SqlExpr.Call c -> call(c, parentPrec);
            case SqlExpr.Case c -> caseExpr(c);
            case SqlExpr.Exists ex -> "EXISTS (" + inline(ex.subquery()) + ")";
            case SqlExpr.ScalarSubquery sq -> "(" + inline(sq.subquery()) + ")";
            case SqlExpr.WindowCall w -> windowCall(w);
            case SqlExpr.Lambda l -> lambda(l);
            case SqlExpr.Cast c -> variantAwareCast(c);
            case SqlExpr.FoldCall f -> foldCall(f);
            case SqlExpr.JsonObject j -> "json_object(" + j.kv().stream()
                    .map(kvE -> expr(kvE, 0)).collect(Collectors.joining(", ")) + ")";
            // COALESCE: an aggregate over ZERO rows is SQL NULL; the graph
            // contract says empty collection = the EMPTY ARRAY.
            case SqlExpr.JsonArrayAgg j -> "coalesce(json_group_array("
                    + expr(j.value(), 0) + "), '[]')";
            case SqlAgg.Reducer r -> reducer(r);
        };
    }

    /**
     * ONE exhaustive switch over the {@link SqlFn} vocabulary — javac fails a
     * dialect the moment a semantic function lacks a rendering decision.
     * ANSI-expressible entries render here; idiom entries delegate to the
     * dialect hooks (which THROW in this base).
     */
    protected String call(SqlExpr.Call c, int parentPrec) {
        Infix infix = INFIX.get(c.fn());
        if (infix != null) {
            // NON-COMMUTATIVE ops (-): trailing SAME-precedence operands
            // must parenthesize — 6 - (4 - 5) is not 6 - 4 - 5 (a real
            // wrong-answer bug PCT caught on the minus composition tests).
            boolean nonCommutative = c.fn() == SqlFn.MINUS;
            StringBuilder joined = new StringBuilder();
            for (int i = 0; i < c.args().size(); i++) {
                if (i > 0) {
                    joined.append(" ").append(infix.sql()).append(" ");
                }
                joined.append(expr(c.args().get(i),
                        i > 0 && nonCommutative ? infix.prec() + 1 : infix.prec()));
            }
            return infix.prec() < parentPrec ? "(" + joined + ")" : joined.toString();
        }
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            case AND, OR, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL,
                 PLUS, MINUS, TIMES, CONCAT ->
                    throw new IllegalStateException("infix operator fell through: " + c.fn());
            case NOT -> {
                String inner = "NOT " + expr(a.get(0), 3);
                yield 3 < parentPrec ? "(" + inner + ")" : inner;
            }
            case NEGATE -> "-" + expr(a.get(0), 7);
            case IS_NULL -> expr(a.get(0), 4) + " IS NULL";
            case IS_NOT_NULL -> expr(a.get(0), 4) + " IS NOT NULL";
            case IN -> expr(a.get(0), 4) + " IN (" + list(a.subList(1, a.size())) + ")";
            case IS_DISTINCT -> "(" + expr(a.get(0), 4) + " IS DISTINCT FROM "
                    + expr(a.get(1), 4) + ")";
            case STRFTIME -> fn("strftime", a);
            // MUST-honor semantics (PHASE_HIJ_LOWERING.md):
            // operands render ABOVE TIMES precedence: a composite child
            // ((2*t)/(1+p)) must parenthesize or SQL re-associates it
            case DIVIDE -> "((1.0 * " + expr(a.get(0), 7) + ") / " + expr(a.get(1), 7) + ")";
            case MOD -> "MOD(MOD(" + expr(a.get(0), 0) + ", " + expr(a.get(1), 0) + ") + "
                    + expr(a.get(1), 0) + ", " + expr(a.get(1), 0) + ")";
            case REM -> "MOD(" + expr(a.get(0), 0) + ", " + expr(a.get(1), 0) + ")";
            case ABS -> fn("abs", a);
            case LENGTH -> fn("length", a);
            case UPPER -> fn("upper", a);
            case LOWER -> fn("lower", a);
            case COALESCE -> fn("coalesce", a);
            case GREATEST -> fn("greatest", a);
            case LEAST -> fn("least", a);
            // Math — ANSI/portable spellings; ROUND is banker's (dialect maps).
            case SQRT -> fn("sqrt", a);
            case CBRT -> fn("cbrt", a);
            case EXP -> fn("exp", a);
            case LN -> fn("ln", a);
            case LOG10 -> fn("log10", a);
            case POW -> fn("power", a);
            case PI -> "pi()";
            case SIN -> fn("sin", a);
            case COS -> fn("cos", a);
            case TAN -> fn("tan", a);
            case ASIN -> fn("asin", a);
            case ACOS -> fn("acos", a);
            case ATAN -> fn("atan", a);
            case ATAN2 -> fn("atan2", a);
            case SINH -> fn("sinh", a);
            case COSH -> fn("cosh", a);
            case TANH -> fn("tanh", a);
            case CEILING -> "CAST(ceil(" + expr(a.get(0), 0) + ") AS BIGINT)";
            case FLOOR -> "CAST(floor(" + expr(a.get(0), 0) + ") AS BIGINT)";
            case ROUND -> roundHalfEven(a);
            // Pure's divide-with-scale is BigDecimal HALF_UP — plain SQL
            // ROUND (half away from zero) says exactly that.
            case ROUND_HALF_UP -> fn("ROUND", a);
            // Runtime assertion: raises with the message when evaluated
            // (guards that must fail LOUD, never clamp).
            case ERROR -> fn("error", a);
            // floor WITHOUT the BIGINT cast (FLOOR casts — overflows at
            // 1e18): fraction-free tests over the full double range.
            case FLOOR_RAW -> fn("floor", a);
            case SIGN -> "CAST(sign(" + expr(a.get(0), 0) + ") AS BIGINT)";
            case XOR -> {
                String x = expr(a.get(0), 3);
                String y = expr(a.get(1), 3);
                yield "(" + x + " AND NOT " + y + ") OR (NOT " + x + " AND " + y + ")";
            }
            case BIT_AND, BIT_OR, BIT_XOR, BIT_SHIFT_LEFT, BIT_SHIFT_RIGHT -> bitOp(c.fn(), a);
            // Strings
            case SUBSTRING -> fn("substr", a);
            case STRPOS -> fn("strpos", a);
            case STARTS_WITH -> fn("starts_with", a);
            case ENDS_WITH -> fn("ends_with", a);
            case MATCHES -> fn("regexp_matches", a);
            case REGEXP_EXTRACT_ALL -> fn("regexp_extract_all", a);
            case REGEXP_REPLACE -> fn("regexp_replace", a);
            case MAP_FROM_LISTS -> fn("map", a);
            case MAP_FROM_ENTRIES -> fn("map_from_entries", a);
            case MAP_EMPTY -> "MAP {}";
            case MAP_EXTRACT -> fn("map_extract", a);
            case MAP_KEYS -> fn("map_keys", a);
            case MAP_VALUES -> fn("map_values", a);
            case MAP_CONCAT -> fn("map_concat", a);
            case BIT_NOT -> "xor(" + expr(a.get(0), 0) + ", -1)";   // ~x without negation overflow at MIN_LONG
            case LEFT -> fn("left", a);
            case RIGHT -> fn("right", a);
            // the PAD CHAR is optional in Pure; SQL requires it — ' '.
            case LPAD -> fn("lpad", a.size() == 2
                    ? List.of(a.get(0), a.get(1), new SqlExpr.StringLit(" ")) : a);
            case RPAD -> fn("rpad", a.size() == 2
                    ? List.of(a.get(0), a.get(1), new SqlExpr.StringLit(" ")) : a);
            case TRIM -> fn("trim", a);
            case LTRIM -> fn("ltrim", a);
            case RTRIM -> fn("rtrim", a);
            case REPLACE -> fn("replace", a);
            case SPLIT -> fn("string_split", a);
            case SPLIT_PART -> fn("split_part", a);
            case REVERSE_STRING -> fn("reverse", a);
            case ASCII_CODE -> fn("ascii", a);
            case CHR -> fn("chr", a);
            case UC_FIRST -> "upper(substr(" + expr(a.get(0), 0) + ", 1, 1)) || substr("
                    + expr(a.get(0), 0) + ", 2)";
            case LC_FIRST -> "lower(substr(" + expr(a.get(0), 0) + ", 1, 1)) || substr("
                    + expr(a.get(0), 0) + ", 2)";
            case ENCODE_BASE64 -> "to_base64(CAST(" + expr(a.get(0), 0) + " AS BLOB))";
            case LEVENSHTEIN -> fn("levenshtein", a);
            case GUID -> "uuid()";
            case FORMAT -> fn("printf", a);
            case HASH -> fn("hash", a);
            case MD5 -> fn("md5", a);
            case SHA1 -> fn("sha1", a);
            case SHA256 -> fn("sha256", a);
            // Temporal
            case EXTRACT -> fn("date_part", a);
            case TODAY -> "current_date";
            case NOW -> "now()";
            case DATE_TRUNC_DAY -> "CAST(" + expr(a.get(0), 0) + " AS DATE)";
            case MAKE_DATE -> fn("make_date", a);
            // make_timestamp wants DOUBLE seconds.
            case MAKE_TIMESTAMP -> a.size() == 6
                    ? "make_timestamp(" + a.subList(0, 5).stream()
                            .map(x -> expr(x, 0)).collect(Collectors.joining(", "))
                            + ", CAST(" + expr(a.get(5), 0) + " AS DOUBLE))"
                    : fn("make_timestamp", a);
            case DATE_TRUNC -> fn("date_trunc", a);           // (part, value)
            // (unitFn literal, amount, date) — the unit FUNCTION NAME rides
            // as a string literal and renders bare: d + to_years(n).
            case ADD_INTERVAL -> expr(a.get(2), 5) + " + "
                    + ((SqlExpr.StringLit) a.get(0)).value()
                    + "(" + expr(a.get(1), 0) + ")";
            case DATE_DIFF -> fn("date_diff", a);              // (part, d1, d2)
            // Week buckets align to the Monday ON/BEFORE the epoch
            // (1969-12-29 — real pure's origin, PCT-pinned); every other
            // unit aligns to the 1970 epoch.
            case TIME_BUCKET -> "time_bucket("
                    + ((SqlExpr.StringLit) a.get(0)).value()
                    + "(" + expr(a.get(1), 0) + "), " + expr(a.get(2), 0)
                    + ("to_weeks".equals(((SqlExpr.StringLit) a.get(0)).value())
                            ? ", TIMESTAMP '1969-12-29 00:00:00'"
                            : ", TIMESTAMP '1970-01-01 00:00:00'")
                    + ")";
            case EPOCH_SECONDS -> fn("epoch", a);
            case EPOCH_MS -> fn("epoch_ms", a);
            case FROM_EPOCH_SECONDS -> fn("to_timestamp", a);
            case FROM_EPOCH_MS -> "epoch_ms(CAST(" + expr(a.get(0), 0) + " AS BIGINT))";
            case DAYNAME -> fn("dayname", a);
            case MONTHNAME -> fn("monthname", a);
            case COT -> fn("cot", a);
            case INT_DIVIDE -> "(" + expr(a.get(0), 6) + " // " + expr(a.get(1), 6) + ")";
            case RADIANS -> fn("radians", a);
            case DEGREES -> fn("degrees", a);
            case REPEAT_STR -> fn("repeat", a);
            case JARO_WINKLER -> fn("jaro_winkler_similarity", a);
            case DECODE_BASE64 -> "CAST(from_base64(" + expr(a.get(0), 0) + ") AS VARCHAR)";
            case CURRENT_USER_FN -> "current_user";
            case LIST_LENGTH -> fn("len", a);
            // Lists (dialect-owned; base throws like the lambda family)
            case LIST_ZIP, LIST_DISTINCT, LIST_APPEND, LIST_SUM, LIST_MIN, LIST_MAX,
                 LIST_AVG, LIST_MEDIAN, LIST_MODE, LIST_AGG, LIST_SORT,
                 LIST_SORT_DESC, LIST_TAIL, LIST_INIT, RANGE_FN,
                 LIST_PRODUCT, LIST_REDUCE, LIST_SLICE, LIST_BOOL_AND, LIST_BOOL_OR,
                 LIST_REVERSE, TYPEOF ->
                    listCall(c.fn(), a);
            case TO_VARIANT -> variantConstruct(a);
            // Idiom points — no ANSI spelling; the dialect decides or dies.
            case UNNEST -> unnestProjection(a);
            case LIST_FILTER, LIST_TRANSFORM, LIST_CONCAT, LIST_CONTAINS, LIST_GET,
                 LIST_POSITION ->
                    listCall(c.fn(), a);
            case LIST_EXISTS -> listExists(a);
            case LIST_FOR_ALL -> listForAll(a);
            case VARIANT_ELEMENTS -> variantElements(a);
            case VARIANT_GET -> variantGet(a);
        };
    }

    // ---- idiom extension points (base = capability statement, loud) ----

    /** Pure ROUND is HALF-EVEN (banker's) — every dialect must honor it. */
    protected String roundHalfEven(List<SqlExpr> a) {
        throw new IllegalStateException("banker's ROUND reached a dialect without a spelling");
    }

    protected String bitOp(SqlFn fnName, List<SqlExpr> a) {
        throw new IllegalStateException(fnName + " reached a dialect without bit-op support");
    }

    /** Construct a variant (JSON) value from any value. */
    protected String variantConstruct(List<SqlExpr> a) {
        throw new IllegalStateException("toVariant reached a dialect without JSON support");
    }

    /** Fold with PURE (element, accumulator) lambda; the encoding is the dialect's. */
    protected String foldCall(SqlExpr.FoldCall f) {
        throw new IllegalStateException("fold reached a dialect without a fold encoding");
    }

    /**
     * exists/forAll over a collection value. The expansion MUST honor Pure's
     * empty-collection semantics: {@code exists([]) = false},
     * {@code forAll([]) = true}.
     */
    protected String listExists(List<SqlExpr> args) {
        throw new IllegalStateException("collection exists reached a dialect"
                + " without a list-predicate encoding");
    }

    /** Contract includes Pure's empty-collection semantics: {@code forAll([]) = true}. */
    protected String listForAll(List<SqlExpr> args) {
        throw new IllegalStateException("collection forAll reached a dialect"
                + " without a list-predicate encoding");
    }

    /** map/filter/concat/contains over list values. */
    protected String listCall(SqlFn fn, List<SqlExpr> args) {
        throw new IllegalStateException(fn + " reached a dialect without a list encoding");
    }

    /** Explode a collection into rows, aligned with sibling projections. */
    protected String unnestProjection(List<SqlExpr> args) {
        throw new IllegalStateException("UNNEST reached a dialect without an unnest placement");
    }

    /** The elements of a variant (JSON) array value. */
    protected String variantElements(List<SqlExpr> args) {
        throw new IllegalStateException("variant navigation reached a dialect without JSON support");
    }

    /** JSON access ({@code v -> key}). */
    protected String variantGet(List<SqlExpr> args) {
        throw new IllegalStateException("variant navigation reached a dialect without JSON support");
    }

    /** Lambda expression — only dialects with lambda-capable functions render these. */
    protected String lambda(SqlExpr.Lambda l) {
        throw new IllegalStateException("a lambda reached a dialect without lambda support");
    }

    /**
     * CAST rendering; a dialect may route a variant-access value through its
     * text-extraction idiom first (DuckDB {@code ->>}). Base: plain CAST.
     */
    protected String variantAwareCast(SqlExpr.Cast c) {
        return "CAST(" + expr(c.value(), 0) + " AS "
                + castTypeName(c.target()) + ")";
    }

    // ---- window / aggregate / case (ANSI) ----

    protected String caseExpr(SqlExpr.Case c) {
        StringBuilder sb = new StringBuilder("CASE");
        for (SqlExpr.Case.When w : c.whens()) {
            sb.append(" WHEN ").append(expr(w.condition(), 0))
                    .append(" THEN ").append(expr(w.then(), 0));
        }
        if (c.otherwise() != null) {
            sb.append(" ELSE ").append(expr(c.otherwise(), 0));
        }
        return sb.append(" END").toString();
    }

    protected String windowCall(SqlExpr.WindowCall w) {
        String fnText = switch (w.fn()) {
            case SqlAgg.Reducer r -> reducer(r);
            case SqlAgg.RankingFn r -> r.fn() + "(" + list(r.args()) + ")";
            case SqlAgg.ValueFn v -> v.fn() + "(" + list(v.args()) + ")";
        };
        StringBuilder over = new StringBuilder();
        if (!w.partitionBy().isEmpty()) {
            over.append("PARTITION BY ").append(list(w.partitionBy()));
        }
        if (!w.orderBy().isEmpty()) {
            if (over.length() > 0) {
                over.append(" ");
            }
            over.append("ORDER BY ").append(w.orderBy().stream()
                    .map(this::sortKey).collect(Collectors.joining(", ")));
        }
        if (w.frame() != null) {
            over.append(" ").append(w.frame().kind()).append(" BETWEEN ")
                    .append(bound(w.frame().from())).append(" AND ").append(bound(w.frame().to()));
        }
        return fnText + " OVER (" + over + ")";
    }

    protected static String bound(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> "UNBOUNDED PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> p.n() + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> "CURRENT ROW";
            case SqlExpr.WindowCall.Frame.Bound.Following f -> f.n() + " FOLLOWING";
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> "UNBOUNDED FOLLOWING";
            // DuckDB interval spelling; DurationUnit names (DAYS, MONTHS...)
            // are valid interval units as-is.
            case SqlExpr.WindowCall.Frame.Bound.IntervalPreceding p ->
                    "INTERVAL " + p.n() + " " + p.unit() + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.IntervalFollowing f ->
                    "INTERVAL " + f.n() + " " + f.unit() + " FOLLOWING";
        };
    }

    protected String reducer(SqlAgg.Reducer r) {
        String args = r.args().isEmpty() ? "*" : list(r.args());
        return r.fn() + "(" + (r.distinct() ? "DISTINCT " : "") + args + ")";
    }

    // ==================================================================
    // Lexical extension points
    // ==================================================================

    /** Reserved words forcing quotes even when plainly spelled (lowercase). */
    protected abstract Set<String> reservedWords();

    protected char quoteChar() {
        return '"';
    }

    protected String stringLit(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    protected String boolLit(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    protected String dateLit(String iso) {
        return "DATE '" + iso + "'";
    }

    protected String timestampLit(String iso) {
        return "TIMESTAMP '" + iso + "'";
    }

    protected String arrayLit(List<SqlExpr> elements) {
        throw new IllegalStateException("an array literal reached a dialect without array support");
    }

    protected String structLit(SqlExpr.StructLit s) {
        throw new IllegalStateException("a struct literal reached a dialect without struct support");
    }

    protected String structGet(SqlExpr.StructGet g) {
        throw new IllegalStateException("a struct extraction reached a dialect without struct support");
    }

    /** SQL type → this dialect's CAST spelling (ANSI defaults). */
    protected String castTypeName(com.legend.sql.SqlType t) {
        return switch (t) {
            case com.legend.sql.SqlType.Scalar s -> switch (s) {
                case BOOLEAN -> "BOOLEAN";
                case BIGINT -> "BIGINT";
                case HUGEINT -> "HUGEINT";
                case TIMESTAMPTZ -> "TIMESTAMPTZ";
                case DOUBLE -> "DOUBLE PRECISION";
                case VARCHAR -> "VARCHAR";
                case DATE -> "DATE";
                case TIMESTAMP -> "TIMESTAMP";
                case JSON -> throw new IllegalStateException(
                        "JSON cast reached a dialect without JSON support");
            };
            case com.legend.sql.SqlType.Decimal d ->
                    "DECIMAL(" + d.precision() + ", " + d.scale() + ")";
            case com.legend.sql.SqlType.Array a -> castTypeName(a.element()) + "[]";
            case com.legend.sql.SqlType.Map m ->
                    "MAP(" + castTypeName(m.key()) + ", " + castTypeName(m.value()) + ")";
            case com.legend.sql.SqlType.Struct s -> throw new IllegalStateException(
                    "a STRUCT type reached a dialect without struct support");
        };
    }

    protected String fn(String spelling, List<SqlExpr> args) {
        return spelling + "(" + list(args) + ")";
    }

    protected String list(List<SqlExpr> es) {
        return es.stream().map(e -> expr(e, 0)).collect(Collectors.joining(", "));
    }

    /**
     * A subquery rendered inline (EXISTS / scalar position): SINGLE-LINE mode
     * — {@link #nl} emits a space instead of a newline while set. Structural,
     * never text post-processing (collapsing rendered text would corrupt
     * whitespace inside string LITERALS).
     */
    protected String inline(SqlQuery q) {
        boolean previous = inlineMode;
        inlineMode = true;
        try {
            StringBuilder sb = new StringBuilder();
            query(sb, q, 0);
            return sb.toString();
        } finally {
            inlineMode = previous;
        }
    }

    /** When set, clause separators render as single spaces (see {@link #inline}). */
    private boolean inlineMode;

    /** Quote ONLY when necessary (the lean tenet), per this dialect's rules. */
    /**
     * A table name may be schema-qualified (hr.EMPLOYEES): each part quotes
     * UNCONDITIONALLY — the engine's emission for schema tables, pinned by
     * the corpus ("hr"."EMPLOYEES").
     */
    protected String tableName(String name) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            return ident(name);
        }
        char q = quoteChar();
        return q + name.substring(0, dot) + q + "." + q + name.substring(dot + 1) + q;
    }

    protected String ident(String name) {
        if (PLAIN.matcher(name).matches() && !reservedWords().contains(name.toLowerCase())) {
            return name;
        }
        char q = quoteChar();
        return q + name.replace(String.valueOf(q), String.valueOf(q) + q) + q;
    }

    protected StringBuilder nl(StringBuilder sb, int depth) {
        return inlineMode ? sb.append(" ")
                : sb.append("\n").append("  ".repeat(depth));
    }
}
