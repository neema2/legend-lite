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

    /**
     * JSON cells normalize to pure's canonical COMPACT spelling. WATCHED
     * STAND-IN (audit pct2-a F2, deletion adjudicated by sweep: -3 —
     * wire cells that never pass the SQL {@code json()} channel need it):
     * whitespace-outside-strings only — no number-lexeme reprint, no
     * duplicate-key dedup (pure's Variant is a full Jackson reprint).
     * Payload fidelity probe-verified (pct2-b F8); the missing
     * canonicalizations have no fixture witnesses and fail LOUD
     * (string-exact grid compare) when they ever bite. A real reprint
     * needs a JSON reader in core — ledgered, not smuggled.
     */
    @Override
    public Object normalize(Object jdbcValue, com.legend.sql.SqlType type) {
        if (type == com.legend.sql.SqlType.Scalar.JSON && jdbcValue != null) {
            return compactJson(jdbcValue.toString());
        }
        return jdbcValue;
    }

    private static String compactJson(String json) {
        StringBuilder out = new StringBuilder(json.length());
        boolean inString = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (inString) {
                out.append(c);
                if (c == '\\' && i + 1 < json.length()) {
                    out.append(json.charAt(++i));
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            if (c == '"') {
                inString = true;
                out.append(c);
                continue;
            }
            if (!Character.isWhitespace(c)) {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static final Set<String> RESERVED = Set.of(
            "all", "and", "as", "asc", "between", "by", "case", "cast", "create", "cross",
            "default", "delete", "desc", "distinct", "drop", "else", "end", "except", "exists",
            "false", "from", "full", "group", "having", "in", "inner", "insert", "intersect",
            "into", "is", "join", "left", "like", "limit", "not", "null", "offset", "on", "or",
            "order", "outer", "pivot", "qualify", "right", "select", "table", "then", "true",
            "union", "update", "using", "values", "when", "where", "window", "with");

    @Override
    protected Set<String> reservedWords() {
        return RESERVED;
    }

    // ---- structural capabilities ----

    @Override
    protected boolean supportsQualify() {
        return true;
    }

    @Override
    protected void appendQualify(StringBuilder sb, SqlSelect s, int depth) {
        nl(sb, depth).append("QUALIFY ").append(expr(s.qualify(), 0));
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
        sb.append(" ON ").append(p.on().stream()
                .map(e -> unqualify(e) instanceof SqlExpr.Column c
                        ? quoteChar() + c.name() + quoteChar()
                        : expr(unqualify(e), 0))
                .collect(Collectors.joining(", ")));
        if (!p.in().isEmpty()) {
            sb.append(" IN (").append(p.in().stream()
                    .map(e -> expr(e, 0))
                    .collect(Collectors.joining(", "))).append(")");
        }
        sb.append(" USING ").append(p.usings().stream()
                .map(u -> reducer(new SqlAgg.Reducer(u.agg().fn(),
                        u.agg().args().stream().map(DuckDb::unqualify).toList(),
                        u.agg().distinct()))
                        // real pure names pivot columns value__|__agg; DuckDB
                        // joins value + '_' + alias, so the alias carries the
                        // '_|__agg' tail.
                        + " AS " + ident("_|__" + u.alias()))
                .collect(Collectors.joining(", ")));
        sb.append(") AS ").append(ident(p.alias()));
    }

    private static SqlExpr unqualify(SqlExpr e) {
        return switch (e) {
            case SqlExpr.Column c -> new SqlExpr.Column(null, c.name());
            case SqlExpr.Call c -> new SqlExpr.Call(c.fn(),
                    c.args().stream().map(DuckDb::unqualify).toList());
            case SqlExpr.Cast c -> new SqlExpr.Cast(unqualify(c.value()), c.target());
            case SqlExpr.Case c -> new SqlExpr.Case(
                    c.whens().stream().map(w -> new SqlExpr.Case.When(
                            unqualify(w.condition()), unqualify(w.then()))).toList(),
                    c.otherwise() == null ? null : unqualify(c.otherwise()));
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(
                    a.elements().stream().map(DuckDb::unqualify).toList());
            case SqlExpr.StructGet g ->
                    new SqlExpr.StructGet(unqualify(g.source()), g.field());
            case SqlExpr.StructLit sl -> new SqlExpr.StructLit(
                    sl.fields().stream().map(f -> new SqlExpr.StructLit.Field(
                            f.name(), unqualify(f.value()))).toList());
            case SqlExpr.OrderedListAgg o -> new SqlExpr.OrderedListAgg(
                    unqualify(o.value()), unqualify(o.orderBy()));
            case SqlExpr.JsonObject j -> new SqlExpr.JsonObject(
                    j.kv().stream().map(DuckDb::unqualify).toList());
            case SqlExpr.JsonArrayAgg ja ->
                    new SqlExpr.JsonArrayAgg(unqualify(ja.value()));
            case SqlAgg.Reducer r -> new SqlAgg.Reducer(r.fn(),
                    r.args().stream().map(DuckDb::unqualify).toList(),
                    r.distinct());
            case SqlExpr.FoldCall f -> new SqlExpr.FoldCall(
                    unqualify(f.source()), f.lambda(), unqualify(f.init()),
                    f.accIsList(), f.homogeneous());
            case SqlExpr.WindowCall w -> new SqlExpr.WindowCall(w.fn(),
                    w.partitionBy().stream().map(DuckDb::unqualify).toList(),
                    w.orderBy(), w.frame());
            // subqueries own their aliases; lambdas bind their own params;
            // leaves carry no qualifier (audit 15: exhaustive, no default)
            case SqlExpr.Exists x -> x;
            case SqlExpr.ScalarSubquery ss -> ss;
            case SqlExpr.Lambda l -> l;
            case SqlExpr.Star st -> st;
            case SqlExpr.StarExcept se -> se;
            case SqlExpr.StringLit ignored -> e;
            case SqlExpr.IntLit ignored -> e;
            case SqlExpr.FloatLit ignored -> e;
            case SqlExpr.DecimalLit ignored -> e;
            case SqlExpr.BoolLit ignored -> e;
            case SqlExpr.NullLit ignored -> e;
            case SqlExpr.DateLit ignored -> e;
            case SqlExpr.TimestampLit ignored -> e;
        };
    }

    // ---- list idioms: DuckDB is the lambda backend ----

    @Override
    protected String lambda(SqlExpr.Lambda l) {
        return (l.params().size() == 1
                ? l.params().get(0)
                : "(" + String.join(", ", l.params()) + ")") + " -> " + expr(l.body(), 0);
    }

    /**
     * {@code list_reduce} encoding. Pure's lambda is {@code (element, acc)};
     * DuckDB's is {@code (acc, element)} — the parameters SWAP here, at the
     * one place that knows DuckDB's convention. {@code list_reduce} demands
     * init type == list child type: a LIST accumulator wraps each element as
     * a single-item list and unwraps refs in the body (master's Path 4); a
     * scalar accumulator over a non-decomposed body cannot be encoded.
     */
    @Override
    protected String foldCall(SqlExpr.FoldCall f) {
        String elem = f.lambda().params().get(0);
        String acc = f.lambda().params().get(1);
        if (!f.accIsList()) {
            // DuckDB's list_reduce demands init type == list child type; a
            // scalar accumulator over HETEROGENEOUS elements cannot be
            // encoded here — DuckDB's limitation, stated by DuckDB's dialect.
            if (!f.homogeneous()) {
                throw new IllegalStateException("fold body is not decomposable and the"
                        + " accumulator is scalar — rewrite accumulator-first"
                        + " ({e, a | $a <op> ...}) so the reduction can decompose");
            }
            SqlExpr.Lambda swapped = new SqlExpr.Lambda(List.of(acc, elem), f.lambda().body());
            // fold over the EMPTY (SQL NULL) collection is the INIT value —
            // list_reduce(NULL, ...) is NULL
            return fn("list_reduce", List.of(
                    SqlExpr.Call.of(com.legend.sql.SqlFn.COALESCE, f.source(),
                            new SqlExpr.ArrayLit(List.of())),
                    swapped, f.init()));
        }
        // List accumulator: wrap elements as single-item lists ([e] — the
        // semantic ArrayLit), unwrap refs via LIST_GET(e, 1).
        SqlExpr wrapped = new SqlExpr.Call(SqlFn.LIST_TRANSFORM, List.of(f.source(),
                new SqlExpr.Lambda(List.of(elem),
                        new SqlExpr.ArrayLit(List.of(new SqlExpr.Column(null, elem))))));
        SqlExpr body = unwrapElemRefs(f.lambda().body(), elem);
        SqlExpr.Lambda swapped = new SqlExpr.Lambda(List.of(acc, elem), body);
        return fn("list_reduce", List.of(wrapped, swapped, f.init()));
    }

    /**
     * Replace bare refs to {@code elem} with {@code LIST_GET(elem, 1)} —
     * EXHAUSTIVE over the expression tree (a ref nested under a cast, case,
     * array, or inner lambda must unwrap too; javac keeps this honest).
     */
    private SqlExpr unwrapElemRefs(SqlExpr e, String elem) {
        return switch (e) {
            case SqlExpr.Column c when c.table() == null && elem.equals(c.name()) ->
                    SqlExpr.Call.of(SqlFn.LIST_GET,
                            new SqlExpr.Column(null, elem), new SqlExpr.IntLit(1));
            case SqlExpr.Column c -> c;
            case SqlExpr.StarExcept se -> se;
            case SqlExpr.OrderedListAgg ola -> ola;   // no elem refs inside
            case SqlExpr.Call call -> new SqlExpr.Call(call.fn(),
                    call.args().stream().map(x -> unwrapElemRefs(x, elem)).toList());
            case SqlExpr.Cast c -> new SqlExpr.Cast(unwrapElemRefs(c.value(), elem),
                    c.target());
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(
                    a.elements().stream().map(x -> unwrapElemRefs(x, elem)).toList());
            case SqlExpr.Case cs -> new SqlExpr.Case(
                    cs.whens().stream().map(w -> new SqlExpr.Case.When(
                            unwrapElemRefs(w.condition(), elem),
                            unwrapElemRefs(w.then(), elem))).toList(),
                    cs.otherwise() == null ? null : unwrapElemRefs(cs.otherwise(), elem));
            case SqlExpr.StructLit s -> new SqlExpr.StructLit(s.fields().stream()
                    .map(fld -> new SqlExpr.StructLit.Field(fld.name(),
                            unwrapElemRefs(fld.value(), elem))).toList());
            case SqlExpr.StructGet g -> new SqlExpr.StructGet(
                    unwrapElemRefs(g.source(), elem), g.field());
            case SqlExpr.Lambda l -> l.params().contains(elem)
                    ? l   // inner lambda shadows the element name
                    : new SqlExpr.Lambda(l.params(), unwrapElemRefs(l.body(), elem));
            case SqlExpr.FoldCall f -> new SqlExpr.FoldCall(
                    unwrapElemRefs(f.source(), elem),
                    f.lambda().params().contains(elem) ? f.lambda()
                            : new SqlExpr.Lambda(f.lambda().params(),
                                    unwrapElemRefs(f.lambda().body(), elem)),
                    unwrapElemRefs(f.init(), elem), f.accIsList(), f.homogeneous());
            // Leaves and structures that cannot contain the element ref:
            case SqlExpr.Star st -> st;
            case SqlExpr.StringLit v -> v;
            case SqlExpr.IntLit v -> v;
            case SqlExpr.FloatLit v -> v;
            case SqlExpr.DecimalLit v -> v;
            case SqlExpr.BoolLit v -> v;
            case SqlExpr.NullLit v -> v;
            case SqlExpr.DateLit v -> v;
            case SqlExpr.TimestampLit v -> v;
            case SqlExpr.Exists x -> x;
            case SqlExpr.ScalarSubquery x -> x;
            case SqlExpr.WindowCall w -> w;
            // JSON envelope nodes never appear inside fold bodies (the
            // serialize envelope is a projection-level construct).
            case SqlExpr.JsonObject j -> j;
            case SqlExpr.JsonArrayAgg j -> j;
            case com.legend.sql.SqlAgg.Reducer r -> r;
        };
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
            String path = java.net.URI.create(url).getPath();
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
            case LIST_CONTAINS -> fn("list_contains", args);
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
            // Generic list aggregation: the AGG NAME rides as a leading
            // string-literal arg (the EXTRACT part-name pattern).
            case LIST_AGG -> "list_aggregate(" + expr(args.get(1), 0) + ", "
                    + expr(args.get(0), 0)
                    + args.subList(2, args.size()).stream()
                            .map(x -> ", " + expr(x, 0))
                            .collect(java.util.stream.Collectors.joining())
                    + ")";
            case LIST_TAIL -> expr(args.get(0), 8) + "[2:]";
            case LIST_INIT -> expr(args.get(0), 8) + "[:-2]";
            case RANGE_FN -> fn("range", args);
            default -> throw new IllegalStateException("not a list call: " + fnName);
        };
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
        return "{" + s.fields().stream()
                .map(f -> "'" + f.name() + "': " + expr(f.value(), 0))
                .collect(java.util.stream.Collectors.joining(", ")) + "}";
    }

    @Override
    protected String structGet(SqlExpr.StructGet g) {
        return "struct_extract(" + expr(g.source(), 0) + ", '" + g.field() + "')";
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

    @Override
    protected String castTypeName(com.legend.sql.SqlType t) {
        return switch (t) {
            case com.legend.sql.SqlType.Scalar s -> switch (s) {
                case DOUBLE -> "DOUBLE";
                case JSON -> "JSON";
                default -> super.castTypeName(t);
            };
            case com.legend.sql.SqlType.Struct s -> "STRUCT(" + s.fields().stream()
                    .map(f -> ident(f.name()) + " " + castTypeName(f.type()))
                    .collect(java.util.stream.Collectors.joining(", ")) + ")";
            default -> super.castTypeName(t);
        };
    }

    // ---- raw-SQL adaptation (the K-native executeInDb boundary) ----

    private static final java.util.regex.Pattern CREATE_HEAD = java.util.regex.Pattern.compile(
            "(?i)^\\s*create\\s+table\\s+[\\w.\"]+\\s*\\(");

    private static final java.util.regex.Pattern INSERT_COLS = java.util.regex.Pattern.compile(
            "(?i)^(\\s*insert\\s+into\\s+[\\w.\"]+\\s*\\()([^)]*)(\\))");

    /**
     * The engine test corpus writes H2-flavored raw DDL/DML: SQL-keyword
     * column names ({@code default, do, else, ...}) UNQUOTED — legal on H2,
     * a syntax error here — and {@code CURRENT_TIMESTAMP()} with parens.
     * Quote every column identifier in CREATE TABLE / INSERT column lists
     * (constraint entries pass through) and drop the parens; everything
     * else is untouched.
     */
    @Override
    public String adaptRawSql(String sql) {
        String out = sql.replaceAll("(?i)\\bCURRENT_TIMESTAMP\\(\\)", "CURRENT_TIMESTAMP");
        java.util.regex.Matcher cm = CREATE_HEAD.matcher(out);
        if (cm.find()) {
            return quoteCreateColumns(out, cm.end());
        }
        java.util.regex.Matcher m = INSERT_COLS.matcher(out);
        if (!m.find()) {
            return out;
        }
        StringBuilder cols = new StringBuilder();
        for (String c : m.group(2).split(",")) {
            if (cols.length() > 0) {
                cols.append(", ");
            }
            String name = c.strip();
            cols.append(name.startsWith("\"") ? name : "\"" + name + "\"");
        }
        return m.group(1) + cols + m.group(3) + out.substring(m.end(3));
    }

    /**
     * Quote the column NAME of each top-level column definition in a
     * CREATE TABLE literal (constraint entries — PRIMARY KEY(...) etc —
     * pass through). The type part maps H2's FLOAT (an 8-byte double) to
     * DOUBLE (DuckDB's FLOAT is REAL) — on the TYPE PART only: a
     * whole-statement replace once renamed a column literally named
     * "float" (relationalSetUp testTable).
     */
    private static String quoteCreateColumns(String sql, int bodyStart) {
        int depth = 1;
        int end = bodyStart;
        while (end < sql.length() && depth > 0) {
            char c = sql.charAt(end);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            end++;
        }
        String body = sql.substring(bodyStart, end - 1);
        java.util.List<String> parts = new java.util.ArrayList<>();
        int d = 0;
        int start = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '(') {
                d++;
            } else if (c == ')') {
                d--;
            } else if (c == ',' && d == 0) {
                parts.add(body.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(body.substring(start));
        StringBuilder out = new StringBuilder();
        for (String part : parts) {
            String col = part.strip();
            if (out.length() > 0) {
                out.append(", ");
            }
            int sp = 0;
            while (sp < col.length() && !Character.isWhitespace(col.charAt(sp))
                    && col.charAt(sp) != '(') {
                sp++;
            }
            String head = col.substring(0, sp);
            if (col.startsWith("\"")) {
                // pre-quoted name (model-derived DDL quotes fully): the
                // TYPE PART still needs the H2->DuckDB kind mapping
                int endq = col.indexOf('"', 1);
                out.append(col, 0, endq + 1)
                        .append(col.substring(endq + 1)
                                .replaceAll("(?i)\\bFLOAT\\b", "DOUBLE")
                                .replaceAll("(?i)\\bBIT\\b", "BOOLEAN"));
            } else if (head.matches("(?i)primary|constraint|foreign|unique|check")) {
                out.append(col);
            } else {
                // H2 semantics on the TYPE PART only: FLOAT is an 8-byte
                // double; BIT is a boolean (DuckDB's BIT is a bitstring)
                out.append('\"').append(head).append('\"').append(
                        col.substring(sp).replaceAll("(?i)\\bFLOAT\\b", "DOUBLE")
                                .replaceAll("(?i)\\bBIT\\b", "BOOLEAN"));
            }
        }
        return sql.substring(0, bodyStart) + out + sql.substring(end - 1);
    }
}
