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
        sb.append(" ON ").append(p.on().stream()
                .map(e -> expr(unqualify(e), 0)).collect(Collectors.joining(", ")));
        sb.append(" USING ").append(p.usings().stream()
                .map(u -> reducer(new SqlAgg.Reducer(u.agg().fn(),
                        u.agg().args().stream().map(DuckDb::unqualify).toList(),
                        u.agg().distinct())) + " AS " + ident(u.alias()))
                .collect(Collectors.joining(", ")));
        sb.append(") AS ").append(ident(p.alias()));
    }

    private static SqlExpr unqualify(SqlExpr e) {
        return e instanceof SqlExpr.Column c ? new SqlExpr.Column(null, c.name()) : e;
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
            return fn("list_reduce", List.of(f.source(), swapped, f.init()));
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

    /** Replace bare refs to {@code elem} with {@code LIST_GET(elem, 1)}. */
    private SqlExpr unwrapElemRefs(SqlExpr e, String elem) {
        if (e instanceof SqlExpr.Column c && c.table() == null && elem.equals(c.name())) {
            return SqlExpr.Call.of(SqlFn.LIST_GET,
                    new SqlExpr.Column(null, elem), new SqlExpr.IntLit(1));
        }
        if (e instanceof SqlExpr.Call call) {
            return new SqlExpr.Call(call.fn(),
                    call.args().stream().map(x -> unwrapElemRefs(x, elem)).toList());
        }
        return e;
    }

    /** Pure semantics ride the expansion: exists([])=false, forAll([])=true. */
    @Override
    protected String listExists(List<SqlExpr> args, boolean forAll) {
        String agg = forAll ? "list_bool_and" : "list_bool_or";
        String dflt = forAll ? boolLit(true) : boolLit(false);
        return "coalesce(" + agg + "(" + fn("list_transform", args) + "), " + dflt + ")";
    }

    @Override
    protected String listCall(SqlFn fnName, List<SqlExpr> args) {
        return switch (fnName) {
            case LIST_FILTER -> fn("list_filter", args);
            case LIST_TRANSFORM -> fn("list_transform", args);
            case LIST_CONCAT -> fn("list_concat", args);
            case LIST_CONTAINS -> fn("list_contains", args);
            case LIST_GET -> fn("list_extract", args);
            default -> throw new IllegalStateException("not a list call: " + fnName);
        };
    }

    /** DuckDB explodes select-list unnest into rows — placement idiom. */
    @Override
    protected String unnestProjection(List<SqlExpr> args) {
        return fn("unnest", args);
    }

    @Override
    protected String arrayLit(List<SqlExpr> elements) {
        return "[" + list(elements) + "]";
    }

    // ---- variant (JSON) idioms ----

    @Override
    protected String variantGet(List<SqlExpr> args) {
        return expr(args.get(0), 7) + " -> " + expr(args.get(1), 8);
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
        if (!c.array() && c.value() instanceof SqlExpr.Call call && call.fn() == SqlFn.VARIANT_GET) {
            String text = expr(call.args().get(0), 7) + " ->> " + expr(call.args().get(1), 8);
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
            default -> super.castTypeName(t);
        };
    }
}
