package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
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
 * The DuckDB renderer &mdash; lean, human-readable SQL:
 *
 * <ul>
 *   <li>Identifiers are quoted ONLY when necessary (non-plain spelling or a
 *       reserved word) &mdash; {@code SELECT NAME FROM T_PERSON AS t0}, not a
 *       wall of quotes.</li>
 *   <li>Minimal parentheses via operator precedence &mdash; {@code WHERE AGE > 30
 *       AND ACTIVE}, with parens only where grouping demands them.</li>
 *   <li>One clause per line; subqueries indent by two spaces.</li>
 * </ul>
 *
 * <p>Semantic-name spellings encode the MUST-honor contract
 * (PHASE_HIJ_LOWERING.md): float-forcing division, always-positive {@code mod},
 * {@code isEmpty}&rarr;{@code IS NULL}. An unregistered semantic name throws.
 */
public final class DuckDb implements SqlDialect {

    private static final Pattern PLAIN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** Reserved words that force quoting even when plainly spelled (lowercase). */
    private static final Set<String> RESERVED = Set.of(
            "all", "and", "as", "asc", "between", "by", "case", "cast", "create", "cross",
            "default", "delete", "desc", "distinct", "drop", "else", "end", "except", "exists",
            "false", "from", "full", "group", "having", "in", "inner", "insert", "intersect",
            "into", "is", "join", "left", "like", "limit", "not", "null", "offset", "on", "or",
            "order", "outer", "pivot", "qualify", "right", "select", "table", "then", "true",
            "union", "update", "using", "values", "when", "where", "window", "with");

    /**
     * Functions that render as plain {@code fn(args)} under their own name.
     * An EXPLICIT allowlist: anything else still throws — the list grows with
     * execution-pinned tests, never by fallback.
     */
    private static final Set<String> PLAIN_FNS = Set.of(
            "abs", "length", "upper", "lower", "coalesce", "greatest", "least",
            "list_filter", "list_transform", "list_reduce", "list_concat", "list_contains",
            "list_bool_or", "list_bool_and", "len");

    /** Infix operators: semantic name → (sql, precedence). Higher binds tighter. */
    private record Infix(String sql, int prec) {
    }

    private static final Map<String, Infix> INFIX = Map.ofEntries(
            Map.entry("or", new Infix("OR", 1)),
            Map.entry("and", new Infix("AND", 2)),
            Map.entry("equal", new Infix("=", 4)),
            Map.entry("notEqual", new Infix("<>", 4)),
            Map.entry("less", new Infix("<", 4)),
            Map.entry("lessEqual", new Infix("<=", 4)),
            Map.entry("greater", new Infix(">", 4)),
            Map.entry("greaterEqual", new Infix(">=", 4)),
            Map.entry("plus", new Infix("+", 5)),
            Map.entry("minus", new Infix("-", 5)),
            Map.entry("concat", new Infix("||", 5)),
            Map.entry("times", new Infix("*", 6)));

    @Override
    public String render(SqlQuery query) {
        StringBuilder sb = new StringBuilder();
        query(sb, query, 0);
        return sb.toString();
    }

    // ----- queries -----

    private void query(StringBuilder sb, SqlQuery q, int depth) {
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

    private void select(StringBuilder sb, SqlSelect s, int depth) {
        sb.append("SELECT ");
        if (s.distinct()) {
            sb.append("DISTINCT ");
        }
        sb.append(s.projections().isEmpty()
                ? "*"
                : s.projections().stream().map(this::projection).collect(Collectors.joining(", ")));
        nl(sb, depth).append("FROM ");
        source(sb, s.from(), depth);
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
            nl(sb, depth).append("QUALIFY ").append(expr(s.qualify(), 0));
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

    private String projection(SqlSelect.Projection p) {
        String e = expr(p.expr(), 0);
        return p.alias() == null ? e : e + " AS " + ident(p.alias());
    }

    private String sortKey(SqlSelect.SortKey k) {
        String s = expr(k.expr(), 0) + (k.ascending() ? "" : " DESC");
        if (k.nullOrder() != null) {
            s += k.nullOrder() == SqlSelect.SortKey.NullOrder.NULLS_FIRST
                    ? " NULLS FIRST" : " NULLS LAST";
        }
        return s;
    }

    // ----- sources -----

    private void source(StringBuilder sb, SqlSource src, int depth) {
        switch (src) {
            case SqlSource.Table t -> {
                sb.append(ident(t.name()));
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
            case SqlSource.Values v -> {
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
            case SqlSource.Join j -> {
                source(sb, j.left(), depth);
                nl(sb, depth).append(j.kind().sql).append(" ");
                source(sb, j.right(), depth);
                if (j.on() != null) {
                    sb.append(" ON ").append(expr(j.on(), 0));
                }
            }
        }
    }

    // ----- expressions (minimal parens: wrap a child only when it binds looser) -----

    private String expr(SqlExpr e, int parentPrec) {
        return switch (e) {
            case SqlExpr.Column c -> c.table() == null
                    ? ident(c.name()) : ident(c.table()) + "." + ident(c.name());
            case SqlExpr.Star s -> s.table() == null ? "*" : ident(s.table()) + ".*";
            case SqlExpr.StringLit s -> "'" + s.value().replace("'", "''") + "'";
            case SqlExpr.IntLit i -> String.valueOf(i.value());
            case SqlExpr.FloatLit f -> String.valueOf(f.value());
            case SqlExpr.DecimalLit d -> d.value().toPlainString();
            case SqlExpr.BoolLit b -> b.value() ? "TRUE" : "FALSE";
            case SqlExpr.NullLit n -> "NULL";
            case SqlExpr.DateLit d -> "DATE '" + d.iso() + "'";
            case SqlExpr.TimestampLit t -> "TIMESTAMP '" + t.iso() + "'";
            case SqlExpr.ArrayLit a -> "[" + list(a.elements()) + "]";
            case SqlExpr.Call c -> call(c, parentPrec);
            case SqlExpr.Case c -> caseExpr(c);
            case SqlExpr.Exists ex -> "EXISTS (" + inline(ex.subquery()) + ")";
            case SqlExpr.ScalarSubquery sq -> "(" + inline(sq.subquery()) + ")";
            case SqlExpr.WindowCall w -> windowCall(w);
            case SqlExpr.Lambda l -> (l.params().size() == 1
                    ? l.params().get(0)
                    : "(" + String.join(", ", l.params()) + ")") + " -> " + expr(l.body(), 0);
            case SqlAgg.Reducer r -> reducer(r);
        };
    }

    private String call(SqlExpr.Call c, int parentPrec) {
        Infix infix = INFIX.get(c.fn());
        if (infix != null) {
            String s = c.args().stream()
                    .map(a -> expr(a, infix.prec()))
                    .collect(Collectors.joining(" " + infix.sql() + " "));
            return infix.prec() < parentPrec ? "(" + s + ")" : s;
        }
        List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            case "not" -> {
                String s = "NOT " + expr(a.get(0), 3);
                yield 3 < parentPrec ? "(" + s + ")" : s;
            }
            case "negate" -> "-" + expr(a.get(0), 7);
            case "isNull" -> expr(a.get(0), 4) + " IS NULL";
            case "isNotNull" -> expr(a.get(0), 4) + " IS NOT NULL";
            case "in" -> expr(a.get(0), 4) + " IN ("
                    + list(a.subList(1, a.size())) + ")";
            // MUST-honor semantics (PHASE_HIJ_LOWERING.md):
            case "divide" -> "((1.0 * " + expr(a.get(0), 0) + ") / " + expr(a.get(1), 0) + ")";
            case "mod" -> "MOD(MOD(" + expr(a.get(0), 0) + ", " + expr(a.get(1), 0) + ") + "
                    + expr(a.get(1), 0) + ", " + expr(a.get(1), 0) + ")";
            case "rem" -> "MOD(" + expr(a.get(0), 0) + ", " + expr(a.get(1), 0) + ")";
            default -> {
                if (!PLAIN_FNS.contains(c.fn())) {
                    throw new IllegalStateException(
                            "no DuckDB rendering registered for semantic function '" + c.fn() + "'");
                }
                yield c.fn() + "(" + list(a) + ")";
            }
        };
    }

    private String caseExpr(SqlExpr.Case c) {
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

    private String windowCall(SqlExpr.WindowCall w) {
        String fn = switch (w.fn()) {
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
        return fn + " OVER (" + over + ")";
    }

    private static String bound(SqlExpr.WindowCall.Frame.Bound b) {
        return switch (b) {
            case SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding u -> "UNBOUNDED PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.Preceding p -> p.n() + " PRECEDING";
            case SqlExpr.WindowCall.Frame.Bound.CurrentRow c -> "CURRENT ROW";
            case SqlExpr.WindowCall.Frame.Bound.Following f -> f.n() + " FOLLOWING";
            case SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing u -> "UNBOUNDED FOLLOWING";
        };
    }

    private String reducer(SqlAgg.Reducer r) {
        String args = r.args().isEmpty() ? "*" : list(r.args());
        return r.fn() + "(" + (r.distinct() ? "DISTINCT " : "") + args + ")";
    }

    private String list(List<SqlExpr> es) {
        return es.stream().map(e -> expr(e, 0)).collect(Collectors.joining(", "));
    }

    /** A subquery rendered inline (EXISTS / scalar position): single line. */
    private String inline(SqlQuery q) {
        StringBuilder sb = new StringBuilder();
        query(sb, q, 0);
        return sb.toString().replace("\n", " ").replaceAll(" +", " ");
    }

    // ----- identifiers: quote ONLY when necessary (the lean tenet) -----

    private String ident(String name) {
        return PLAIN.matcher(name).matches() && !RESERVED.contains(name.toLowerCase())
                ? name
                : "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static StringBuilder nl(StringBuilder sb, int depth) {
        return sb.append("\n").append("  ".repeat(depth));
    }
}
