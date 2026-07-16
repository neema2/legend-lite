// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The ENGINE's own H2 SQL text &mdash; for byte-exact {@code toSQLString}
 * goldens (transform/fromPure/tests), never for execution. The formatting
 * contract is MATCHED against the engine's output, not invented: one
 * line, lowercase keywords, each SELECT's leftmost source aliased
 * {@code "root"}, every other join source aliased
 * {@code <lowercase-table>_<n>} with a query-global counter (the engine's
 * replaceAliasName pass), source/output aliases double-quoted, physical
 * column names bare, parenthesized ON clauses.
 */
public final class EngineStyleH2 extends AnsiSqlRenderer {

    private final Map<String, String> renames = new LinkedHashMap<>();

    @Override
    public String render(SqlQuery query) {
        renames.clear();
        planQuery(query, new int[] {0});
        StringBuilder sb = new StringBuilder();
        query(sb, query, 0);
        return sb.toString();
    }

    // == the engine's alias plan (replaceAliasName parity) ==============

    private void planQuery(SqlQuery q, int[] ctr) {
        switch (q) {
            case SqlSelect s -> {
                if (s.from() != null) {
                    planSource(s.from(), true, ctr);
                }
            }
            case SqlUnion u -> u.branches().forEach(b -> planQuery(b, ctr));
        }
    }

    private void planSource(SqlSource src, boolean leftmost, int[] ctr) {
        switch (src) {
            case SqlSource.Table t -> {
                if (t.alias() != null) {
                    renames.put(t.alias(), leftmost ? "root"
                            : t.name().toLowerCase(Locale.ROOT) + "_" + ctr[0]++);
                }
            }
            case SqlSource.Subselect sub -> {
                renames.put(sub.alias(), leftmost ? "root"
                        : "subselect_" + ctr[0]++);
                planQuery(sub.inner(), ctr);
            }
            case SqlSource.Join j -> {
                planSource(j.left(), leftmost, ctr);
                planSource(j.right(), false, ctr);
            }
            default -> { }
        }
    }

    private String rename(String alias) {
        return renames.getOrDefault(alias, alias);
    }

    // == single-line lowercase clause assembly ==========================

    @Override
    protected void select(StringBuilder sb, SqlSelect s, int depth) {
        if (s.qualify() != null) {
            throw new IllegalStateException(
                    "QUALIFY has no engine-H2 golden spelling");
        }
        sb.append("select ");
        // H2: a bare row cap is TOP; a slice is OFFSET .. FETCH NEXT
        if (s.limit() != null && s.offset() == null) {
            sb.append("top ").append(s.limit()).append(' ');
        }
        if (s.distinct()) {
            sb.append("distinct ");
        }
        sb.append(s.projections().isEmpty() ? "*"
                : s.projections().stream().map(this::projection)
                        .collect(Collectors.joining(", ")));
        if (s.from() != null) {
            sb.append(" from ");
            source(sb, s.from(), depth);
        }
        if (s.where() != null) {
            sb.append(" where ").append(expr(s.where(), 0));
        }
        if (!s.groupBy().isEmpty()) {
            sb.append(" group by ").append(s.groupBy().stream()
                    .map(e -> expr(e, 0)).collect(Collectors.joining(", ")));
        }
        if (s.having() != null) {
            sb.append(" having ").append(expr(s.having(), 0));
        }
        if (!s.orderBy().isEmpty()) {
            sb.append(" order by ").append(s.orderBy().stream()
                    .map(this::sortKey).collect(Collectors.joining(", ")));
        }
        if (s.offset() != null) {
            sb.append(" offset ").append(s.offset()).append(" rows");
            if (s.limit() != null) {
                sb.append(" fetch next ").append(s.limit()).append(" rows only");
            }
        }
    }

    @Override
    protected void source(StringBuilder sb, SqlSource src, int depth) {
        switch (src) {
            case SqlSource.Table t -> {
                sb.append(t.name());
                if (t.alias() != null) {
                    sb.append(" as \"").append(rename(t.alias())).append('"');
                }
            }
            case SqlSource.Subselect sub -> {
                sb.append('(');
                query(sb, sub.inner(), depth);
                sb.append(") as \"").append(rename(sub.alias())).append('"');
            }
            case SqlSource.Join j -> {
                source(sb, j.left(), depth);
                sb.append(' ')
                        .append(j.kind() == SqlSource.Join.Kind.INNER
                                ? "inner join"
                                : j.kind().sql.toLowerCase(Locale.ROOT))
                        .append(' ');
                source(sb, j.right(), depth);
                if (j.on() != null) {
                    sb.append(" on (").append(expr(j.on(), 0)).append(')');
                }
            }
            default -> super.source(sb, src, depth);
        }
    }

    @Override
    protected String projection(SqlSelect.Projection p) {
        String e = expr(p.expr(), 0);
        return p.alias() == null ? e : e + " as \"" + p.alias() + '"';
    }

    @Override
    protected String expr(SqlExpr e, int parentPrec) {
        // alias part quoted, physical column bare — "root".FIRSTNAME
        if (e instanceof SqlExpr.Column c) {
            return c.table() == null ? c.name()
                    : '"' + rename(c.table()) + "\"." + c.name();
        }
        String dd = engineDateDiff(e);
        if (dd != null) {
            return dd;
        }
        return super.expr(e, parentPrec);
    }

    /**
     * dateDiff's composite IR shapes fold BACK to the engine's plain
     * {@code datediff(unit, a, b)} emission. The IR encodes REAL pure's
     * per-unit semantics (truncated elapsed time; Sunday-boundary weeks —
     * PCT-pinned, Scalars.dateDiffExpr); the engine's H2 SQL emits plain
     * DATEDIFF regardless, and the toSQLString goldens pin that TEXT.
     * The shapes (epoch_ms pairs under integer division; the week CASE)
     * are only produced by the dateDiff lowering.
     */
    private String engineDateDiff(SqlExpr e) {
        // truncated elapsed: (epoch_ms(end) - epoch_ms(start)) // unitMs
        if (e instanceof SqlExpr.Call div
                && div.fn() == com.legend.sql.SqlFn.INT_DIVIDE
                && div.args().size() == 2
                && div.args().get(1) instanceof SqlExpr.IntLit u
                && div.args().get(0) instanceof SqlExpr.Call minus
                && minus.fn() == com.legend.sql.SqlFn.MINUS
                && minus.args().size() == 2
                && minus.args().get(0) instanceof SqlExpr.Call end
                && end.fn() == com.legend.sql.SqlFn.EPOCH_MS
                && minus.args().get(1) instanceof SqlExpr.Call start
                && start.fn() == com.legend.sql.SqlFn.EPOCH_MS) {
            String unit = switch ((int) u.value()) {
                case 3_600_000 -> "hour";
                case 60_000 -> "minute";
                case 1_000 -> "second";
                default -> null;
            };
            if (unit != null) {
                return "datediff(" + unit + ", " + expr(start.args().get(0), 0)
                        + ", " + expr(end.args().get(0), 0) + ")";
            }
        }
        // Sunday-boundary weeks: CASE WHEN date_diff('day', end, start) <= 0
        // THEN forward ELSE backward — start/end recovered from the guard
        if (e instanceof SqlExpr.Case cs && cs.whens().size() == 1
                && cs.otherwise() != null
                && cs.whens().get(0).condition() instanceof SqlExpr.Call le
                && le.fn() == com.legend.sql.SqlFn.LESS_EQUAL
                && le.args().size() == 2
                && le.args().get(0) instanceof SqlExpr.Call dayDiff
                && dayDiff.fn() == com.legend.sql.SqlFn.DATE_DIFF
                && dayDiff.args().get(0) instanceof SqlExpr.StringLit day
                && day.value().equals("day")
                && le.args().get(1) instanceof SqlExpr.IntLit zero
                && zero.value() == 0) {
            return "datediff(week, " + expr(dayDiff.args().get(2), 0)
                    + ", " + expr(dayDiff.args().get(1), 0) + ")";
        }
        return null;
    }

    // == engine-H2 spellings (each MATCHED against a corpus golden) =====

    @Override
    protected String call(SqlExpr.Call c, int parentPrec) {
        java.util.List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            // n-ary concat: nested CONCAT calls SPLICE (the engine emits
            // one flat concat(a, '_', b), never concat(concat(a,'_'),b))
            case CONCAT -> "concat(" + flattenConcat(a).stream()
                    .map(x -> expr(x, 0))
                    .collect(Collectors.joining(", ")) + ")";
            // datediff(<bare unit>, a, b); composite elapsed-time forms
            // built at lowering (weeks/hours/...) have no re-spelling here
            case DATE_DIFF -> a.get(0) instanceof SqlExpr.StringLit u
                    ? "datediff(" + u.value() + ", " + expr(a.get(1), 0)
                            + ", " + expr(a.get(2), 0) + ")"
                    : super.call(c, parentPrec);
            case CHR -> "char(" + expr(a.get(0), 0) + ")";
            case REVERSE_STRING -> "legend_h2_extension_reverse_string("
                    + expr(a.get(0), 0) + ")";
            case SPLIT_PART -> "legend_h2_extension_split_part("
                    + a.stream().map(x -> expr(x, 0))
                            .collect(Collectors.joining(", ")) + ")";
            case TODAY -> "cast(now() as date)";
            case TRIM -> a.size() == 1
                    ? "trim(both from " + expr(a.get(0), 0) + ")"
                    : super.call(c, parentPrec);
            case DATE_TRUNC_DAY -> "cast(truncate(" + expr(a.get(0), 0)
                    + ") as date)";
            // parseDate: the engine truncates to the 10 date characters and
            // parses with the Java pattern (parsedatetime golden)
            case STRPTIME -> a.size() == 2
                    && a.get(1) instanceof SqlExpr.StringLit f
                    && f.value().equals("%Y-%m-%d")
                    ? "parsedatetime(substring(" + expr(a.get(0), 0)
                            + ", 1, 10), 'yyyy-MM-dd')"
                    : super.call(c, parentPrec);
            default -> super.call(c, parentPrec);
        };
    }

    private static java.util.List<SqlExpr> flattenConcat(java.util.List<SqlExpr> a) {
        java.util.List<SqlExpr> out = new java.util.ArrayList<>();
        for (SqlExpr e : a) {
            if (e instanceof SqlExpr.Call c && c.fn() == com.legend.sql.SqlFn.CONCAT) {
                out.addAll(flattenConcat(c.args()));
            } else {
                out.add(e);
            }
        }
        return out;
    }

    @Override
    protected String variantAwareCast(SqlExpr.Cast c) {
        String t = castTypeName(c.target()).toLowerCase(Locale.ROOT);
        // engine H2 type spellings (each matched against a golden):
        // bare decimal, float not double, integer for pure Integer parses
        if (t.startsWith("decimal(")) {
            t = "decimal";
        } else if (t.equals("double precision") || t.equals("double")) {
            t = "float";
        } else if (t.equals("bigint")) {
            t = "integer";
        }
        return "cast(" + expr(c.value(), 0) + " as " + t + ")";
    }

    @Override
    protected Set<String> reservedWords() {
        return Set.of();
    }
}
