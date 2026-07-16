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
        if (s.limit() != null) {
            sb.append(" limit ").append(s.limit());
        }
        if (s.offset() != null) {
            sb.append(" offset ").append(s.offset());
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
        return super.expr(e, parentPrec);
    }

    // == engine-H2 spellings (each MATCHED against a corpus golden) =====

    @Override
    protected String call(SqlExpr.Call c, int parentPrec) {
        java.util.List<SqlExpr> a = c.args();
        return switch (c.fn()) {
            case CONCAT -> "concat(" + a.stream().map(x -> expr(x, 0))
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
            default -> super.call(c, parentPrec);
        };
    }

    @Override
    protected String variantAwareCast(SqlExpr.Cast c) {
        return "cast(" + expr(c.value(), 0) + " as "
                + castTypeName(c.target()).toLowerCase(Locale.ROOT) + ")";
    }

    @Override
    protected Set<String> reservedWords() {
        return Set.of();
    }
}
