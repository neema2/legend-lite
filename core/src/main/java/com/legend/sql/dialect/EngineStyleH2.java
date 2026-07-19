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
        planQuery(query, new LinkedHashMap<>());
        StringBuilder sb = new StringBuilder();
        query(sb, query, 0);
        return sb.toString();
    }

    // == the engine's alias plan (replaceAliasName parity, audit 19 F2) ==
    // The engine groups aliases BY LOWERCASED TABLE NAME and numbers
    // 0..n-1 within each group in encounter order; each SELECT's leftmost
    // source renders "root" but still CONSUMES its group's next index
    // (self-join goldens: root + persontable_1). A subselect belongs to
    // its FIRST INNER TABLE's group (goldens: account_info_1, never
    // subselect_0), and its interior plans BEFORE the alias itself.

    private void planQuery(SqlQuery q, Map<String, Integer> groups) {
        switch (q) {
            case SqlSelect s -> {
                if (s.from() != null) {
                    planSource(s.from(), true, groups);
                }
            }
            case SqlUnion u -> u.branches().forEach(b -> planQuery(b, groups));
        }
    }

    private void planSource(SqlSource src, boolean leftmost,
            Map<String, Integer> groups) {
        switch (src) {
            case SqlSource.Table t -> {
                if (t.alias() != null) {
                    String named = nextInGroup(
                            t.name().toLowerCase(Locale.ROOT), groups);
                    renames.put(t.alias(), leftmost ? "root" : named);
                }
            }
            case SqlSource.Subselect sub -> {
                planQuery(sub.inner(), groups);
                String named = nextInGroup(firstInnerTable(sub.inner()), groups);
                renames.put(sub.alias(), leftmost ? "root" : named);
            }
            case SqlSource.Join j -> {
                planSource(j.left(), leftmost, groups);
                planSource(j.right(), false, groups);
            }
            default -> { }
        }
    }

    private static String nextInGroup(String group, Map<String, Integer> groups) {
        int i = groups.merge(group, 1, Integer::sum) - 1;
        return group + "_" + i;
    }

    /** The group a subselect alias renames into: its leftmost inner
     * table's lowercased name (the engine's traverse rule). */
    private static String firstInnerTable(SqlQuery q) {
        SqlSource src = q instanceof SqlSelect s ? s.from()
                : q instanceof SqlUnion u && !u.branches().isEmpty()
                        ? (u.branches().get(0) instanceof SqlSelect bs
                                ? bs.from() : null)
                        : null;
        while (src instanceof SqlSource.Join j) {
            src = j.left();
        }
        return switch (src) {
            case SqlSource.Table t -> t.name().toLowerCase(Locale.ROOT);
            case SqlSource.Subselect sub -> firstInnerTable(sub.inner());
            case null, default -> "subselect";
        };
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
        // THEN forward ELSE backward. The BRANCHES must also match the
        // sundayIndex-difference shape (audit 19 F1): a user-written
        // if(dateDiff(a,b,DAYS) <= 0, |x, |y) lowers to the SAME guard,
        // and folding it would silently delete both branches.
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
                && zero.value() == 0
                && isSundayIndexDifference(cs.whens().get(0).then())
                && isSundayIndexDifference(cs.otherwise())) {
            return "datediff(week, " + expr(dayDiff.args().get(2), 0)
                    + ", " + expr(dayDiff.args().get(1), 0) + ")";
        }
        return null;
    }

    /** {@code sundayIndex(d2) - sundayIndex(d1)} where sundayIndex =
     * {@code date_diff('day', DATE '0001-01-07', d) // 7} (Scalars'
     * dateDiff week emission — the only producer of this shape). */
    private static boolean isSundayIndexDifference(SqlExpr e) {
        return e instanceof SqlExpr.Call m
                && m.fn() == com.legend.sql.SqlFn.MINUS
                && m.args().size() == 2
                && isSundayIndex(m.args().get(0))
                && isSundayIndex(m.args().get(1));
    }

    private static boolean isSundayIndex(SqlExpr e) {
        return e instanceof SqlExpr.Call div
                && div.fn() == com.legend.sql.SqlFn.INT_DIVIDE
                && div.args().size() == 2
                && div.args().get(1) instanceof SqlExpr.IntLit seven
                && seven.value() == 7
                && div.args().get(0) instanceof SqlExpr.Call dd
                && dd.fn() == com.legend.sql.SqlFn.DATE_DIFF
                && dd.args().get(1) instanceof SqlExpr.DateLit epoch
                && epoch.iso().equals("0001-01-07");
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
            // enum-by-name temporals: the engine's H2 formatdatetime forms.
            // UNMATCHED formats THROW — falling back to strftime() would
            // leak a DuckDB spelling into engine-H2 golden text (audit 19)
            case STRFTIME -> {
                if (a.size() == 2 && a.get(1) instanceof SqlExpr.StringLit fmt) {
                    String java = switch (fmt.value()) {
                        case "%B" -> "MMMM";
                        case "%A" -> "EEEE";
                        default -> null;
                    };
                    if (java != null) {
                        yield "formatdatetime(" + expr(a.get(0), 0) + ", '"
                                + java + "')";
                    }
                }
                throw new IllegalStateException("strftime format has no"
                        + " engine-H2 formatdatetime spelling yet: " + a);
            }
            case TRIM -> a.size() == 1
                    ? "trim(both from " + expr(a.get(0), 0) + ")"
                    : super.call(c, parentPrec);
            case DATE_TRUNC_DAY -> "cast(truncate(" + expr(a.get(0), 0)
                    + ") as date)";
            // parse-date family: the engine's rule (convertToDateH2) is
            // substring(x, 1, 10) + the Java pattern for ALL date-only
            // formats; datetime formats parse the whole string. UNMATCHED
            // formats THROW rather than leak DuckDB strptime() text.
            case STRPTIME -> {
                if (a.size() == 2 && a.get(1) instanceof SqlExpr.StringLit f) {
                    String java = javaDatePattern(f.value());
                    if (java != null) {
                        boolean dateOnly = !f.value().contains("%H");
                        yield dateOnly
                                ? "parsedatetime(substring(" + expr(a.get(0), 0)
                                        + ", 1, 10), '" + java + "')"
                                : "parsedatetime(" + expr(a.get(0), 0)
                                        + ", '" + java + "')";
                    }
                }
                throw new IllegalStateException("strptime format has no"
                        + " engine-H2 parsedatetime spelling yet: " + a);
            }
            default -> super.call(c, parentPrec);
        };
    }

    /** C-style strptime directives → the Java pattern the engine's
     * parsedatetime takes; null when any directive has no mapping (the
     * caller throws — never a silent DuckDB fallback). */
    private static String javaDatePattern(String cFormat) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < cFormat.length(); i++) {
            char ch = cFormat.charAt(i);
            if (ch != '%') {
                out.append(ch);
                continue;
            }
            if (++i >= cFormat.length()) {
                return null;
            }
            String java = switch (cFormat.charAt(i)) {
                case 'Y' -> "yyyy";
                case 'm' -> "MM";
                case 'd' -> "dd";
                case 'H' -> "HH";
                case 'M' -> "mm";
                case 'S' -> "ss";
                default -> null;
            };
            if (java == null) {
                return null;
            }
            out.append(java);
        }
        return out.toString();
    }


    @Override
    protected String variantAwareCast(SqlExpr.Cast c) {
        String t = castTypeName(c.target()).toLowerCase(Locale.ROOT);
        // The engine spells casts PER DYNAFUNCTION (audit 19 F3), so only
        // respells that cannot collide survive here:
        // - toDecimal is the ONLY producer of DECIMAL(38, 18) and spells
        //   bare 'decimal'; parseDecimal keeps its declared (p, s).
        // - parseInteger carries SqlType.INTEGER from lowering (no respell;
        //   round/ceiling/floor keep 'bigint', matching their goldens).
        // - 'float': parseFloat's golden spelling. KNOWN LATENT COLLISION:
        //   toFloat shares the DOUBLE IR and the engine spells it 'double
        //   precision' — no corpus H2 golden pins toFloat text today; a
        //   per-dynafunction origin tag is the clean fix when one appears.
        if (t.equals("decimal(38, 18)")) {
            t = "decimal";
        } else if (t.equals("double precision") || t.equals("double")) {
            t = "float";
        }
        return "cast(" + expr(c.value(), 0) + " as " + t + ")";
    }

    @Override
    protected Set<String> reservedWords() {
        return Set.of();
    }
}
