// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.List;

/**
 * DYNAMIC-PIVOT STATICIZATION (PV1b) — the TWO-PHASE execution a
 * backend without native dynamic PIVOT needs (the engine's own H2
 * route): a {@code Pivot} whose IN list is EMPTY has data-dependent
 * output columns, so the key's DISTINCT values are discovered by a
 * FIRST query on the SAME connection and pinned as literals; the now
 * STATIC pivot then takes the ordinary emulation strategy
 * ({@code CarrierStrategies}). Value order is ascending — the
 * reference target's dynamic-pivot column order. NULL keys are
 * skipped: a NULL never names an output column.
 *
 * <p>Dialects with native dynamic PIVOT ({@link
 * com.legend.sql.dialect.SqlDialect#needsStaticPivot} false) pass
 * through untouched — this pre-pass runs at the EXECUTION seam, where
 * a connection exists, never inside rendering.
 */
public final class DynamicPivot {

    private DynamicPivot() {
    }

    /** How a key's distinct values are read on the body's session (track B's B1a, 2026-10-29: the pivot reads no
     *  JDBC itself): a query of one column {@code v} run as TYPED rows by the caller, the column's facts the pivot
     *  gives (the cells arrive as the typed method's carriers, docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3). */
    @FunctionalInterface
    public interface KeyReader {
        List<List<Object>> rows(SqlQuery query, List<com.legend.executionplan.ExecutionPlan.Column> columns);
    }

    /** {@code plan} with each dynamic pivot's keys spelled in, on a database with no native dynamic pivot: a key
     *  the lowering did not type is typed by what the database reports for it (a LIMIT-0 schema read, a plan
     *  statement of {@code target} the plan runner runs on a session of {@code sessions}: {@link PlanRunner#columns},
     *  the doors, 2026-10-29), and {@code reader} reads the keys' values as typed rows. */
    public static SqlQuery staticize(SqlQuery plan,
            com.legend.sql.dialect.SqlDialect dialect,
            PlanSessions.Source sessions, ExecutionPlan.Target target, KeyReader reader) {
        if (!dialect.needsStaticPivot()) {
            return plan;
        }
        return new SqlRewriter() {
                @Override
                protected SqlSource source(SqlSource s) {
                    if (!(s instanceof SqlSource.Pivot p) || !p.in().isEmpty()
                            || p.on().size() != 1) {
                        return s;
                    }
                    return new SqlSource.Pivot(p.source(), p.on(),
                            discover(p, dialect, sessions, target, reader), p.named(), p.usings(),
                            p.alias(), p.outputs());
                }
        }.rewrite(plan);
    }

    /** The key's distinct non-null values, ascending, as literals. */
    private static List<SqlExpr> discover(SqlSource.Pivot p, com.legend.sql.dialect.SqlDialect dialect,
            PlanSessions.Source sessions, ExecutionPlan.Target target, KeyReader reader) {
        SqlExpr key = p.on().get(0);
        SqlSelect q = SqlSelect.starOf(p.source())
                .withProjections(List.of(
                        new SqlSelect.Projection(key, "v", null)))
                .withDistinct()
                .withWhere(SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NOT_NULL,
                        key))
                .withOrderBy(List.of(SqlSelect.SortKey.asc(
                        SqlExpr.Column.derived(null, "v"))));
        // the key's DECLARED type drives the decode (the typed method's rule, slot first): an integer key
        // arriving as a numeric (Postgres delivers HUGEINT, a sum of integers, as NUMERIC(38)) is read as an
        // integer, a REAL as the double its printed value reads back to (JdbcDecode), so each literal is the key's
        // own kind. A key whose column reference carries no type fact (a physical column the lowering did not type)
        // is typed by what the database reports for it (a LIMIT-0 schema read, the probe this pivot always was); a
        // key the database reports in no kind of the vocabulary is refused by name (AGENTS.md invariant 4)
        com.legend.sql.SqlType keyType = key.type() instanceof com.legend.sql.TypeFact.Typed t ? t.type()
                : reportedType(q, dialect, sessions, target, key);
        String slot = com.legend.executionplan.ColumnFacts.slot(keyType);
        com.legend.executionplan.ExecutionPlan.Column column = new com.legend.executionplan.ExecutionPlan.Column(
                "v", WireTypes.pureTypeNameOf(slot), new com.legend.executionplan.ExecutionPlan.Multiplicity(0, 1),
                slot, null, null);
        List<SqlExpr> in = new ArrayList<>();
        StatementOrigin.count(StatementOrigin.PROBE);
        for (List<Object> row : reader.rows(q, List.of(column))) {
            Object v = row.get(0);
            // F7.6: every pivot-key kind gets its TYPED literal — the old default silently respelled
            // DATE/DECIMAL/TIMESTAMP keys as strings in the regenerated IN list; an unmapped kind throws
            in.add(switch (v) {
                case Long l -> new SqlExpr.IntLit(l);
                // ClickHouse delivers an Int128 (a sum of integers) past a long
                case java.math.BigInteger b -> new SqlExpr.IntLit(b.longValueExact());
                case Boolean b -> new SqlExpr.BoolLit(b);
                case java.math.BigDecimal d -> new SqlExpr.DecimalLit(d);
                case Double d -> new SqlExpr.FloatLit(d);
                case String str -> new SqlExpr.StringLit(str);
                case com.legend.values.PureDateLiteral.StrictDate d -> new SqlExpr.DateLit(d.toEngineString());
                case com.legend.values.PureDateLiteral d -> new SqlExpr.TimestampLit(tsText(d.toInstantFloor()));
                case null -> throw new IllegalStateException(
                        "NULL pivot key past the IS NOT NULL guard");
                default -> throw new com.legend.error
                        .NotImplementedException("dynamic pivot key of"
                        + " kind " + v.getClass().getName()
                        + " has no typed literal arm");
            });
        }
        return in;
    }

    /** The database's own type for the key column of {@code q}, read as a schema (never values): the one column
     *  {@code v} of {@code SELECT * FROM (q) LIMIT 0}. */
    private static com.legend.sql.SqlType reportedType(SqlSelect q, com.legend.sql.dialect.SqlDialect dialect,
            PlanSessions.Source sessions, ExecutionPlan.Target target, SqlExpr key) {
        SqlSelect probe = SqlSelect.starOf(new SqlSource.Subselect(q, "_k", null)).withLimit(0L);
        List<Statements.Reported> reported = PlanRunner.columns(ExecutionPlan.Sql.of(dialect.render(probe), target),
                sessions);
        com.legend.sql.SqlType type = reported.size() == 1 ? reported.get(0).type() : null;
        if (type == null) {
            throw new IllegalStateException("a dynamic pivot's key carries no type fact and the database reports it in"
                    + " no kind of the SQL vocabulary: its values cannot be read as typed cells (" + key + "; reported "
                    + reported + ")");
        }
        return type;
    }

    /** Deterministic timestamp-literal text: seconds ALWAYS present,
     * subseconds exactly as carried (audit 2026-08-18 finding D:
     * {@code LocalDateTime.toString()} drops {@code :00} seconds and
     * varies subsecond width — a repr-ruled value spelled by a JDK
     * convenience). */
    private static String tsText(java.time.LocalDateTime t) {
        StringBuilder sb = new StringBuilder(29)
                .append(t.toLocalDate()).append(' ');
        pad2(sb, t.getHour()).append(':');
        pad2(sb, t.getMinute()).append(':');
        pad2(sb, t.getSecond());
        int nano = t.getNano();
        if (nano != 0) {
            String frac = String.valueOf(1_000_000_000L + nano)
                    .substring(1);
            int end = frac.length();
            while (frac.charAt(end - 1) == '0') {
                end--;
            }
            sb.append('.').append(frac, 0, end);
        }
        return sb.toString();
    }

    private static StringBuilder pad2(StringBuilder sb, int v) {
        if (v < 10) {
            sb.append('0');
        }
        return sb.append(v);
    }

}
