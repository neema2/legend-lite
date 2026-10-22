// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

    public static SqlQuery staticize(SqlQuery plan,
            com.legend.sql.dialect.SqlDialect dialect,
            Connection connection) {
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
                            discover(p, dialect, connection), p.named(), p.usings(),
                            p.alias(), p.outputs());
                }
        }.rewrite(plan);
    }

    /** The key's distinct non-null values, ascending, as literals. */
    private static List<SqlExpr> discover(SqlSource.Pivot p,
            com.legend.sql.dialect.SqlDialect dialect, Connection connection) {
        SqlExpr key = p.on().get(0);
        SqlSelect q = SqlSelect.starOf(p.source())
                .withProjections(List.of(
                        new SqlSelect.Projection(key, "v", null)))
                .withDistinct()
                .withWhere(SqlExpr.Call.of(com.legend.sql.SqlFn.IS_NOT_NULL,
                        key))
                .withOrderBy(List.of(SqlSelect.SortKey.asc(
                        SqlExpr.Column.derived(null, "v"))));
        // the key's DECLARED type drives the decode (Executor.unwrap's rule): an integer key
        // arriving as a numeric (Postgres delivers HUGEINT, a sum of integers, as NUMERIC(38)) is
        // an integer literal
        boolean integral = key.type() instanceof com.legend.sql.TypeFact.Typed t && java.util.Set.of(
                com.legend.sql.SqlType.Scalar.INTEGER, com.legend.sql.SqlType.Scalar.BIGINT,
                com.legend.sql.SqlType.Scalar.HUGEINT).contains(t.type());
        List<SqlExpr> in = new ArrayList<>();
        StatementOrigin.count(StatementOrigin.PROBE);
        try (Statement st = connection.createStatement();
                ResultSet rs = st.executeQuery(dialect.render(q))) {
            while (rs.next()) {
                Object v = rs.getObject(1);
                // F7.6: every JDBC pivot-key kind gets its TYPED
                // literal — the old default silently respelled
                // DATE/DECIMAL/TIMESTAMP keys as strings in the
                // regenerated IN list; an unmapped kind now throws
                in.add(switch (v) {
                    case Integer i -> new SqlExpr.IntLit(i);
                    case Long l -> new SqlExpr.IntLit(l);
                    case Boolean b -> new SqlExpr.BoolLit(b);
                    case java.math.BigDecimal d when integral -> new SqlExpr.IntLit(d.longValueExact());
                    // ClickHouse delivers an Int128 (a sum of integers) as a BigInteger
                    case java.math.BigInteger b when integral -> new SqlExpr.IntLit(b.longValueExact());
                    case java.math.BigDecimal d -> new SqlExpr.DecimalLit(d);
                    case Double d -> new SqlExpr.FloatLit(d);
                    // audit 2026-08-18 finding D: a bare (double) widen
                    // of 3.14f is 3.140000104904175 — the regenerated IN
                    // literal could never match the source row and the
                    // pivot column silently vanished. Float.toString is
                    // the shortest round-trip repr; parsing THAT as
                    // double preserves the printed value
                    case Float f -> new SqlExpr.FloatLit(
                            Double.parseDouble(Float.toString(f)));
                    case java.sql.Date d ->
                            new SqlExpr.DateLit(d.toLocalDate().toString());
                    case java.time.LocalDate d ->
                            new SqlExpr.DateLit(d.toString());
                    case java.sql.Timestamp t -> new SqlExpr.TimestampLit(
                            tsText(t.toLocalDateTime()));
                    case java.time.LocalDateTime t ->
                            new SqlExpr.TimestampLit(tsText(t));
                    case String str -> new SqlExpr.StringLit(str);
                    case null -> throw new IllegalStateException(
                            "NULL pivot key past the IS NOT NULL guard");
                    default -> throw new com.legend.error
                            .NotImplementedException("dynamic pivot key of"
                            + " JDBC kind " + v.getClass().getName()
                            + " has no typed literal arm");
                });
            }
        } catch (SQLException e) {
            // the seam: java.sql stops at this discover boundary
            throw new com.legend.error.DataError(
                    String.valueOf(e.getMessage()), e);
        }
        return in;
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
