// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.StatementOrigin;
import com.legend.exec.Statements;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.List;

/**
 * The LIMIT-0 metadata probe — the ONE execution-bound ingredient of
 * late-bound raw-grid resolution (staged compilation, Invariant 7): the
 * REWRITE pass lives in {@code resolver.RawGridSchema} and takes the
 * probed column roster as INPUT through its {@code SchemaReferee}; this
 * class is the executor's referee implementation. A schema read, never
 * values (the E1 probe discipline; moved from ResultNav at its Phase 1c
 * deletion). The one {@code new SqlSource.RawSql} here is a chartered
 * construction site (RawSqlLedgerTest register); the text is the
 * AUTHORED statement, MIR-rendered through the dialect like every
 * query.
 */
public final class GridProbe {

    private GridProbe() {
    }

    /** The grid's projection NAMES for the authored SQL. */
    public static List<String> probeNames(String sql, String writtenFor, PlanSessions.Source sessions,
            ExecutionPlan.Target target, com.legend.sql.dialect.SqlDialect dialect) {
        List<String> names = new ArrayList<>();
        for (String[] c : probeColumns(sql, writtenFor, sessions, target, dialect)) {
            names.add(c[0]);
        }
        return names;
    }

    /** The grid's TYPED columns for the boundary resolver's referee
     * (§4bZ-U follow-on): the database's own column type when the SQL
     * type maps to a Pure primitive; the trusted-Any wildcard column
     * otherwise — an upgrade over the old names-only stamp, never a
     * new wall. */
    public static List<com.legend.compiler.element.type.Type.Column>
            probeTypedColumns(String sql, String writtenFor, PlanSessions.Source sessions,
            ExecutionPlan.Target target, com.legend.sql.dialect.SqlDialect dialect) {
        List<com.legend.compiler.element.type.Type.Column> cols =
                new ArrayList<>();
        for (String[] c : probeColumns(sql, writtenFor, sessions, target, dialect)) {
            // typed by the name its dialect reads the driver's into, where that name says the type
            String wire = dialect.typingWireName(c[1]);
            com.legend.compiler.element.type.Type t = wire == null ? null : WireProbe.pureOfSqlTypeOrNull(wire);
            cols.add(t == null
                    ? com.legend.compiler.element.type.Type.RelationType
                            .trustedColumn(c[0])
                    : new com.legend.compiler.element.type.Type.Column(
                            c[0], t, com.legend.compiler.element.type
                                    .Multiplicity.Bounded.ZERO_ONE));
        }
        return cols;
    }

    /** The grid's projection (name, SQL type name) pairs — the SAME
     * LIMIT-0 metadata read, types included; the text written for the
     * database {@code writtenFor} (decision D2). A plan statement of {@code target}, run by the plan runner on a
     * session of {@code sessions} ({@link PlanRunner#columns}: the doors, 2026-10-29; on the session seam since
     * track B's B1a). */
    public static List<String[]> probeColumns(String sql, String writtenFor,
            PlanSessions.Source sessions, ExecutionPlan.Target target, com.legend.sql.dialect.SqlDialect dialect) {
        SqlSelect probe = SqlSelect.starOf(
                new SqlSource.RawSql(sql, "_p", List.of(), writtenFor))
                .withLimit(0L);
        StatementOrigin.count(StatementOrigin.PROBE);
        List<String[]> cols = new ArrayList<>();
        for (Statements.Reported r : PlanRunner.columns(ExecutionPlan.Sql.of(dialect.render(probe), target),
                sessions)) {
            cols.add(new String[] {r.name(), r.driverType()});
        }
        return cols;
    }
}
