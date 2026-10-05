// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.test.StorelessRuntime;

import com.legend.Execution;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE SINGLE-QUERY PIN (P3-2): the LIMIT-0 schema probe runs ONLY when
 * the tree statically DEMANDS a late-bound grid's schema (a
 * {@code columnNames}/{@code values} read — {@code RawGridSchema}'s
 * demand gate). An undemanded grid executes as ONE query: the lowering
 * emits the zero-output star-select and the egress adopts the result-set
 * headers ({@code Executor.resolveColumns}' late-bound arm, gated on
 * {@code schema.isLateBound()} — the TYPE, never an outputs-emptiness
 * proxy). This test counts actual JDBC traffic through a proxy — the
 * probe count is PINNED, not narrated.
 */
class ExecuteInDbProbeCountTest {

    private static final String CONN_LET =
            "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                    + "type=meta::relational::runtime::DatabaseType.DuckDB);\n";

    private static Connection real;

    @BeforeAll
    static void open() throws Exception {
        real = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterAll
    static void close() throws Exception {
        real.close();
    }

    /** Every SQL string executed through the proxied connection. */
    private static List<String> executed;

    private static Connection counting(Connection target) {
        return (Connection) Proxy.newProxyInstance(
                ExecuteInDbProbeCountTest.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    Object r = method.invoke(target, args);
                    if (r instanceof Statement st
                            && method.getName().equals("createStatement")) {
                        return Proxy.newProxyInstance(
                                ExecuteInDbProbeCountTest.class.getClassLoader(),
                                new Class<?>[] {Statement.class},
                                (p2, m2, a2) -> {
                                    if (a2 != null && a2.length > 0
                                            && a2[0] instanceof String sql
                                            && m2.getName().startsWith("execute")) {
                                        executed.add(sql);
                                    }
                                    return m2.invoke(st, a2);
                                });
                    }
                    return r;
                });
    }

    private static long probes() {
        return executed.stream()
                .filter(sql -> sql.toLowerCase(Locale.ROOT).contains("limit 0"))
                .count();
    }

    private static ExecutionResult run(String tail) throws Exception {
        executed = new ArrayList<>();
        return Execution.execute(StorelessRuntime.with("", DatabaseType.DuckDB), CONN_LET + tail, StorelessRuntime.RUNTIME, counting(real));
    }

    @Test
    @DisplayName("undemanded grid as relation root: ZERO probes, headers adopted at egress")
    void undemandedGridSingleQuery() throws Exception {
        ExecutionResult r = run(
                "let r = meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as X, 2 as Y', $c, 0, 1000); $r.rows;}");
        assertEquals(0, probes(),
                "no columnNames/values demand — the one executed query is"
                + " its own schema authority; probe traffic: " + executed);
        ExecutionResult.Tabular t = (ExecutionResult.Tabular) r;
        assertEquals(List.of("X", "Y"),
                t.columns().stream().map(Column::name).toList(),
                "late-bound egress arm adopts the result-set headers");
        assertEquals(1, t.rows().size());
        assertEquals(1L, ((Number) t.rows().get(0).values().get(0)).longValue());
        assertEquals(2L, ((Number) t.rows().get(0).values().get(1)).longValue());
    }

    @Test
    @DisplayName("named cell read DEMANDS the schema: exactly ONE probe, typed cells")
    void namedReadProbesOnce() throws Exception {
        // §4bZ-U RULING (2026-08-25, wire-ledger burn): a by-name FIELD
        // read over a late-bound grid IS a static schema demand — the
        // old trust-name path typed such cells Any (JSON labels, the
        // last 2 corpus wire-diverge rows); the probe's typed columns
        // give the cell the database's own type. Cost: one LIMIT-0
        // metadata read per demanded grid. The bare .rows egress stays
        // probe-free (the test above pins that surviving P3-2 case).
        ExecutionResult r = run(
                "meta::relational::metamodel::execute::executeInDb("
                + "'select 3 as V', $c, 0, 1000).rows->map(x|$x.V);}");
        assertEquals(1, probes(), "a by-name field read demands the"
                + " typed schema (one LIMIT-0); probe traffic: " + executed);
        Object v = r instanceof ExecutionResult.Scalar s ? s.value()
                : ((ExecutionResult.Collection) r).values().get(0);
        assertEquals(3L, ((Number) v).longValue());
    }

    @Test
    @DisplayName("columnNames demand: EXACTLY ONE probe (the second query is genuinely needed)")
    void columnNamesDemandsProbe() throws Exception {
        ExecutionResult r = run(
                "meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as X, 2 as Y', $c, 0, 1000).columnNames;}");
        assertEquals(1, probes(),
                "columnNames resolves statically — needs the LIMIT-0"
                + " probe; probe traffic: " + executed);
        List<Object> names =
                ((ExecutionResult.Collection) r).values();
        assertTrue(names.contains("X") && names.contains("Y"), names.toString());
    }
}
