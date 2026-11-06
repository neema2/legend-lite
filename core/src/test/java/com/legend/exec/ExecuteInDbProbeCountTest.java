// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.database.Databases;
import com.legend.testbackend.TestBackends;
import com.legend.test.StorelessRuntime;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A raw grid read at planning: NEVER (the planner-driver seam program's S3c, 2026-10-30; until then the LIMIT-0 schema
 * probe ran when the tree demanded a late-bound grid's schema, P3-2's single-query pin). An undemanded grid executes
 * as ONE query whose columns are the ones its result reports; a by-name read names its column; {@code columnNames}
 * and {@code values} are a keyed plan whose keys step (one {@code LIMIT 0}) runs with the plan, never at planning.
 * This test counts actual JDBC traffic through a proxy and the statement-origin census.
 */
class ExecuteInDbProbeCountTest {

    private static final String CONN_LET =
            "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                    + "type=meta::relational::runtime::DatabaseType." + Databases.PLATFORM.name() + ");\n";

    private static Connection real;

    @BeforeAll
    static void open() throws Exception {
        real = TestBackends.platform().open();
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

    private static Lean.Result run(String tail) throws Exception {
        executed = new ArrayList<>();
        return Program.last(StorelessRuntime.with("", Databases.PLATFORM), CONN_LET + tail, StorelessRuntime.RUNTIME, counting(real));
    }

    @Test
    @DisplayName("undemanded grid as relation root: ZERO probes, headers adopted at egress")
    void undemandedGridSingleQuery() throws Exception {
        Lean.Result r = run(
                "let r = meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as X, 2 as Y', $c, 0, 1000); $r.rows;}");
        assertEquals(0, probes(),
                "no columnNames/values demand — the one executed query is"
                + " its own schema authority; probe traffic: " + executed);
        Lean.Result t = r;
        assertEquals(List.of("X", "Y"),
                t.columns().stream().map(Lean.Column::name).toList(),
                "late-bound egress arm adopts the result-set headers");
        assertEquals(1, t.rows().size());
        assertEquals(1L, ((Number) t.rows().get(0).get(0)).longValue());
        assertEquals(2L, ((Number) t.rows().get(0).get(1)).longValue());
    }

    @Test
    @DisplayName("named cell read: ZERO probes, the cell typed by the wire at the row")
    void namedReadProbesNothing() throws Exception {
        // the planner-driver seam program's S3c (2026-10-30; the user's D76: "no step where nothing needs the names
        // before the grid runs"): a by-name field read over a late-bound grid names its column, the database resolves
        // it, and the cell is decoded by the wire -- its column carries no slot (ColumnFacts.rawGridCell), so the
        // runner reads the driver's own kind. Until then (the §4bZ-U ruling of 2026-08-25) the read demanded one
        // LIMIT-0 probe at planning to type the cell; the cell is typed the same, from the real run.
        Lean.Result r = run(
                "meta::relational::metamodel::execute::executeInDb("
                + "'select 3 as V', $c, 0, 1000).rows->map(x|$x.V);}");
        assertEquals(0, probes(), "a by-name field read demands no schema at planning; probe traffic: "
                + executed);
        Object v = r.values().get(0);
        assertEquals(3L, ((Number) v).longValue());
    }

    @Test
    @DisplayName("named cell read: a text that looks like JSON is the text its column reports")
    void namedReadOfTextIsTheText() throws Exception {
        // the seam program's S3c audit (2026-10-31): a cell read by name is decoded by the type its result reports
        // for its column (VARCHAR here), never guessed from its value -- a text starting with '[' or '{' was parsed as
        // JSON (a JSON error for '[draft]', a list for '[1]')
        assertEquals(List.of("[draft]", "[1]", "{x}"), run(
                "meta::relational::metamodel::execute::executeInDb("
                + "'select \\'[draft]\\' as N union all select \\'[1]\\' union all select \\'{x}\\'', $c, 0,"
                + " 1000).rows->map(x|$x.N);}").values());
    }

    @Test
    @DisplayName("columnNames: nothing read at planning; the names the grid's result reports, its keys step")
    void columnNamesProbesNothing() throws Exception {
        // the planner-driver seam program's S3c (2026-10-30; D76): the names are a keyed plan's keys, the grid's
        // reported columns read when it runs, and the body a region per name; until then a LIMIT-0 probe at planning
        // stamped them into the program
        long before = StatementOrigin.snapshot()[StatementOrigin.PROBE.ordinal()];
        Lean.Result r = run(
                "meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as X, 2 as Y', $c, 0, 1000).columnNames;}");
        assertEquals(1, probes(), "one LIMIT 0: the keyed plan's keys step, sent as the plan runs; traffic: "
                + executed);
        assertEquals(before, StatementOrigin.snapshot()[StatementOrigin.PROBE.ordinal()], "nothing read at planning");
        List<Object> names = r.values();
        assertEquals(List.of("X", "Y"), names.stream().map(n -> String.valueOf(n).toUpperCase(Locale.ROOT)).toList(),
                names.toString());
    }

    @Test
    @DisplayName("values: nothing read at planning; the grid's cells row-major, each of its own kind")
    void valuesProbesNothing() throws Exception {
        long before = StatementOrigin.snapshot()[StatementOrigin.PROBE.ordinal()];
        Lean.Result r = run(
                "meta::relational::metamodel::execute::executeInDb("
                + "'select 1 as X, \\'a\\' as Y', $c, 0, 1000).rows.values;}");
        assertEquals(1, probes(), "one LIMIT 0: the keyed plan's keys step, sent as the plan runs; traffic: "
                + executed);
        assertEquals(before, StatementOrigin.snapshot()[StatementOrigin.PROBE.ordinal()], "nothing read at planning");
        assertEquals(List.of(1L, "a"), r.values());
    }
}
