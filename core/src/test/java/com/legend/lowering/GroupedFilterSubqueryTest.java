// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.exec.ExecutionResult;
import com.legend.exec.Row;
import com.legend.testbackend.TestBackend;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A filter with a subquery over sorted grouped rows (Lowerer.filter). Folded into the grouped select's HAVING, an
 * aggregate that reads no column ({@code COUNT(1)}) read by the subquery lands inside it, where no database can tell
 * whose rows it aggregates (the IQ scale program's fuzzer, 2026-10-22: DuckDB's Binder Error); such a filter reads the
 * grouped rows from outside. An independent review (2026-10-23) found the rule too broad and its isolation dropping
 * the sort: a subquery reading only a group key isolated too, and the rows came back ascending. Now only a subquery
 * holding such an aggregate, or a computed key (fuzz round 10, 2026-10-23), isolates, and the isolation keeps the
 * sort.
 */
class GroupedFilterSubqueryTest {

    private static String model(TestBackend backend) {
        return """
                ###Relational
                Database g::db
                (
                  Table T (ID INTEGER PRIMARY KEY, BT INTEGER)
                  Table U (ID INTEGER PRIMARY KEY, T_ID INTEGER)
                )
                ###Connection
                RelationalDatabaseConnection g::Conn { store: g::db; %s }
                ###Runtime
                Runtime g::RT { mappings: []; connections: [ g::db: [ c: g::Conn ] ]; }
                """.formatted(backend.connectionBody());
    }

    private static List<List<Long>> run(Connection conn, TestBackend backend, String query) throws Exception {
        ExecutionResult r = Execution.execute(model(backend), query, "g::RT", conn);
        List<List<Long>> rows = new ArrayList<>();
        for (Row row : ((ExecutionResult.Tabular) r).rows()) {
            rows.add(row.values().stream().map(v -> ((Number) v).longValue()).toList());
        }
        return rows;
    }

    /** The plan's SQL for {@code query}. */
    private static String sql(TestBackend backend, String query) {
        return Compiler.query(Compiler.compileModel(model(backend)), query).plan("g::RT").sql();
    }

    /** Whether the statement's outermost select orders its rows: its last line (the renderers put each clause of a
     *  select on a line of its own, a subselect's indented) is an ORDER BY. */
    private static boolean orderedOutermost(String sql) {
        String[] lines = sql.strip().split("\n");
        return lines[lines.length - 1].startsWith("ORDER BY");
    }

    @ParameterizedTest(name = "{0}: a subquery over sorted grouped rows keeps their order, and binds")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void aSubqueryOverSortedGroupedRows(TestBackend backend) throws Exception {
        try (Connection conn = backend.open()) {
            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, BT INTEGER)");
                st.execute("CREATE TABLE U (ID INTEGER PRIMARY KEY, T_ID INTEGER)");
                st.execute("INSERT INTO T VALUES (1, 10), (2, 20), (3, 10), (4, 30), (5, 30), (6, 30)");
                st.execute("INSERT INTO U VALUES (2, 1), (3, 3), (9, 9), (16, 0), (36, 0)");
            }
            // the subquery reads a group key alone: the HAVING form, the sort kept (the review's input)
            String byKey = "|#>{g::db.T}#->groupBy(~ID, ~c: x|1 : y|$y->count())->sort(~ID->descending())"
                    + "->filter(r|#>{g::db.U}#->filter(u|$u.T_ID == $r.ID)->isNotEmpty())";
            String plan = sql(backend, byKey);
            assertTrue(plan.contains("HAVING") && orderedOutermost(plan), backend + ": HAVING, the sort kept\n" + plan);
            assertEquals(List.of(List.of(3L, 1L), List.of(1L, 1L)), run(conn, backend, byKey),
                    backend + ": the rows in the sort's order");
            // the subquery reads COUNT(1): from outside, the sort kept
            String byCount = "|#>{g::db.T}#->groupBy(~BT, ~c: x|1 : y|$y->count())->sort(~BT->descending())"
                    + "->filter(r|#>{g::db.U}#->filter(u|$u.ID == $r.c)->isNotEmpty())";
            plan = sql(backend, byCount);
            assertTrue(!plan.contains("HAVING") && orderedOutermost(plan), backend + ": no HAVING, the sort kept\n"
                    + plan);
            assertEquals(List.of(List.of(30L, 3L), List.of(10L, 2L)), run(conn, backend, byCount),
                    backend + ": the groups whose count is a U id, in the sort's order");
            // the subquery reads a computed key: from outside, the sort kept (fuzz round 10, 2026-10-23: in HAVING,
            // DuckDB's "column BT must appear in the GROUP BY clause")
            String byComputedKey = "|#>{g::db.T}#->extend(~k:x|$x.BT->toOne() + 6)"
                    + "->groupBy(~k, ~c: x|$x.ID : y|$y->count())->sort(~k->descending())"
                    + "->filter(r|#>{g::db.U}#->filter(u|$u.ID == $r.k)->isNotEmpty())";
            plan = sql(backend, byComputedKey);
            assertTrue(!plan.contains("HAVING") && orderedOutermost(plan), backend + ": no HAVING, the sort kept\n"
                    + plan);
            assertEquals(List.of(List.of(36L, 3L), List.of(16L, 2L)), run(conn, backend, byComputedKey),
                    backend + ": the groups whose key is a U id, in the sort's order");
        }
    }
}
