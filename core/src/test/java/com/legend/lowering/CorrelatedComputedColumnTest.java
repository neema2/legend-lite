// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.testing.Lean;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.SqlIslandCases;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A computed column inside a correlated subquery reads the enclosing row. Until 2026-10-14 {@code tryComputedColumns}
 * resolved a column lambda's reads by name against the base select alone, so a correlated scalar subquery whose select
 * list read the outer row inside an expression ({@code SELECT max(x.age) + e.ID FROM ... x} under {@code FROM ... e})
 * threw "unresolvable even after isolation" (the tenth line's find, compiling real-world queries), and a read named
 * like a base column resolved to the base's, silently (the outer row's `e.firstName` read plain in the select list
 * came out as the inner row's). It now resolves as a predicate's reads do
 * ({@code scopedResolver}): the lambda's own row against the base, any other variable through the enclosing scopes.
 */
class CorrelatedComputedColumnTest {

    /** The platform's own engine: the table, the island's session and the answer it is held to. */
    private static final TestBackend PLATFORM = TestBackends.platform();

    private static final String MODEL =
            SqlIslandCases.model(PLATFORM.connectionBody());
    private static final String EMP = "tb('store::HR.EMP')";
    private static Connection session;

    @BeforeAll
    static void seed() throws SQLException {
        session = PLATFORM.open();
        try (Statement s = session.createStatement()) {
            s.execute(SqlIslandCases.table(PLATFORM));
            s.execute("INSERT INTO EMP VALUES " + SqlIslandCases.ROWS);
        }
    }

    @AfterAll
    static void close() throws SQLException {
        session.close();
    }

    /** The island's answer to {@code sql} (its tables spelled through the source function) as rows of cell texts. */
    private static List<List<String>> island(String sql) throws SQLException {
        Lean.Result result = Lean.run(MODEL, "|#SQL{" + sql + "}#",
                SqlIslandCases.RUNTIME, session);
        List<List<String>> rows = new ArrayList<>();
        for (var row : result.rows()) {
            List<String> cells = new ArrayList<>();
            for (Object value : row) {
                cells.add(SqlIslandCases.cell(value));
            }
            rows.add(cells);
        }
        return rows;
    }

    /** The platform database's own answer to the same SQL over the same table: the referee, no snap needed. */
    private static List<List<String>> direct(String sql) throws SQLException {
        List<List<String>> rows = new ArrayList<>();
        try (Statement s = session.createStatement(); ResultSet rs = s.executeQuery(sql.replace(EMP, "EMP"))) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> cells = new ArrayList<>();
                for (int i = 1; i <= n; i++) {
                    cells.add(SqlIslandCases.cell(rs.getObject(i)));
                }
                rows.add(cells);
            }
        }
        return rows;
    }

    @Test
    @DisplayName("a correlated subquery's select list reads the outer row inside an expression")
    void theOuterRowInsideAnExpression() throws SQLException {
        for (String sql : List.of(
                "SELECT e.ID, (SELECT max(x.age) + e.ID FROM " + EMP + " x) AS s FROM " + EMP + " e ORDER BY e.ID",
                "SELECT e.ID, (SELECT max(x.age) + e.age FROM " + EMP + " x WHERE x.dept = e.dept) AS s FROM " + EMP
                        + " e ORDER BY e.ID",
                // the outer row read plain in the select list (the island spells it as a computed column too): before
                // the fix it resolved to the inner x's firstName, the first row's on every row, silently
                "SELECT e.ID, (SELECT e.firstName FROM " + EMP + " x WHERE x.ID = 1) AS s FROM " + EMP
                        + " e ORDER BY e.ID",
                // the outer row read in the WHERE only: worked before, kept
                "SELECT e.ID, (SELECT max(x.age) FROM " + EMP + " x WHERE x.dept = e.dept) AS s FROM " + EMP
                        + " e ORDER BY e.ID")) {
            assertEquals(direct(sql), island(sql), sql);
        }
    }

    @Test
    @DisplayName("the plain-Pure spelling: an extend inside a correlated relation reads the enclosing row")
    void thePlainPureSpelling() throws SQLException {
        // each person's top order amount plus the person's own id; no order is SQL's NULL
        Lean.Result result = Lean.run(MODEL, """
                |#TDS
                id, name
                1, Ann
                2, Bob
                3, Cy
                #->extend(~[top:p|#TDS
                person, amount
                1, 7
                1, 3
                2, 9
                #->filter(o|$o.person == $p.id)->groupBy(~[person], ~[m:o|$o.amount:y|$y->max()])
                ->extend(~[v:x|$x.m->toOne() + $p.id])->map(x|$x.v)->toOne()])
                ->select(~[id, top])->sort(ascending(~id))""",
                SqlIslandCases.RUNTIME, session);
        // an Integer cell is a long, the plan decode's carrier of Pure's Integer (the legacy executor handed an int; 2026-10-27,
        // the result boundary program's phase 5, a correction of the old decoder)
        assertEquals(List.of(8L, 11L), result.rows().subList(0, 2).stream().map(r -> r.get(1)).toList());
        assertEquals(null, result.rows().get(2).get(1), "no order: SQL's NULL");
    }

    @Test
    @DisplayName("a column named like the base's resolves to the enclosing row it reads, never the base's")
    void aSameNamedColumnIsTheEnclosingRows() throws SQLException {
        // the inner relation has an `id` too: $p.id is the outer person's, never the order's
        Lean.Result result = Lean.run(MODEL, """
                |#TDS
                id, name
                1, Ann
                2, Bob
                #->extend(~[tag:p|#TDS
                id, person
                100, 1
                200, 2
                #->filter(o|$o.person == $p.id)->extend(~[v:o|$p.id * 10 + $o.person])->map(o|$o.v)->toOne()])
                ->select(~[id, tag])->sort(ascending(~id))""", SqlIslandCases.RUNTIME, session);
        // an Integer cell is a long, the plan decode's carrier of Pure's Integer (the legacy executor handed an int; 2026-10-27,
        // the result boundary program's phase 5, a correction of the old decoder)
        assertEquals(List.of(11L, 22L), result.rows().stream().map(r -> r.get(1)).toList());
    }
}
