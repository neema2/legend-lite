// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.probe;

import org.finos.legend.lite.pct.extension.PctBackend;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * THE LEAN PROGRAM'S TWO OPEN ORDER ITEMS (measure only, 2026-11-15; the orchestrator's order, step 2): a database
 * without lists writes an explode as a UNION ALL, and SQL promises no order for one. {@code CarrierStrategies}
 * orders a union of one-row literal branches by its branches' positions (lane round 52's fix); two shapes are left as
 * they are and named open: (A) a union of MULTI-ROW branches (a collect's rows, concatenated) and (B) a union whose
 * branches read the ENCLOSING row. Each case is a Pure expression whose answer depends on that order, its expected
 * answer (Pure's: a concatenation keeps its sides' order) beside it; the value is planned by lite for the lane's own
 * database (no SQL spelled here), and each line is printed as {@code [order-probe]} with the rows the database
 * answered, then {@code AS EXPECTED} or {@code ORDER DIFFERS}. The test always passes: it decides nothing. Run per
 * lane: {@code bazel test //pct:pct_union_order_probe_<lane> --test_output=all}.
 */
class UnionOrderProbeTest {

    private static final String TDS = "#TDS\n  v\n  3\n  1\n  2\n#";

    /** expression, expected rows (each row's cells joined by ',', rows by ';'). */
    private static final List<String[]> CASES = List.of(
            // (A) multi-row branches: a sorted collect concatenated with another, then with a literal
            new String[] {"|" + TDS + "->sort(~v->ascending())->map(r|$r.v)->concatenate("
                    + TDS + "->sort(~v->descending())->map(r|$r.v))", "1;2;3;3;2;1"},
            new String[] {"|[9, 8]->concatenate(" + TDS + "->sort(~v->ascending())->map(r|$r.v))", "9;8;1;2;3"},
            new String[] {"|" + TDS + "->sort(~v->descending())->map(r|$r.v)->concatenate([7, 0])", "3;2;1;7;0"},
            new String[] {"|" + TDS + "->sort(~v->ascending())->map(r|$r.v)->concatenate("
                    + TDS + "->sort(~v->descending())->map(r|$r.v))->filter(x|$x > 1)", "2;3;3;2"},
            // (A) the same lists read as ONE value (an explode under a reduction or a positional read)
            new String[] {"|" + TDS + "->sort(~v->ascending())->map(r|$r.v)->concatenate("
                    + TDS + "->sort(~v->descending())->map(r|$r.v))->map(x|$x->toString())->joinStrings(',')",
                    "1,2,3,3,2,1"},
            new String[] {"|[9, 8]->concatenate(" + TDS + "->sort(~v->ascending())->map(r|$r.v))"
                    + "->map(x|$x->toString())->joinStrings(',')", "9,8,1,2,3"},
            new String[] {"|" + TDS + "->sort(~v->ascending())->map(r|$r.v)->concatenate("
                    + TDS + "->sort(~v->descending())->map(r|$r.v))->at(3)", "3"},
            new String[] {"|[9, 8]->concatenate(" + TDS + "->sort(~v->ascending())->map(r|$r.v))->at(2)", "1"},
            // (B) branches that read the enclosing row: a per-row list of its own cells, order-sensitive
            new String[] {"|#TDS\n  a, b, c\n  1, 2, 3\n  6, 5, 4\n#->sort(~a->ascending())"
                    + "->extend(~s:r|[$r.c, $r.a, $r.b]->map(x|$x->toString())->joinStrings(','))->select(~s)",
                    "3,1,2;4,6,5"},
            new String[] {"|#TDS\n  a, b, c\n  1, 2, 3\n  6, 5, 4\n#->sort(~a->ascending())"
                    + "->extend(~s:r|[$r.c, $r.a, $r.b]->filter(x|$x > 1)->at(0))->select(~s)", "3;4"},
            new String[] {"|#TDS\n  a, b, c\n  1, 2, 3\n  6, 5, 4\n#->sort(~a->ascending())"
                    + "->extend(~s:r|[$r.c, $r.a, $r.b]->filter(x|$x > 1)->map(x|$x->toString())"
                    + "->joinStrings(','))->select(~s)", "3,2;4,6,5"});

    @Test
    void probe() throws Exception {
        String model = PctBackend.storelessRuntime();
        String runtime = com.legend.testbackend.StorelessRuntime.RUNTIME;
        System.out.println("[order-probe] lane " + PctBackend.name());
        for (String[] c : CASES) {
            String expression = c[0];
            String flat = expression.replace('\n', ' ');
            String sql;
            List<String> session;
            try {
                com.legend.executionplan.ExecutionPlan.Sql answer = com.legend.Compiler.query(
                        com.legend.Compiler.compileModel(model), expression).executionPlan(runtime).answerSql();
                sql = answer.statement();
                session = answer.target().session();
            } catch (RuntimeException e) {
                System.out.println("[order-probe] " + flat + " | refused at planning: " + e.getMessage());
                continue;
            }
            System.out.println("[order-probe] " + flat + " | sql: " + sql.replace('\n', ' '));
            try (Connection conn = PctBackend.connect(); Statement st = conn.createStatement();
                    ResultSet rs = runIn(st, session, sql)) {
                int n = rs.getMetaData().getColumnCount();
                StringBuilder rows = new StringBuilder();
                while (rs.next()) {
                    if (rows.length() > 0) {
                        rows.append(';');
                    }
                    for (int i = 1; i <= n; i++) {
                        if (i > 1) {
                            rows.append(',');
                        }
                        rows.append(rs.getString(i));
                    }
                }
                String got = rows.toString();
                System.out.println("[order-probe]   rows: " + got + " | expected: " + c[1] + " | "
                        + (got.equals(c[1]) ? "AS EXPECTED" : "ORDER DIFFERS"));
            } catch (java.sql.SQLException e) {
                System.out.println("[order-probe]   the statement raised: " + e.getMessage());
            }
        }
    }

    /** {@code sql}'s rows, the plan's target's session statements sent first (MariaDB's quoting, for one). */
    private static ResultSet runIn(Statement st, List<String> session, String sql) throws java.sql.SQLException {
        for (String setup : session) {
            st.execute(setup);
        }
        return st.executeQuery(sql);
    }
}
