// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.order;

import org.finos.legend.lite.pct.extension.PctBackend;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A LIST KEEPS ITS ORDER on every lane (2026-11-15; the lean program's order probe found Trino, Databricks and
 * BigQuery answering sorted lists out of order: a collect over a sorted subquery, and a UNION ALL of sorted branches,
 * relied on an order SQL never promises). Each case is an order-sensitive Pure expression, planned by lite for the
 * lane's database and run there; its rows must be Pure's answer, in Pure's order. A case the lane refuses at planning
 * is reported and passes (the lean baseline counts refusals); a wrong order, or a statement the database rejects,
 * fails. Run per lane: {@code bazel test //pct:pct_list_order_<lane>}.
 */
class ListOrderTest {

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
    void everyListKeepsItsOrder() throws Exception {
        String model = PctBackend.storelessRuntime();
        String runtime = com.legend.testbackend.StorelessRuntime.RUNTIME;
        List<String> wrong = new ArrayList<>();
        for (String[] c : CASES) {
            String expression = c[0];
            com.legend.executionplan.ExecutionPlan.Sql answer;
            try {
                answer = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), expression)
                        .executionPlan(runtime).answerSql();
            } catch (RuntimeException e) {
                System.out.println("[list-order] " + PctBackend.name() + " refused at planning: "
                        + expression.replace('\n', ' ') + " -- " + e.getMessage());
                continue;
            }
            String got;
            try (Connection conn = PctBackend.connect(); Statement st = conn.createStatement()) {
                for (String setup : answer.target().session()) {
                    st.execute(setup);
                }
                got = rows(st, answer.statement());
            } catch (java.sql.SQLException e) {
                wrong.add(expression.replace('\n', ' ') + " -> the database rejected it: " + e.getMessage());
                continue;
            }
            if (!got.equals(c[1])) {
                wrong.add(expression.replace('\n', ' ') + " -> " + got + ", expected " + c[1] + " | "
                        + answer.statement().replace('\n', ' '));
            }
        }
        assertTrue(wrong.isEmpty(), PctBackend.name() + ": " + wrong.size() + " list(s) out of order:\n  "
                + String.join("\n  ", wrong));
    }

    private static String rows(Statement st, String sql) throws java.sql.SQLException {
        StringBuilder rows = new StringBuilder();
        try (ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
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
        }
        return rows.toString();
    }
}
