// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SPIKE (H14, scratch, never merged): THE PRODUCT'S TEXT OF A FLOAT on the database {@code LEGENDLITE_PROBE_DATABASE}
 * names: each value's {@code toString()} as the database writes it, one {@code [h14-float]} line per value, and the
 * statement's characters. Measurement only: it asserts nothing.
 */
class H14FloatTextProbeTest {

    private static final String[] VALUES = {"2.0", "-2.0", "0.0", "-0.0", "1.5", "-1.25", "100.0", "101.0",
            "100000000000000000000.0", "0.0000001", "-0.0000001", "0.30000000000000004", "0.3333333333333333",
            "123456789.0", "0.000001", "1234567.89"};

    private static final String[] PARSED = {"NaN", "Infinity", "-Infinity"};

    @Test
    void floatTexts() throws Exception {
        String db = System.getenv("LEGENDLITE_PROBE_DATABASE");
        TestBackend backend = TestBackends.named(db == null ? "duckdb" : db);
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < VALUES.length; i++) {
            rows.append("  ").append(i).append(", ").append(VALUES[i]).append('\n');
        }
        report(backend, VALUES, "|#TDS\n  i:Integer, x:Float\n" + rows
                + "#->sort(~i->ascending())->extend(~t: r|$r.x->toOne()->toString())->select(~t)->from("
                + StorelessRuntime.RUNTIME + ")");
        // each parsed value in its own query: a database that cannot hold NaN refuses that one alone
        for (String v : PARSED) {
            report(backend, new String[] {v}, "|#TDS\n  i:Integer, x:String\n  0, " + v + "\n"
                    + "#->extend(~t: r|$r.x->toOne()->parseFloat()->toString())->select(~t)->from("
                    + StorelessRuntime.RUNTIME + ")");
        }
    }

    private static void report(TestBackend backend, String[] values, String query) {
        try {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")), query)
                    .executionPlan(StorelessRuntime.RUNTIME);
            String sql = plan.answerSql().statement();
            System.out.println("[h14-float-sql] " + backend.name() + " chars=" + sql.length() + " " + sql.replace("\n", " "));
            StringWriter out = new StringWriter();
            if (backend.opensJdbc()) {
                try (java.sql.Connection c = backend.open()) {
                    PlanRunner.stream(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                            PlanCases.Written.JSON.writer(out, plan.answerShape()), java.util.UUID.randomUUID());
                }
            } else {
                PlanRunner.stream(plan, Map.of(), PlanSessions.shared(), PlanCases.Written.JSON.writer(out,
                        plan.answerShape()), java.util.UUID.randomUUID());
            }
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"t\":(\"[^\"]*\"|null)").matcher(out.toString());
            List<String> got = new ArrayList<>();
            while (m.find()) {
                got.add(m.group(1));
            }
            for (int i = 0; i < values.length; i++) {
                System.out.println("[h14-float] " + backend.name() + " " + values[i] + " -> "
                        + (i < got.size() ? got.get(i) : "(missing)"));
            }
        } catch (Exception e) {
            System.out.println("[h14-float] " + backend.name() + " " + String.join(",", values) + " FAILED " + e.getClass().getSimpleName() + ": "
                    + String.valueOf(e.getMessage()).replace("\n", " "));
        }
    }
}
