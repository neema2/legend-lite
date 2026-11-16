// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.leansql;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;
import java.util.Set;

/**
 * THE PRODUCT'S ANSWERS, SNAPPED (a measurement, never a gate; O2's step 3, the orchestrator's condition (b),
 * 2026-11-13): every case of a fixed set -- {@code -Dlegend.lean_sql.channel_a}, channel A's cases -- planned and run
 * on each in-process database, its answer written as the product's JSON rows, or its refusal's class and first line.
 * One {@code answers-<database>.tsv} per database in the test's outputs: database, family, expression, answer. Two
 * snaps, before and after a lowering change, diff answer by answer. It always passes.
 */
class AnswerSnapshotTest {

    @Test
    void snap() throws Exception {
        List<String> lines = Files.readAllLines(com.legend.testing.Runfile.property("legend.lean_sql.channel_a"), StandardCharsets.UTF_8);
        Path out = Path.of(System.getenv("TEST_UNDECLARED_OUTPUTS_DIR"));
        for (TestBackend backend : TestBackends.inProcess()) {
            StringBuilder tsv = new StringBuilder();
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                Json.Obj c = Json.parseObject(line);
                String model = ((Json.Str) c.get("model")).value();
                String expression = ((Json.Str) c.get("expression")).value();
                String family = "refused";
                String answer;
                try {
                    TypedQuery q = Compiler.query(Compiler.compileModel(
                            LeanSqlBaseline.withRuntime(model, backend.type())), expression);
                    family = LeanSqlBaseline.family(q);
                    ExecutionPlan plan = q.executionPlan(StorelessRuntime.RUNTIME,
                            Set.of(com.legend.platform.Feature.CORRECT_SQL_SUBSTRING_INDEXING));
                    StringWriter w = new StringWriter();
                    try (Connection conn = backend.open()) {
                        java.util.UUID run = java.util.UUID.randomUUID();
                        com.legend.exec.PlanRunner.stream(plan, java.util.Map.of(), plan.sendsTheAnswer()
                                ? com.legend.exec.PlanSessions.acquired(com.legend.testbackend.TestSessions.over(conn),
                                        plan.answerSql().target())
                                : com.legend.exec.PlanSessions.refusing("a constant answer sends no statement"),
                                new com.legend.result.Output.JsonRows(w).writerFor(plan, run), run);
                    }
                    answer = w.toString();
                } catch (Exception | StackOverflowError e) {
                    String m = String.valueOf(e.getMessage());
                    int nl = m.indexOf('\n');
                    answer = "REFUSED " + e.getClass().getSimpleName() + ": " + (nl < 0 ? m : m.substring(0, nl));
                }
                tsv.append(backend.type()).append('\t').append(family).append('\t')
                        .append(flat(expression)).append('\t').append(flat(answer)).append('\n');
            }
            Files.writeString(out.resolve("answers-" + backend.type() + ".tsv"), tsv.toString(), StandardCharsets.UTF_8);
        }
    }

    private static String flat(String s) {
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\r", "\\r").replace("\n", "\\n");
    }
}
