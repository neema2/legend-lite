// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.RecordingConnection;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE RUNNER SENDS A PLAN'S TEXT AS IT IS (the tenets re-audit's B-6, 2026-11-09: the type hole's census checked
 * names only): each statement a session is sent while a plan runs, its per-execution trace comment set aside (written at
 * run until D3, the total wall's M5), is text the plan carries -- its answer's statement, its target's session
 * statements, a load's statements and staging -- character for character, for every parameter kind on every database
 * this process runs. A keyed body (the per-key regions the runner writes, the charter's A3(a)) is not among these cases.
 */
class SentStatementsAreThePlansTest {

    @Test
    void everySentStatementIsThePlansText() throws Exception {
        int checked = 0;
        for (DatabaseType type : PlanMakerTest.IN_PROCESS) {
            for (PlanCases.Parameterised q : PlanCases.scalars("T")) {
                ExecutionPlan plan = Compiler.query(Compiler.compileModel(PlanMakerTest.model(type.name())),
                        q.withParameters()).executionPlan("s::RT");
                Set<String> planned = texts(plan);
                List<String> sent = new ArrayList<>();
                try (Connection c = RecordingConnection.wrap(TestBackends.of(type).open(), sent)) {
                    PlanCases.write(plan, c, q.values(), PlanCases.Written.JSON::writer);
                }
                assertFalse(sent.isEmpty(), type + " " + q.withParameters());
                for (String s : sent) {
                    String statement = unstamped(s);
                    assertTrue(planned.contains(statement), type + " " + q.withParameters() + ": sent a statement the"
                            + " plan does not carry:\n" + statement + "\nthe plan's:\n" + String.join("\n---\n", planned));
                    checked++;
                }
                assertTrue(sent.stream().map(SentStatementsAreThePlansTest::unstamped)
                        .anyMatch(plan.answerSql().statement()::equals), "the answer's statement was sent as written");
            }
        }
        assertTrue(checked > 50, "too few statements checked to prove anything: " + checked);
    }

    /** The statement {@code sent} with the per-execution trace comment the runner writes before it set aside (D3). */
    private static String unstamped(String sent) {
        String prefix = ExecutionPlan.traceComment(java.util.UUID.randomUUID());
        String head = prefix.substring(0, prefix.indexOf(':') + 1);
        if (sent.startsWith(head)) {
            int nl = sent.indexOf('\n');
            assertTrue(nl > 0, sent);
            assertEquals(prefix.length(), nl, "a trace comment's line: " + sent);
            return sent.substring(nl + 1);
        }
        return sent;
    }

    /** Every text {@code plan} carries for a session: its answer's statement, its target's session statements, and each
     *  load's statements and staging. */
    private static Set<String> texts(ExecutionPlan plan) {
        Set<String> out = new LinkedHashSet<>();
        ExecutionPlan.Target target = plan.answerSql().target();
        out.add(plan.answerSql().statement());
        out.addAll(target.session());
        for (ExecutionPlan.Load load : target.loads()) {
            out.addAll(load.target().session());
            for (ExecutionPlan.LoadStep step : load.steps()) {
                switch (step) {
                    case ExecutionPlan.LoadStep.Statement s -> out.add(s.sql());
                    case ExecutionPlan.LoadStep.Statements st -> out.addAll(com.legend.executionplan.DataRef.Statements
                            .decode(load.content().get(st.ref().digest())));
                    case ExecutionPlan.LoadStep.File f -> throw new IllegalStateException("no file in these plans");
                }
            }
        }
        return out;
    }
}
