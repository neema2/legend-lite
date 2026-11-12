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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE RUNNER SENDS A PLAN'S TEXT AS IT IS (the tenets re-audit's B-6, 2026-11-09: the type hole's census checked
 * names only): each statement a session is sent while a plan runs is text the plan carries -- its answer's statement,
 * its target's session statements, a load's statements and staging -- character for character, for every parameter kind
 * on every database this process runs. Nothing is set aside: the per-execution trace comment the seam wrote in front of
 * every statement until C1 (2026-11-12, D3) is gone, the execution id a bound value. The one sanctioned run-time
 * writing is a keyed body's per-key regions (the charter's A3(a)), not among these cases.
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
                } catch (com.legend.error.DataError refused) {
                    // a value finer than its slot keeps, refused by the statement's own raise, which was sent as the
                    // plan wrote it (S181, A7, 2026-11-10: DuckDB's microsecond slot and the nanosecond case)
                    assertTrue(String.valueOf(refused.getMessage()).contains("refused, never cut (S181)"),
                            type + " " + q.withParameters() + ": " + refused.getMessage());
                }
                assertFalse(sent.isEmpty(), type + " " + q.withParameters());
                for (String statement : sent) {
                    assertTrue(planned.contains(statement), type + " " + q.withParameters() + ": sent a statement the"
                            + " plan does not carry:\n" + statement + "\nthe plan's:\n" + String.join("\n---\n", planned));
                    checked++;
                }
                assertTrue(sent.stream().anyMatch(plan.answerSql().statement()::equals),
                        "the answer's statement was sent as written");
            }
        }
        assertTrue(checked > 50, "too few statements checked to prove anything: " + checked);
    }

    /** Every text {@code plan} carries for a session: its answer's statement, its target's session statements, and each
     *  load's statements and staging. */
    private static Set<String> texts(ExecutionPlan plan) {
        Set<String> out = new LinkedHashSet<>();
        ExecutionPlan.Target target = plan.answerSql().target();
        out.add(plan.answerSql().statement());
        out.addAll(target.session());
        // the error read-back the target declares, sent after a failure (C2, 2026-11-12: MariaDB's and MySQL's)
        if (target.raiseReadBack() != null) {
            out.add(target.raiseReadBack().read());
            out.add(target.raiseReadBack().reset());
        }
        for (ExecutionPlan.Load load : target.loads()) {
            out.addAll(load.target().session());
            for (ExecutionPlan.LoadStep step : load.steps()) {
                switch (step) {
                    case ExecutionPlan.LoadStep.Statement s -> out.add(s.sql());
                    case ExecutionPlan.LoadStep.Statements st -> out.addAll(com.legend.executionplan.DataRef.Statements
                            .decode(load.content().get(st.ref().digest())));
                    case ExecutionPlan.LoadStep.File f -> throw new IllegalStateException("no file in these plans");
                    case ExecutionPlan.LoadStep.Bound b -> throw new IllegalStateException("no host's bound statement"
                            + " in these plans");
                    case ExecutionPlan.LoadStep.Stream r -> throw new IllegalStateException("no host's stream in these"
                            + " plans");
                    case ExecutionPlan.LoadStep.Attach a -> throw new IllegalStateException("no host's attached value in"
                            + " these plans");
                }
            }
        }
        return out;
    }
}
