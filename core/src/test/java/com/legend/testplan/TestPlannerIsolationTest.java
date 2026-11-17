// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.TestPlanJson;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * BOTH JUDGES, ISOLATED (the user's judge model, 2026-11-17; (b')): under BOTH the planner makes ONE test plan holding
 * two independent plans, the host judge's and the database judge's, at one step. Each is BYTE-IDENTICAL to what that
 * judge plans alone (its JSON written the same), so neither judge's plan carries anything of the other's: no host
 * column in a database statement, no fused agreement.
 */
class TestPlannerIsolationTest {

    private static ModelContext ctx;

    @BeforeAll
    static void setUp() {
        ctx = Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "{| assertEquals(2, 1 + 1); assertEquals('ab', 'a' + 'b');}",
        "{| assertEquals(2, 1 + 1); assertEquals(3, 1 + 1);}",
        "{| assertEquals(2, 1 + 1); assertEquals(#TDS\na\n2\n#, #TDS\na\n1\n2\n#->filter(r|$r.a > 1));}",
        "{| assertEquals(1, [1, 2, 3]->filter(x|$x > 2)->size()); assertEquals(1, 1 + 0);}",
        "{| assertEquals(2, 1 + 1); assertError(|assertEquals(1, 2), 'x');}",
        "{| assertSize([1, 2, 3], 3); assertEquals([1, 2], [2, 1]->sort());}"
    })
    void eachJudgesPlanIsTheOneItMakesAlone(String body) {
        TypedQuery typed = typed(body);
        PlanningOptions both = ExecuteOptions.NONE.withJudgeMode(PlanningOptions.JudgeMode.BOTH).planning();
        TestPlan isolated = TestPlanner.planIsolated(typed, StorelessRuntime.RUNTIME, both);
        assertEquals(TestPlan.Judge.AGREEMENT, isolated.judge());
        assertEquals(1, isolated.steps().size(), "one step: the pair");
        TestPlan.Isolated pair = assertInstanceOf(TestPlan.Isolated.class, isolated.steps().get(0));

        TestPlan hostAlone = TestPlanner.plan(typed, StorelessRuntime.RUNTIME,
                both.withJudgeMode(PlanningOptions.JudgeMode.HOST));
        TestPlan databaseAlone = TestPlanner.plan(typed, StorelessRuntime.RUNTIME,
                both.withJudgeMode(PlanningOptions.JudgeMode.DATABASE));
        assertEquals(TestPlanJson.write(hostAlone), TestPlanJson.write(pair.host()), body + ": the host's plan");
        assertEquals(TestPlanJson.write(databaseAlone), TestPlanJson.write(pair.database()),
                body + ": the database's plan");
        assertEquals(TestPlanJson.write(isolated), TestPlanJson.write(TestPlanJson.read(TestPlanJson.write(isolated))),
                body + ": the pair reads back as written");
    }

    private static TypedQuery typed(String body) {
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        return Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx));
    }
}
