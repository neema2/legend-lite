// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.test.StorelessRuntime;
import com.legend.testplan.TestDoor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE VERDICT IDS (the plan-as-data design's step 0, docs/PLAN_AS_DATA_2026_11_07.md, 2026-11-08): under each judge,
 * every verdict of a body's plan has an id no other verdict of that plan has, and the ids survive the plan's JSON --
 * the references that replace the run's identity maps (a database judge's members with equal names are equal records
 * but for their ids).
 */
class VerdictIdsTest {

    private static final String BODY = "{| assertEquals(1, 1); assertSize([1, 2], 2); assertEquals('a', 'a');"
            + " assertSize([3], 1); assert(1 < 2);}";

    @Test
    void everyVerdictOfABodysPlanHasItsOwnIdUnderEachJudge() {
        ModelContext ctx = Compiler.compileModel(StorelessRuntime.with("", Databases.PLATFORM));
        TypedQuery typed = typed(ctx, BODY);
        String rt = StorelessRuntime.RUNTIME;
        for (ExecuteOptions.JudgeMode mode : ExecuteOptions.JudgeMode.values()) {
            ExecuteOptions options = ExecuteOptions.NONE.withJudgeMode(mode);
            BodyDriver.ExecEnv env = BodyDriver.bodyEnv(typed, rt, Databases.dialect(Databases.PLATFORM), null, null,
                    options);
            TestDoor.Asserts asserts = switch (mode) {
                case HOST -> HostAsserts.plan(typed, rt, env);
                case DATABASE -> DatabaseAsserts.plan(typed, rt, env);
                case BOTH -> AgreementAsserts.plan(typed, rt, env, null, false);
            };
            assertNotNull(asserts, mode + ": the body's asserts planned");
            ExecutionPlan plan = TestDoor.planned(typed, rt, options.features(), asserts).plan();
            List<ExecutionPlan.Verdict> verdicts = new ArrayList<>();
            collect(plan.root(), verdicts);
            assertFalse(verdicts.isEmpty(), mode + ": the plan holds verdicts");
            Set<Integer> ids = new HashSet<>();
            for (ExecutionPlan.Verdict v : verdicts) {
                assertTrue(v.id() >= 1, mode + ": " + v.name() + "'s id");
                assertTrue(ids.add(v.id()), mode + ": " + v.name() + "'s id " + v.id() + " is another verdict's");
            }
            // the ids are the plan's, read back from its JSON as written
            List<ExecutionPlan.Verdict> read = new ArrayList<>();
            collect(PlanJson.read(PlanJson.write(plan)).root(), read);
            assertEquals(verdicts.stream().map(ExecutionPlan.Verdict::id).toList(),
                    read.stream().map(ExecutionPlan.Verdict::id).toList(), mode + ": the ids through the JSON");
        }
    }

    private static void collect(ExecutionPlan.Node node, List<ExecutionPlan.Verdict> into) {
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> collect(step, into));
            case ExecutionPlan.Verdicts g -> into.addAll(g.verdicts());
            case ExecutionPlan.Branch b -> {
                b.then().forEach(step -> collect(step, into));
                b.otherwise().forEach(step -> collect(step, into));
            }
            default -> {
                // a step that holds no verdict
            }
        }
    }

    private static TypedQuery typed(ModelContext ctx, String body) {
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        return Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx));
    }
}
