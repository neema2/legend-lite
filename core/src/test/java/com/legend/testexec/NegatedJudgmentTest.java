// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testexec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.database.Databases;
import com.legend.error.AssertFailed;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.BodyFacts;
import com.legend.testexecutionplan.HostJudgment;
import com.legend.testexecutionplan.TestPlan;
import com.legend.testexecutionplan.TypedSide;
import com.legend.testexecutionplan.Verdicts;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * A NEGATED ASSERT DECLINES WHERE ITS POSITIVE JUDGMENT DECLINES (the PR #118 audit's JUDGES-8, track A5, 2026-11-19):
 * {@code assertNotContains} is {@code assertContains}' judgment negated (#28); where the positive judgment declines the
 * sides it read (a value that is not one value), the negation declines too -- never read as "the positive held", which
 * failed the assert with "should not contain" words where the assert was not judged at all. Latent today: the planner
 * keeps the shape out ({@code AssertVerdicts}' exactlyOneValue), so this is the judgment's own contract, read directly.
 */
final class NegatedJudgmentTest {

    private static final HostJudgment.SideFacts INTEGERS = new HostJudgment.SideFacts(null, null, false, false, false,
            "Integer", "Integer");

    private static final HostJudgment NOT_CONTAINS = new HostJudgment.Negated(
            new HostJudgment.Contains(INTEGERS, INTEGERS), "NOT_CONTAINS");

    /** A run that hears no appeal, as lite's runs are opened (NoEdgeRun), over a plan of one step (never run here). */
    private static BodyRun run() {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), List.of(), List.of(), null, null,
                null);
        ExecutionPlan.TdsResult one = new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION,
                List.of(new ExecutionPlan.Column("value", "Integer", new ExecutionPlan.Multiplicity(1, 1), "INTEGER",
                        null, null)), ExecutionPlan.Sql.of("SELECT 1 AS value", target));
        TestPlan plan = new TestPlan(List.of(), List.of(new TestPlan.Run(one)), TestPlan.Judge.HOST, Map.of(),
                Map.of(), false, target, new BodyFacts(false, false, true, "A"), null, TestPlan.LITE_WORDS);
        return new BodyRun(NoEdgeRun.NONE.open(plan, null, UUID.randomUUID()));
    }

    private static List<TypedSide> sides(List<Object> collection, List<Object> value) {
        return List.of(new TypedSide.Collection(collection, "Integer"), new TypedSide.Collection(value, "Integer"));
    }

    @Test
    void aNegatedJudgmentDeclinesWhereItsInnerJudgmentDeclines() {
        // two values where the membership takes one: the positive judgment declines (null), so the negation does
        assertNull(HostJudgments.judge(NOT_CONTAINS, sides(List.of(1L, 2L), List.of(1L, 3L)), run()),
                "a declined membership's negation is declined, not judged");
        assertNull(HostJudgments.judge(NOT_CONTAINS, sides(List.of(1L, 2L), List.of()), run()),
                "an empty value's membership declines, and its negation with it");
    }

    @Test
    void aNegatedJudgmentJudgesWhereItsInnerJudges() {
        // one value: judged, and negated -- absent holds, present fails with the negation's words
        assertEquals(Verdicts.ok(), HostJudgments.judge(NOT_CONTAINS, sides(List.of(1L, 2L), List.of(3L)), run()));
        assertThrows(AssertFailed.class,
                () -> HostJudgments.judge(NOT_CONTAINS, sides(List.of(1L, 2L), List.of(2L)), run()));
    }
}
