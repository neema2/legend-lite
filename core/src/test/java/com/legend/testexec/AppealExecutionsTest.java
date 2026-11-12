// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Executions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.exec.PlanSessions;
import com.legend.testbackend.TestBackends;
import com.legend.testexecutionplan.Appeal;
import com.legend.testexecutionplan.RootLoop;
import com.legend.testexecutionplan.SideShape;
import com.legend.testexecutionplan.TestPlan;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AN APPEAL READS ITS SIDES AS THE BODY'S EXECUTIONS (the re-audit's SHOULD-FIX 1, 2026-11-12): a side read again after
 * the run binds each stamp to its own execute()'s id, as the run did -- until then every stamp to the run's id, and a
 * stamp the plan never declared silently -- and the plan's declared executions include its appeals' sides; the door's
 * minted ids are exactly the plan's executions ({@link MintedPlan}).
 */
class AppealExecutionsTest {

    private static final String STAMP = ExecutionPlan.STAMP + ".appeal";
    private static final ExecutionPlan.Multiplicity ONE = new ExecutionPlan.Multiplicity(1, 1);

    /** A side whose one statement answers the trace comment of the execution {@link #STAMP} names. */
    private static TestPlan.Side.Rows side() {
        return new TestPlan.Side.Rows(new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.VALUE,
                List.of(new ExecutionPlan.Column("c", "String", ONE, "VARCHAR", null, null)),
                new ExecutionPlan.Sql("SELECT CAST(? AS VARCHAR)", List.of(new ExecutionPlan.Slot(STAMP,
                        new ExecutionPlan.Binding.One("VARCHAR", null))), target(), null).stamped(STAMP)));
    }

    /** The platform's own database (a test about no database runs there). */
    private static ExecutionPlan.Target target() {
        return new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(TestBackends.platform().planType(), null),
                List.of(), List.of(), null, TestBackends.settings(TestBackends.platform().planType()));
    }

    @Test
    void theAppealsReaderBindsEachStampToItsOwnExecution() throws Exception {
        UUID run = UUID.fromString("00000000-0000-4000-8000-000000000001");
        UUID own = UUID.fromString("00000000-0000-4000-8000-000000000002");
        PlanSessions.Source source = PlanSessions.shared();
        assertEquals(List.of(List.of(ExecutionPlan.traceComment(own))),
                Appeals.reader(source, Executions.of(run, Map.of(STAMP, own))).read(side()).rows(),
                "the stamp's own execution, never the run's");
        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> Appeals.reader(source, Executions.of(run, Map.of())).read(side()));
        assertTrue(refused.getMessage().startsWith("the plan reads the execution '" + STAMP + "', which it does"
                + " not declare"), refused.getMessage());
    }

    /** A plan whose one step reads no execution, and whose one appeal's side reads {@link #STAMP}. */
    private static TestPlan planWithAnAppealReading() {
        SideShape shape = new SideShape(SideShape.Kind.TABULAR, "Relation", List.of(new SideShape.Column("c", "String",
                ONE)), List.of(), true, false, 1, "[1]", false, true, false);
        Appeal.Rows rows = new Appeal.Rows(new Appeal.Read(side(), shape, null), Appeal.Read.NONE, null, null,
                null, false, false, null, null, Map.of(), List.of(), null, target(), Appeal.Enums.NONE);
        return new TestPlan(List.of(), List.of(new TestPlan.Run(new ExecutionPlan.Constant(List.of(
                new ExecutionPlan.Column("value", "String", ONE, null, null, null)), List.of(List.of("x"))))),
                TestPlan.Judge.HOST, Map.of(), Map.of(1, List.of(new Appeal(1, "rows", Appeal.Kind.ROWS, null, rows,
                        null, RootLoop.NO_APPEAL))), false, target());
    }

    @Test
    void aPlansExecutionsIncludeItsAppealsSides_andItsMintIsExactlyThem() {
        TestPlan plan = planWithAnAppealReading();
        assertEquals(List.of(STAMP), plan.executions());
        UUID run = UUID.randomUUID();
        new MintedPlan(plan, Executions.of(run, Map.of(STAMP, UUID.randomUUID())));
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                () -> new MintedPlan(plan, Executions.of(run, Map.of())));
        assertEquals("a test plan's run is minted one id per execution it declares ([" + STAMP + "]), and was minted"
                + " []", missing.getMessage());
        assertThrows(IllegalArgumentException.class, () -> new MintedPlan(plan, Executions.of(run,
                Map.of(STAMP, UUID.randomUUID(), ExecutionPlan.STAMP + ".other", UUID.randomUUID()))));
        assertThrows(IllegalArgumentException.class, () -> new MintedPlan(plan, Executions.one(run)));
    }
}
