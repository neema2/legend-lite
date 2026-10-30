// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.legend.planner.PlanMakerTest.plan;
import static com.legend.planner.PlanMakerTest.target;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What H2's plans say ({@link PlanMakerTest}'s cases, H2's own answers; moved here from it by the dialect program's
 * Phase 6, 2026-10-16): the versions it runs on, its rows, the type holes it needs and the types it binds.
 */
class PlanMakerH2Test {

    private static final DatabaseType H2 = DatabaseType.H2;

    /** H2 writes no NaN of its own text either ({@link PlanMakerTest#aDoubleThatIsNoNumberPrintsAsPurePrintsIt}'s case
     *  on H2, moved here at main's merge, 2026-10-22). */
    @Test
    void aDoubleThatIsNoNumberPrintsAsPurePrintsIt() throws Exception {
        PlanMakerTest.assertNoNumberPrintsAsPure(H2);
    }

    @Test
    void theTargetIsWhole_itsDatabaseVersionsSessionAndSetup() {
        ExecutionPlan.Target h2 = target(plan(H2, "|#>{s::DB.T}#->select(~[ID, NAME, PRICE])->sort(~ID->ascending())",
                com.legend.testcases.PlanCases.Written.JSON));
        assertEquals(new ExecutionPlan.Servers.Versions(List.of("2.1", "2.2")), h2.servers());
        assertEquals(List.of(), h2.session());
        // H2's rows are one INSERT
        assertInstanceOf(ExecutionPlan.SetupStep.Statement.class, h2.setup().get(h2.setup().size() - 1));
        assertTrue(h2.setup().stream().noneMatch(s -> s instanceof ExecutionPlan.SetupStep.Rows), h2.setup().toString());
    }

    @Test
    void aListIsOneArray() {
        ExecutionPlan.Sql sql = plan(H2, PlanCases.lists("T").get(0).withParameters(), com.legend.testcases.PlanCases.Written.JSON)
                .answerSql();
        assertTrue(sql.statement().contains("= ANY(?)"), sql.statement());
        assertEquals(List.of(new ExecutionPlan.Slot("ns", new ExecutionPlan.Binding.Array("BIGINT"))), sql.slots());
    }

    /** H2 types a parameter when it prepares the statement, and a Float's, a Decimal's, a Date's or a Number's type is
     *  its value's: H2's plan casts it to a TYPE HOLE the runner fills with the type H2 gives that value's literal
     *  (probes/value-typed-cast-results.txt). Every scalar case of {@link PlanMakerTest} answers through it on H2. */
    @Test
    void aParameterTypedByItsValueIsCastToATypeHole() {
        ExecutionPlan.Sql h2 = plan(H2, "{f: Float[1]|#>{s::DB.T}#"
                + "->extend(~x: r|$r.ID * $f)->select(~[ID, x])}", com.legend.testcases.PlanCases.Written.JSON).answerSql();
        assertEquals(1, h2.slots().size());
        ExecutionPlan.Binding.One one = (ExecutionPlan.Binding.One) h2.slots().get(0).binding();
        ExecutionPlan.TypeHole hole = java.util.Objects.requireNonNull(one.hole(), "a type hole");
        assertEquals("CAST(? AS )", h2.statement().substring(hole.at() - "CAST(? AS ".length(), hole.at() + 1),
                h2.statement());
        assertEquals(java.util.Map.of(
                ExecutionPlan.ValueKind.DECIMAL, new ExecutionPlan.TypeSpelling("NUMERIC",
                        ExecutionPlan.Digits.PRECISION_AND_SCALE),
                ExecutionPlan.ValueKind.FLOATING, new ExecutionPlan.TypeSpelling("DECFLOAT",
                        ExecutionPlan.Digits.PRECISION)), hole.types());
        assertEquals(ExecutionPlan.ValueKind.DECIMAL, hole.absent());
        assertEquals("DECIMAL", one.nullType());
    }

    /** A DateTime parameter is cast to the type of its value's literal, and passed as its text: H2's keeps nanoseconds
     *  (a plain TIMESTAMP keeps six digits and rounds the rest; probes/value-typed-cast-results.txt). */
    @Test
    void aDateTimeParameterIsCastToItsLiteralsType() {
        assertEquals(java.util.Map.of(
                ExecutionPlan.ValueKind.DATE_TIME, new ExecutionPlan.TypeSpelling("TIMESTAMP(9)",
                        ExecutionPlan.Digits.NONE),
                ExecutionPlan.ValueKind.DATE_TIME_NANOS, new ExecutionPlan.TypeSpelling("TIMESTAMP(9)",
                        ExecutionPlan.Digits.NONE)), PlanMakerTest.dateTimeBinding(H2).hole().types());
    }

    @Test
    void aScalarParameterIsDeclared_andBoundWhereItIsWritten() {
        ExecutionPlan plan = plan(H2, "{n: Integer[1]|#>{s::DB.T}#->filter(r|$r.ID != $n)"
                + "->extend(~plus: r|$r.ID + $n)->select(~[ID, plus])}", com.legend.testcases.PlanCases.Written.JSON);
        assertEquals(List.of(new ExecutionPlan.Parameter("n", "Integer", new ExecutionPlan.Multiplicity(1, 1),
                List.of())), plan.parameters());
        ExecutionPlan.Sql sql = plan.answerSql();
        ExecutionPlan.Slot n = new ExecutionPlan.Slot("n", new ExecutionPlan.Binding.One("BIGINT", null));
        assertEquals(List.of(n, n), sql.slots());
        assertEquals(2, sql.statement().chars().filter(ch -> ch == '?').count(), sql.statement());
    }
}
