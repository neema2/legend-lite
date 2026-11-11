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
    void theTargetIsWhole_itsDatabaseSessionAndSetup() {
        ExecutionPlan.Target h2 = target(plan(H2, "|#>{s::DB.T}#->select(~[ID, NAME, PRICE])->sort(~ID->ascending())",
                com.legend.testcases.PlanCases.Written.JSON));
        assertEquals(List.of(), h2.session());
        // H2's rows are a content blob of inserts, as every database's (A2, 2026-11-10)
        assertInstanceOf(ExecutionPlan.LoadStep.Statements.class, h2.loads().get(0).steps().get(h2.loads().get(0).steps().size() - 1));
    }

    @Test
    void aListIsOneArray() {
        ExecutionPlan.Sql sql = plan(H2, PlanCases.lists("T").get(0).withParameters(), com.legend.testcases.PlanCases.Written.JSON)
                .answerSql();
        assertTrue(sql.statement().contains("= ANY(?)"), sql.statement());
        assertEquals(List.of(new ExecutionPlan.Slot("ns", new ExecutionPlan.Binding.Array("BIGINT", null))), sql.slots());
    }

    /** A Float parameter is cast to its declared type's fixed type, upstream's own FLOAT (D1, P4), bound bare. */
    @Test
    void aFloatParameterIsCastToFloat() {
        ExecutionPlan.Sql h2 = plan(H2, "{f: Float[1]|#>{s::DB.T}#"
                + "->extend(~x: r|$r.ID * $f)->select(~[ID, x])}", com.legend.testcases.PlanCases.Written.JSON).answerSql();
        assertEquals(1, h2.slots().size());
        assertTrue(h2.statement().contains("CAST(? AS FLOAT)"), h2.statement());
        assertEquals(new ExecutionPlan.Binding.One("DECIMAL", null), h2.slots().get(0).binding());
    }

    /** A DateTime parameter is a TIMESTAMP(9), passed as its text: H2's keeps nanoseconds (a plain TIMESTAMP keeps six
     *  digits and rounds the rest; probes/value-typed-cast-results.txt). */
    @Test
    void aDateTimeParameterIsCastToTimestamp9() {
        assertEquals("CAST(? AS TIMESTAMP(9)) text", PlanMakerTest.dateTimeCast(H2));
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

    /** A Decimal or Number parameter's text is plain digits, as upstream prints the number (the user's ruling of
     *  2026-11-09, S175: DECFLOAT on H2, its text through H2's own number printer): 100, 120 and 1000D, never
     *  {@code 1E+2}; 2.50D's scale is DECFLOAT's, {@code 2.5} (S175, ruled). */
    @Test
    void aDecimalOrNumberParametersTextIsPlainDigits() throws Exception {
        record Case(String parameter, Object value, String text) {
        }
        for (Case c : List.of(new Case("n: Number[1]", 100L, "100"), new Case("n: Number[1]", 120L, "120"),
                new Case("n: Decimal[1]", new java.math.BigDecimal("1000"), "1000"),
                new Case("n: Decimal[1]", new java.math.BigDecimal("2.50"), "2.5"),
                new Case("n: Number[1]", 1.5d, "1.5"), new Case("n: Decimal[1]", new java.math.BigDecimal("0.000001"),
                        "0.000001"))) {
            ExecutionPlan plan = plan(H2, "{" + c.parameter() + "|#>{s::DB.T}#->filter(r|$r.ID == 1)"
                    + "->extend(~s: r|$n->toString())->select(~[s])}", PlanCases.Written.JSON);
            try (java.sql.Connection conn = com.legend.testbackend.TestBackends.of(H2).open()) {
                String json = PlanCases.write(plan, conn, java.util.Map.of("n", c.value()), PlanCases.Written.JSON::writer);
                assertTrue(json.contains("\"" + c.text() + "\""), c + ": " + json);
            }
        }
    }
}
