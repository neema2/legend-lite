// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;


import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.DialectCapability;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * THE SPLIT RUNG AT PLANNING (the P1 milestone audit's 1, 2026-11-07): where the dialect cannot spell a session's
 * fused verdict statement, each member is planned its own statement, and only a member whose own statement the dialect
 * cannot spell is left out (refused at its place in the root loop). Until the audit the members the dialect spelled
 * alone were dropped from every group. The DuckDB corpus never reaches this rung (its fused statements all spell), so
 * the planner's pieces are stubs here: a fusion the dialect refuses, and one member it refuses alone.
 */
class VerdictBatchSplitRungTest {

    private static final ExecutionPlan.Target ON = new ExecutionPlan.Target(
            new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), List.of(), List.of(), null, null);

    private static final OutputCol V = new OutputCol("v", SqlType.Scalar.INTEGER, false);

    /** A member's statement, told apart by its one literal. */
    private static SqlQuery member(long n) {
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(n), "v", V)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(V));
    }

    private static long literal(SqlQuery q) {
        return ((SqlExpr.IntLit) ((SqlSelect) q).projections().get(0).expr()).value();
    }

    /** The dialect refuses every fused statement, and member 3's own statement. */
    private static final VerdictBatch.Runner RUNNER = new VerdictBatch.Runner() {
        @Override
        public ExecutionPlan.Node plan(SqlQuery statement, boolean fused, ExecutionPlan.Target on) {
            if (fused) {
                throw new DialectCapability("no fused statement on this dialect");
            }
            if (literal(statement) == 3) {
                throw new DialectCapability("member 3 does not spell");
            }
            return new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION, List.of(),
                    new ExecutionPlan.Sql("select " + literal(statement), List.of(), on, statement));
        }

        @Override
        public boolean keyed(SqlQuery statement) {
            return false;
        }

        @Override
        public int chars(SqlQuery statement) {
            return 0;
        }

        @Override
        public TestPlan.Verdicts.Savepoint savepoint() {
            return null;
        }

        @Override
        public TestPlan.JudgingRules rules() {
            return TestPlan.JudgingRules.LITE;
        }
    };

    @Test
    void membersTheDialectSpellsAloneAreEachPlannedTheirOwnStatement() {
        VerdictBatch batch = new VerdictBatch((statements, frames) -> statements.get(0), (generic, ruled) -> generic,
                (row, name, wantEqual) -> row, RUNNER, new VerdictIds());
        batch.open("a");
        batch.defer("a1", true, member(1), ON);
        batch.defer("a2", true, member(2), ON);
        batch.close();
        batch.open("b");
        batch.defer("b1", true, member(3), ON);
        batch.close();
        VerdictBatch.Planned planned = batch.plan();
        List<VerdictBatch.Group> groups = planned.groups();
        assertEquals(1, groups.size(), "one group of own statements: " + groups);
        VerdictBatch.Group own = groups.get(0);
        assertFalse(own.keyed());
        assertNull(own.step().fused(), "no fused statement");
        assertEquals(List.of("a1", "a2"), own.step().verdicts().stream().map(TestPlan.Verdict::name).toList(),
                "the members the dialect spells alone, each with its own statement; member 3 left out");
        own.step().verdicts().forEach(v -> assertEquals(ExecutionPlan.TdsResult.class, v.wrapper().getClass()));
    }
}
