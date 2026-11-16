// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.VerdictSql;
import com.legend.sql.DialectCapability;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.testexecutionplan.TestPlan;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * E-5c (the user's ruling O5, 2026-11-13): a root's HOST columns ride its first row that joins a fused statement --
 * the fusion receives them on that member alone, the fused plan's columns gain their facts, and the planned batch names
 * the verdict that carries them; a root whose rows join no fused statement (the split rung) is left uncarried, for the
 * planner to decide. The planner's pieces are stubs: a fusion that records what it was handed.
 */
class VerdictBatchHostColumnsTest {

    private static final ExecutionPlan.Target ON = new ExecutionPlan.Target(
            new ExecutionPlan.Database.Platform(com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null),
            List.of(), List.of(), null, null, null);

    private static final OutputCol V = new OutputCol("v", SqlType.Scalar.INTEGER, false);

    private static SqlQuery member(long n) {
        return new SqlSelect(List.of(new SqlSelect.Projection(new SqlExpr.IntLit(n), "v", V)), false,
                new SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(V));
    }

    private static final ExecutionPlan.Column BATCH_COLUMN = new ExecutionPlan.Column("__ix", "Integer",
            new ExecutionPlan.Multiplicity(1, 1), null, null, null);

    /** A runner that plans every statement (fused or not, as {@code fusedRefused} says) as a typed result. */
    private static VerdictBatch.Runner runner(boolean fusedRefused) {
        return new VerdictBatch.Runner() {
            @Override
            public ExecutionPlan.Node plan(SqlQuery statement, boolean fused, ExecutionPlan.Target on) {
                if (fused && fusedRefused) {
                    throw new DialectCapability("no fused statement on this dialect");
                }
                return new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION,
                        fused ? List.of(BATCH_COLUMN) : List.of(),
                        new ExecutionPlan.Sql("select", List.of(), on, statement));
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
    }

    private static VerdictSql.HostColumn hostSql(String name) {
        return new VerdictSql.HostColumn(new SqlExpr.IntLit(7), new OutputCol(name, SqlType.Scalar.BIGINT, true));
    }

    private static ExecutionPlan.Column hostFacts(String name) {
        return new ExecutionPlan.Column(name, "Integer", new ExecutionPlan.Multiplicity(1, 1), null, null, null);
    }

    @Test
    void aRootsHostColumnsRideItsFirstFusedRow() {
        List<List<VerdictSql.Member>> fused = new ArrayList<>();
        VerdictBatch batch = new VerdictBatch((members, frames) -> {
            fused.add(members);
            return members.get(0).statement();
        }, (generic, ruled) -> generic, (row, name, wantEqual) -> row, runner(false), new VerdictIds());
        batch.open("a");
        batch.defer("a1", true, member(1), ON);
        batch.defer("a2", true, member(2), ON);
        batch.close();
        batch.host(List.of(hostSql("__h0_0")), List.of(hostFacts("__h0_0")));
        batch.open("b");
        batch.defer("b1", true, member(3), ON);
        batch.close();
        VerdictBatch.Planned planned = batch.plan();

        assertEquals(1, fused.size());
        assertEquals(List.of(List.of(hostSql("__h0_0")), List.of(), List.of()),
                fused.get(0).stream().map(VerdictSql.Member::host).toList(),
                "root a's first row carries its host column, no other row any");
        TestPlan.Verdicts group = planned.groups().get(0).step();
        assertEquals(List.of("__ix", "__h0_0"), group.fused().columns().stream().map(ExecutionPlan.Column::name)
                .toList(), "the fused plan's columns: the batch's, then the host column's facts");
        assertEquals(Map.of(0, group.verdicts().get(0).id()), planned.hostCarriers());
        assertEquals(Set.of(), planned.hostUncarried());
    }

    @Test
    void aRootWhoseRowsJoinNoFusedStatementIsUncarried() {
        VerdictBatch batch = new VerdictBatch((members, frames) -> members.get(0).statement(),
                (generic, ruled) -> generic, (row, name, wantEqual) -> row, runner(true), new VerdictIds());
        batch.open("a");
        batch.defer("a1", true, member(1), ON);
        batch.close();
        batch.host(List.of(hostSql("__h0_0")), List.of(hostFacts("__h0_0")));
        VerdictBatch.Planned planned = batch.plan();
        assertEquals(Map.of(), planned.hostCarriers());
        assertEquals(Set.of(0), planned.hostUncarried());
    }

    @Test
    void hostColumnsNameTheirFactsAndFollowAClosedRoot() {
        VerdictBatch batch = new VerdictBatch((members, frames) -> members.get(0).statement(),
                (generic, ruled) -> generic, (row, name, wantEqual) -> row, runner(false), new VerdictIds());
        assertThrows(IllegalStateException.class,
                () -> batch.host(List.of(hostSql("__h")), List.of(hostFacts("__h"))));
        batch.open("a");
        batch.defer("a1", true, member(1), ON);
        batch.close();
        assertThrows(IllegalArgumentException.class,
                () -> batch.host(List.of(hostSql("__h")), List.of(hostFacts("__other"))));
    }
}
