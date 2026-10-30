// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend;

import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.result.Batch;
import com.legend.result.ResultSink;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TYPED PLAN (the result boundary program's step 1, docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md §5, 2026-10-27):
 * every root planned as typed rows -- a relation's rows, a value's elements, a graph fetch's objects, a dynamic pivot's
 * keyed rows, a write program's answer -- read by the session from the driver by the plan's column facts and streamed
 * to a sink in batches, no output text in any statement. Judged on every in-process database by the values decoded.
 */
class TypedPlanTest {

    private static ExecutionPlan plan(DatabaseType type, String query) {
        return Compiler.query(Compiler.compileModel(PlanMakerTest.model(type.name())), query).executionPlan("s::RT");
    }

    private static ExecutionPlan.Target target(ExecutionPlan plan) {
        return switch (plan.root()) {
            case ExecutionPlan.TdsResult t -> t.sql().target();
            case ExecutionPlan.Keyed k -> k.keys().target();
            case ExecutionPlan.Sequence s -> target(new ExecutionPlan(List.of(), s.steps().get(0)));
            case ExecutionPlan.Update u -> u.sql().target();
            case ExecutionPlan.Check k -> k.sql().target();
            case ExecutionPlan.Effect e -> e.sql().target();
        };
    }

    private static PlanRunner.Rows rows(DatabaseType type, String query) throws Exception {
        ExecutionPlan plan = plan(type, query);
        // the plan reads back from its JSON as it was made (format 12: the typed result's shape)
        assertEquals(PlanJson.write(plan), PlanJson.write(PlanJson.read(PlanJson.write(plan))));
        try (Connection c = TestBackends.of(type).open()) {
            return PlanRunner.rows(plan, Map.of(), PlanSessions.setUp(c, target(plan)));
        }
    }

    private static ExecutionPlan.TdsResult.Shape shape(DatabaseType type, String query) {
        return ((ExecutionPlan.TdsResult) plan(type, query).root()).shape();
    }

    @Test
    void aRelationAnswersItsRows_typedByThePlansColumns() throws Exception {
        for (DatabaseType type : PlanMakerTest.IN_PROCESS) {
            String q = "|#>{s::DB.T}#->select(~[ID, NAME, PRICE])->sort(~ID->ascending())";
            assertEquals(ExecutionPlan.TdsResult.Shape.RELATION, shape(type, q), type.name());
            PlanRunner.Rows r = rows(type, q);
            assertEquals(List.of("ID", "NAME", "PRICE"), r.columns().stream().map(ExecutionPlan.Column::name).toList());
            assertEquals(3, r.rows().size(), type.name());
            assertEquals(1L, r.rows().get(0).get(0), type.name());
            assertEquals("O'Brien", r.rows().get(1).get(1), type.name());
            assertEquals(0, new BigDecimal("1.50").compareTo((BigDecimal) r.rows().get(0).get(2)), type.name());
            assertEquals(null, r.rows().get(1).get(2), type.name());
        }
    }

    @Test
    void aValueAnswersItsElements_inTheOneColumnValue() throws Exception {
        for (DatabaseType type : PlanMakerTest.IN_PROCESS) {
            assertEquals(ExecutionPlan.TdsResult.Shape.VALUE, shape(type, "|1 + 2"), type.name());
            PlanRunner.Rows scalar = rows(type, "|1 + 2");
            assertEquals(List.of(List.of(3L)), scalar.rows(), type.name());
            assertEquals("value", scalar.columns().get(0).name(), type.name());
            PlanRunner.Rows many = rows(type, "|s::Item.all()->map(i|$i.id)->sort()");
            assertEquals(List.of(List.of(1L), List.of(2L), List.of(3L)), many.rows(), type.name());
            assertEquals(new ExecutionPlan.Multiplicity(0, null), many.columns().get(0).multiplicity(), type.name());
        }
    }

    @Test
    void aGraphFetchAnswersItsObjects_oneJsonObjectPerRow() throws Exception {
        for (DatabaseType type : PlanMakerTest.IN_PROCESS) {
            String q = "|s::Item.all()->graphFetch(#{s::Item{id, name}}#)->serialize(#{s::Item{id, name}}#)";
            assertEquals(ExecutionPlan.TdsResult.Shape.OBJECTS, shape(type, q), type.name());
            PlanRunner.Rows r = rows(type, q);
            assertEquals(3, r.rows().size(), type.name());
            for (List<Object> row : r.rows()) {
                assertTrue(row.get(0) instanceof String s && s.startsWith("{") && s.contains("\"id\""),
                        type.name() + ": " + row);
            }
        }
    }

    @Test
    void aDynamicPivotAnswersItsKeyedRows() throws Exception {
        String trees = "#TDS\n city, year, treePlanted\n NYC, 2011, 5\n NYC, 2000, 15\n SAN, 2011, 2\n"
                + " SAN, 2000, 1\n SAN, 2000, 2\n#";
        String q = "|" + trees + "->pivot(~[year], ~['newCol' : x | $x.treePlanted : y | $y->plus()])"
                + "->sort(~city->ascending())";
        for (DatabaseType type : PlanMakerTest.IN_PROCESS) {
            assertTrue(plan(type, q).root() instanceof ExecutionPlan.Keyed, type.name());
            PlanRunner.Rows r = rows(type, q);
            assertEquals("city", r.columns().get(0).name(), type.name());
            assertEquals(3, r.columns().size(), type.name() + ": " + r.columns());
            assertEquals(2, r.rows().size(), type.name());
            assertEquals("NYC", r.rows().get(0).get(0), type.name());
        }
    }

    @Test
    void aWriteProgramAnswersItsCount_afterItsChangesCommit() throws Exception {
        // on the platform's database, as WritePlansTest plans its writes (a test names no database: the census's
        // DialectLeakRatchetTest, 2026-10-27; the writes design's leg 2 spells them on three)
        DatabaseType type = TestBackends.platform().type();
        String q = "|#>{s::DB.T}#->filter(r|$r.ID == 1)->write(#>{s::DB.T}#)";
        ExecutionPlan plan = plan(type, q);
        if (!(plan.root() instanceof ExecutionPlan.Sequence s)) {
            throw new AssertionError(type + ": a write program is a sequence, given " + plan.root());
        }
        assertTrue(s.steps().get(s.steps().size() - 1) instanceof ExecutionPlan.TdsResult t
                && t.shape() == ExecutionPlan.TdsResult.Shape.VALUE, type.name() + ": " + s.steps());
    }

    @Test
    void theRowsStreamInBatches_theColumnsFirst_thenTheEnd() throws Exception {
        DatabaseType type = TestBackends.platform().type();
        ExecutionPlan plan = plan(type, "|#>{s::DB.T}#->select(~[ID, NAME])->sort(~ID->ascending())");
        List<String> events = new ArrayList<>();
        try (Connection c = TestBackends.of(type).open()) {
            PlanRunner.stream(plan, Map.of(), PlanSessions.setUp(c, target(plan)), new ResultSink() {
                @Override
                public void columns(List<ExecutionPlan.Column> columns) {
                    events.add("columns " + columns.stream().map(ExecutionPlan.Column::name).toList());
                }

                @Override
                public void batch(Batch batch) {
                    events.add("batch " + batch.size() + " " + batch.get(0, 0));
                }

                @Override
                public void end() {
                    events.add("end");
                }
            }, 2);
        }
        assertEquals(List.of("columns [ID, NAME]", "batch 2 1", "batch 1 3", "end"), events);
    }
}
