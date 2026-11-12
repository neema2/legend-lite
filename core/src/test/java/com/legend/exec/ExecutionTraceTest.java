// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batch;
import com.legend.result.PureV1Writer;
import com.legend.result.ResultSink;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AN EXECUTION'S TRACE (C1's second half, the user's ruling of 2026-11-10, "Agree with all"; built 2026-11-12 by the C
 * checkpoint's code audit, SHOULD-FIX 4): each execution mints one id, as upstream's {@code ExecutionState} mints its
 * {@code execID}; it is the id the run names to its listener before its columns, the id its statements' stamps bind,
 * and the id the answer's activity reports.
 */
class ExecutionTraceTest {

    /** A plan whose one statement reads its own stamp: the execution-trace comment, bound when the plan runs. */
    private static ExecutionPlan stampReader() {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), List.of(), List.of(), null);
        ExecutionPlan.Sql sql = new ExecutionPlan.Sql("SELECT CAST(? AS VARCHAR)", List.of(new ExecutionPlan.Slot(
                "#stamp0", new ExecutionPlan.Binding.One("VARCHAR", null))), target, null).stamped("#stamp0");
        return ExecutionPlan.reading(List.of(), PlanRunnerTest.cell(sql));
    }

    /** What a sink was told, in order: the execution's id, then "columns", each cell, then "end". */
    private static final class Told implements ResultSink {
        final List<Object> told = new ArrayList<>();

        @Override
        public void execution(UUID id) {
            told.add(id);
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) {
            told.add("columns");
        }

        @Override
        public void batch(Batch batch) {
            for (int r = 0; r < batch.size(); r++) {
                told.add(batch.get(r, 0));
            }
        }

        @Override
        public void end() {
            told.add("end");
        }
    }

    @Test
    void anExecutionNamesItsIdToItsListenerFirst_andItsStampBindsTheSameId() throws IOException {
        Told first = new Told();
        PlanRunner.stream(stampReader(), Map.of(), PlanSessions.shared(), first);
        UUID id = assertInstanceOf(UUID.class, first.told.get(0), "the execution's id, before its columns");
        assertEquals(List.of(id, "columns", ExecutionPlan.traceComment(id), "end"), first.told,
                "the statement's stamp is the execution's own id: the one the listener was told");
        Told second = new Told();
        PlanRunner.stream(stampReader(), Map.of(), PlanSessions.shared(), second);
        assertNotEquals(id, second.told.get(0), "each execution mints its own");
    }

    @Test
    void theAnswersActivityReportsTheExecutionThatRanItsSql() throws IOException {
        StringWriter out = new StringWriter();
        UUID[] told = {null};
        PureV1Writer writer = PureV1Writer.relation(out, columns -> new PureV1Writer.Envelope(
                "{\"_type\":\"tdsBuilder\",\"columns\":[]}", "SELECT 1", List.of("n")));
        // the writer, the id it is told recorded on the way
        ResultSink recording = new ResultSink() {
            @Override
            public void execution(UUID id) {
                told[0] = id;
                writer.execution(id);
            }

            @Override
            public void columns(List<ExecutionPlan.Column> columns) throws IOException {
                writer.columns(columns);
            }

            @Override
            public void batch(Batch batch) throws IOException {
                writer.batch(batch);
            }

            @Override
            public void end() throws IOException {
                writer.end();
            }
        };
        PlanRunner.stream(stampReader(), Map.of(), PlanSessions.shared(), recording);
        assertTrue(out.toString().contains("\"comment\":\"-- \\\"executionTraceID\\\" : \\\"" + told[0] + "\\\"\""),
                "the activity's comment names the run's execution: " + out);
    }

    @Test
    void aWriterToldOfNoExecutionRefusesToReportOne() {
        PureV1Writer writer = PureV1Writer.relation(new StringWriter(), columns -> new PureV1Writer.Envelope(
                "{}", "SELECT 1", List.of("n")));
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> writer.columns(List.of(
                new ExecutionPlan.Column("n", "String", new ExecutionPlan.Multiplicity(0, 1), "VARCHAR", null, null))));
        assertTrue(refused.getMessage().contains("the run named no execution"), refused.getMessage());
    }

    @Test
    void noIdIsReadOutsideARun() {
        assertThrows(IllegalStateException.class, com.legend.base.RunScope::execution);
        assertEquals(null, com.legend.base.RunScope.currentExecution());
    }
}
