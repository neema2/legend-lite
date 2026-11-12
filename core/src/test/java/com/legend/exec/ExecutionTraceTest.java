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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AN EXECUTION'S TRACE (C1's second half, the user's ruling of 2026-11-10, "Agree with all"; built 2026-11-12 by the C
 * checkpoint's code audit, SHOULD-FIX 4): each execution has one id, as upstream's {@code ExecutionState} has its
 * {@code execID}; it is the id the run names to its listener before its columns, the id its statements' stamps bind,
 * and the id the answer's activity reports. The id is its DOOR's (the user's ruling of 2026-11-12, "Do the exec id as
 * part of C"): minted where the run starts ({@code Execution}, {@code TestExecution}) and passed into exec, which makes
 * none; here the test is the door, and passes the id it chooses.
 */
class ExecutionTraceTest {

    /** A plan whose one statement reads its own stamp: the execution-trace comment, bound when the plan runs. */
    private static ExecutionPlan stampReader() {
        ExecutionPlan.Target target = new ExecutionPlan.Target(new ExecutionPlan.Database.Platform(
                com.legend.planner.ConnectionPlans.type(Databases.PLATFORM), null), List.of(), List.of(), null, null);
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

    private static final UUID ONE = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final UUID TWO = UUID.fromString("00000000-0000-4000-8000-000000000002");

    @Test
    void anExecutionNamesItsDoorsIdToItsListenerFirst_andItsStampBindsThatId() throws IOException {
        Told first = new Told();
        PlanRunner.stream(stampReader(), Map.of(), PlanSessions.shared(), first, ONE);
        assertEquals(List.of(ONE, "columns", ExecutionPlan.traceComment(ONE), "end"), first.told,
                "the door's id, before the columns, and the statement's stamp bound to it");
        Told second = new Told();
        PlanRunner.stream(stampReader(), Map.of(), PlanSessions.shared(), second, TWO);
        assertEquals(List.of(TWO, "columns", ExecutionPlan.traceComment(TWO), "end"), second.told,
                "another execution, the id its door passed");
    }

    /** A door that passes a fixed id gets the same activity report, byte for byte: exec adds no value of its own. */
    @Test
    void aFixedIdWritesADeterministicActivityReport() throws IOException {
        List<String> reports = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            StringWriter out = new StringWriter();
            PureV1Writer writer = PureV1Writer.relation(out, columns -> new PureV1Writer.Envelope(
                    "{\"_type\":\"tdsBuilder\",\"columns\":[]}", "SELECT 1", List.of("n")), ONE);
            PlanRunner.stream(stampReader(), Map.of(), PlanSessions.shared(), writer, ONE);
            reports.add(out.toString());
        }
        assertEquals(reports.get(0), reports.get(1), "one id, one report");
        assertTrue(reports.get(0).contains(ONE.toString()), reports.get(0));
    }

    /** The run's scope carries the id its door passed and mints none: a run of one statement, which reads none, has
     *  none. */
    @Test
    void theRunsScopeCarriesTheDoorsIdAndMintsNone() {
        try (var run = com.legend.base.RunScope.enter(ONE)) {
            assertEquals(ONE, com.legend.base.RunScope.execution());
        }
        try (var run = com.legend.base.RunScope.enter()) {
            assertEquals(null, com.legend.base.RunScope.currentExecution());
            assertThrows(IllegalStateException.class, com.legend.base.RunScope::execution);
        }
        assertThrows(NullPointerException.class, () -> com.legend.base.RunScope.enter(null));
    }

    @Test
    void theAnswersActivityReportsTheExecutionThatRanItsSql() throws IOException {
        StringWriter out = new StringWriter();
        UUID[] told = {null};
        PureV1Writer writer = PureV1Writer.relation(out, columns -> new PureV1Writer.Envelope(
                "{\"_type\":\"tdsBuilder\",\"columns\":[]}", "SELECT 1", List.of("n")), ONE);
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
        PlanRunner.stream(stampReader(), Map.of(), PlanSessions.shared(), recording, ONE);
        assertTrue(out.toString().contains("\"comment\":\"-- \\\"executionTraceID\\\" : \\\"" + told[0] + "\\\"\""),
                "the activity's comment names the run's execution: " + out);
    }

    /** A writer of the engine's answer is made for an execution, its door's id: made for none, it is refused by name,
     *  where it is made -- before the run, or after it (PCT's route, a host's answer) -- so no writer reports none. */
    @Test
    void aWriterMadeForNoExecutionIsRefusedByName() {
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> PureV1Writer.relation(
                new StringWriter(), columns -> new PureV1Writer.Envelope("{}", "SELECT 1", List.of("n")), null));
        assertTrue(refused.getMessage().contains("the run named no execution"), refused.getMessage());
        ExecutionPlan relation = ExecutionPlan.reading(List.of(), new ExecutionPlan.TdsResult(
                ExecutionPlan.TdsResult.Shape.RELATION, List.of(), stampReader().answerSql()));
        IllegalStateException arrow = assertThrows(IllegalStateException.class, () -> new com.legend.result.Output.Arrow(
                new java.io.ByteArrayOutputStream(), null).writerFor(relation, null));
        assertTrue(arrow.getMessage().contains("the run named no execution"), arrow.getMessage());
    }

    /** One answer is one execution's: a writer made for one, told another by the run, refuses by name. */
    @Test
    void aWriterToldAnotherExecutionRefusesByName() {
        PureV1Writer writer = PureV1Writer.relation(new StringWriter(), columns -> new PureV1Writer.Envelope(
                "{}", "SELECT 1", List.of("n")), ONE);
        writer.execution(ONE);
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> writer.execution(TWO));
        assertTrue(refused.getMessage().contains("one answer is one execution's"), refused.getMessage());
    }

    @Test
    void noIdIsReadOutsideARun() {
        assertThrows(IllegalStateException.class, com.legend.base.RunScope::execution);
        assertEquals(null, com.legend.base.RunScope.currentExecution());
    }
}
