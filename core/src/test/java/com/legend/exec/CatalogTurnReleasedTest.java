// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.executionplan.ExecutionPlan;
import com.legend.result.Batch;
import com.legend.result.Collector;
import com.legend.result.ResultSink;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testbackend.TestSessions;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * THE CATALOG'S TURN IS HELD FOR THE DATABASE'S WORK ALONE (C5-J's batch, 4(c), 2026-11-16). A query reading the
 * metamodel runs on the process's catalog in a turn of its own; until then the turn was held while the rows were written
 * to the caller's sink, so a slow sink held every other metamodel query, and a sink that waited on one deadlocked. The
 * run's rows are now kept inside the turn and written after it is released: here the caller's sink, as it receives the
 * end of one catalog answer, waits for a second catalog query run on another thread -- which answers.
 */
class CatalogTurnReleasedTest {

    private static final String MODEL = "Class test::Person { name: String[1]; }\n";
    private static final String COUNT = "|meta::pure::metamodel::type::Class.all()->filter(c | $c.name == '%s')->size()";

    private static ExecutionPlan plan(String name) {
        var type = TestBackends.platform().type();
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(StorelessRuntime.with(MODEL, type)),
                COUNT.formatted(name)).executionPlan(StorelessRuntime.RUNTIME);
    }

    private static void run(ExecutionPlan plan, ResultSink sink) throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            ProductRun.run(plan, SessionSource.given(TestSessions.over(c)), PlanSessions.of(TestSessions.over(c)),
                    ProductRun.Establish.DECLARED_SETUP, new com.legend.result.Output.Rows(sink), 1024,
                    java.util.UUID.randomUUID());
        }
    }

    private static long count(ExecutionPlan plan) throws Exception {
        Collector collector = new Collector();
        run(plan, collector);
        return ((Number) collector.collected().rows().get(0).get(0)).longValue();
    }

    @Test
    void aSinkThatWaitsOnAnotherCatalogQueryIsAnswered() throws Exception {
        ExecutionPlan first = plan("Person");
        ExecutionPlan second = plan("Nobody");
        Collector collector = new Collector();
        long[] other = new long[1];
        ResultSink waiting = new ResultSink() {
            @Override
            public void columns(List<ExecutionPlan.Column> columns) throws IOException {
                collector.columns(columns);
            }

            @Override
            public void batch(Batch batch) throws IOException {
                collector.batch(batch);
            }

            @Override
            public void end() throws IOException {
                // another request's metamodel query, while this answer is being written
                try {
                    other[0] = CompletableFuture.supplyAsync(() -> {
                        try {
                            return count(second);
                        } catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }).get(20, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IOException("the second catalog query did not answer while the first was written", e);
                }
                collector.end();
            }
        };
        run(first, waiting);
        assertEquals(1L, ((Number) collector.collected().rows().get(0).get(0)).longValue());
        assertEquals(0L, other[0]);
    }
}
