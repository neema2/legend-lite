// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.compiler.element.ModelContext;
import com.legend.exec.SessionSource;
import com.legend.executionplan.DataStores;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.result.Collector;
import com.legend.result.Output;
import com.legend.testbackend.TestSessions;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A test body reading a {@code data:} source under the compatibility edge (the orchestrator's late item of the A
 * checkpoint, 2026-11-10; the matrix's cell EDGE/DATA_SOURCE, beside {@link DataDeliveryMatrixTest}): the edge's ledger
 * records a body's seeds by the texts the test planner made for each, and a live source's session table has none, so
 * the run crashed at it ("no ledger texts planned"), then was refused by name at planning. Since B2a (2026-11-11) the
 * edge plans a live source's load as a record of its own, {@code LiveSourceLoad}, which it records as nothing -- the
 * referee replays legend-engine's text, which reads no table of lite's: the body answers its rows under the edge as on
 * lite's own test door.
 */
class EdgeLiveSourceTest {

    /** The platform's own engine. */
    private static final DatabaseType PLATFORM = com.legend.testbackend.TestBackends.platform().type();

    @Test
    void aDataSourceUnderTheEdge() throws Exception {
        String model = DataDeliveryMatrixTest.model(DataDeliveryMatrixTest.bareConnection(PLATFORM), "");
        ModelContext ctx = Compiler.compileModel(model);
        String query = "|sourceUrl('" + DataDeliveryMatrixTest.PAYLOAD + "')" + DataDeliveryMatrixTest.FROM_JSON;
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(query);
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        var resolved = Compiler.resolveQuery(statements, new com.legend.model.ImportScope(List.of()), ctx);
        com.legend.sql.dialect.RawSqlBoundary.Recorder recorder = new com.legend.sql.dialect.RawSqlBoundary.Recorder();
        ExecuteOptions options = ExecuteOptions.recording(new com.legend.edge.CompatEdge(recorder, DataStores.none()),
                DataStores.none());
        try (Connection c = DataDeliveryMatrixTest.session(PLATFORM)) {
            Collector edged = new Collector();
            TestExecution.runProgram(Compiler.queryResolved(ctx, resolved), "x::RT",
                    SessionSource.given(TestSessions.over(c)), (name, pass, detail) -> { }, null, options,
                    new Output.Rows(edged));
            assertEquals(DataDeliveryMatrixTest.ROWS, DataDeliveryMatrixTest.strings(edged));
            // the load that made the table is in no ledger: the referee's replay reads no table of lite's
            assertTrue(recorder.seeds().isEmpty(), String.valueOf(recorder.seeds()));
            // lite's own test door, on the same session, answers it (the table the edge's run made is gone: released
            // after the run)
            Collector collector = new Collector();
            TestExecution.runProgram(Compiler.queryResolved(ctx, resolved), "x::RT",
                    SessionSource.given(TestSessions.over(c)), (name, pass, detail) -> { }, null,
                    new ExecuteOptions(null, DataStores.none(), java.util.Set.of()), new Output.Rows(collector));
            assertEquals(DataDeliveryMatrixTest.ROWS, DataDeliveryMatrixTest.strings(collector));
        }
    }
}
