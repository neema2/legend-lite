// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.edge.CompatEdge;
import com.legend.edge.RefereeLoad;
import com.legend.executionplan.DataRef;
import com.legend.executionplan.DataStores;
import com.legend.executionplan.ExecutionPlan;
import com.legend.setup.LoadSteps;
import com.legend.sql.dialect.RawSqlBoundary;
import com.legend.testexecutionplan.Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE REFEREE'S COPY OF A SEED IS A LOAD (B2b, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6): the edge plans
 * the copy at the seed's step ({@link RefereeLoad}) and records, for each step of the plan's load that ran, the copy's
 * step at its place -- a statement as it answered, a blob's inserts each, a file the referee's own statements over the
 * bytes the plan's load read, verified against the plan's reference (a file that is not refused by name). Steps after a
 * failed one are not recorded.
 */
class EdgeRefereeLoadTest {

    private static final byte[] CSV = "ID\n1\n2\n".getBytes(StandardCharsets.UTF_8);

    @Test
    void theStepsThatRanAreRecordedAsTheRefereesCopy(@TempDir Path resources) throws Exception {
        Files.write(resources.resolve("ids.csv"), CSV);
        RawSqlBoundary.Recorder recorder = new RawSqlBoundary.Recorder();
        CompatEdge edge = new CompatEdge(recorder, DataStores.resources(resources));
        List<Extension> planned = edge.seed(copy(DataStores.sha256(CSV)).steps(), copy(DataStores.sha256(CSV))
                .content());
        // every step ran: the statement, the blob's two inserts, the file's statements as one entry
        edge.seeded(planned, List.of(false, false, false));
        List<RawSqlBoundary.Raw> seeds = recorder.seeds();
        assertEquals(List.of("CREATE TABLE T (ID INTEGER)", "INSERT INTO T VALUES (1)", "INSERT INTO T VALUES (2)"),
                seeds.subList(0, 3).stream().map(RawSqlBoundary.Raw::sql).toList());
        assertNull(seeds.get(0).read());
        RawSqlBoundary.FileRead read = seeds.get(3).read();
        assertArrayEquals(CSV, read.bytes(), "the bytes the plan's load read");
        assertEquals(List.of("SET @legend_csv = ?", "INSERT INTO U SELECT 1"), read.statements());
        assertEquals(List.of("SET @legend_csv = NULL"), read.after());
    }

    @Test
    void aFailedStepAndThoseAfterItAreNotRecorded(@TempDir Path resources) throws Exception {
        Files.write(resources.resolve("ids.csv"), CSV);
        RawSqlBoundary.Recorder recorder = new RawSqlBoundary.Recorder();
        CompatEdge edge = new CompatEdge(recorder, DataStores.resources(resources));
        LoadSteps copy = copy(DataStores.sha256(CSV));
        // the run tells the steps before the failure: the statement alone
        edge.seeded(edge.seed(copy.steps(), copy.content()), List.of(false));
        assertEquals(List.of("CREATE TABLE T (ID INTEGER)"), recorder.seeds().stream().map(RawSqlBoundary.Raw::sql)
                .toList());
    }

    @Test
    void aFileThatIsNotThePlansIsRefusedByName(@TempDir Path resources) throws Exception {
        Files.write(resources.resolve("ids.csv"), "ID\n9\n".getBytes(StandardCharsets.UTF_8));
        CompatEdge edge = new CompatEdge(new RawSqlBoundary.Recorder(), DataStores.resources(resources));
        LoadSteps copy = copy(DataStores.sha256(CSV));
        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> edge.seeded(edge.seed(copy.steps(), copy.content()), List.of(false, false, false)));
        assertTrue(refused.getMessage().startsWith("the referee's copy of the file 'ids.csv' is not the data the plan's"
                + " load read"), refused.getMessage());
    }

    @Test
    void theCopyRoundTripsThroughTheTestPlansFormat() {
        LoadSteps copy = copy(DataStores.sha256(CSV));
        RefereeLoad load = new RefereeLoad(copy.steps(), copy.content());
        com.legend.edge.EdgeCodec codec = new com.legend.edge.EdgeCodec();
        Map<String, Object> json = codec.write(load);
        Extension read = codec.read("refereeLoad", (com.legend.json.Json.Obj) com.legend.json.Json.parse(
                com.legend.json.Json.toCompact(json)));
        assertEquals(load, read);
    }

    /** A seed's copy: a statement, a blob of two inserts, a file by its reference read by its own statements. */
    private static LoadSteps copy(String digest) {
        LoadSteps rows = LoadSteps.blob(List.of("INSERT INTO T VALUES (1)", "INSERT INTO T VALUES (2)"),
                "the data of the table T");
        List<ExecutionPlan.LoadStep> steps = new ArrayList<>();
        steps.add(new ExecutionPlan.LoadStep.Statement("CREATE TABLE T (ID INTEGER)"));
        steps.addAll(rows.steps());
        steps.add(new ExecutionPlan.LoadStep.File(new DataRef(DataRef.Namespace.RESOURCE, "ids.csv", digest,
                CSV.length, DataRef.Csv.DEFAULT), new ExecutionPlan.LoadStep.Delivery.Reads(
                List.of("SET @legend_csv = ?", "INSERT INTO U SELECT 1"), List.of("SET @legend_csv = NULL")), 500));
        return new LoadSteps(steps, rows.content());
    }
}
