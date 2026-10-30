// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testing.Lean;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The AUTO-RESOLVING entry points of the server (QueryService's until D67, 2026-10-30) — the routes
 * that call {@code ConnectionResolver.resolve} themselves rather than taking a
 * caller's connection — and the resource contract they owe.
 *
 * <p>WHY THIS CLASS EXISTS (docs/CONNECTION_LEASE_DESIGN_2026_09_09.md). The
 * resolver hands back connections under two ownership rules through one
 * signature: in-memory DuckDB/SQLite are owned by {@code cache/HandleStore} for
 * the life of the process (evicting one would drop its tables), while every
 * other arm is a fresh {@code DriverManager} connection that somebody must
 * close. Nobody did. A runtime-instrumented census of the whole core suite
 * (2026-09-09) found the suite reaches {@code resolve} 17 times through only
 * TWO of the five methods, so three of them were changed-blind territory:
 * {@code execute(3-arg)}, {@code execute(…, OutputStream, OutputFormat)} and
 * {@code stream(…, OutputStream)} had no coverage at all. {@link #allFour}
 * closes that hole ({@code executeSql}, the fifth, was deleted by plan W0.1).
 *
 * <p>{@link #leaseReleasesOwnedKeepsBorrowed} asserts the CONTRACT directly —
 * a caller-owned connection is closed by its lease, a store-owned one never is.
 * It names the lease type, so it pins the contract but could never have FAILED
 * against the unfixed product; it would not have compiled.
 * {@link #queryServiceReleasesWhatItResolves} is the witness that carries that
 * burden: it names no lease, goes through the public entry point, and fails
 * against the unfixed product.
 *
 * <p>An earlier version of this witness counted open file descriptors, which
 * caught the bug but only narrowly. It needed a database that opens one
 * descriptor per connection (SQLite does; DuckDB keeps ONE instance per path
 * and hides the leak entirely — measured), it worked only on POSIX, and SQLite
 * is a DECLARED lite extension whose grammar belongs to the marked
 * extension-test hosts. {@code isClosed()} is exact, needs no extension
 * grammar, and holds on the Windows runner where the leak first surfaced as a
 * database file that could not be deleted.
 */
class ConnectionLeaseTest {

    /** DuckDB, file-backed: the caller-owned (unpooled) arm. */
    private static Path duckFile;
    private static String duckModel;
    /** DuckDB, in-memory: the store-owned (pooled) arm. */
    private static String memModel;

    @BeforeAll
    static void setup() throws Exception {
        duckFile = Files.createTempFile("lease-test-", ".duckdb");
        Files.delete(duckFile);
        memModel = """
                ###Pure
                Class model::Person { firstName: String[1]; }

                ###Relational
                Database store::TestDatabase (
                    Table T_PERSON ( ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100) )
                )

                ###Mapping
                Mapping model::PersonMapping (
                    model::Person: Relational {
                        ~mainTable [store::TestDatabase] T_PERSON
                        firstName: [store::TestDatabase] T_PERSON.FIRST_NAME
                    }
                )

                ###Connection
                RelationalDatabaseConnection store::TestConnection {
                    type: DuckDB;
                    specification: DuckDB { };
                    auth: Test;
                }

                ###Runtime
                Runtime test::TestRuntime {
                    mappings: [ model::PersonMapping ];
                    connections: [ store::TestDatabase: [ environment: store::TestConnection ] ];
                }
                """;
        duckModel = memModel.replace("specification: DuckDB { };",
                "specification: DuckDB { path: '"
                        + duckFile.toString().replace("\\", "/") + "'; };");
        Seed.sql(duckModel, "CREATE TABLE T_PERSON (ID INTEGER PRIMARY KEY,"
                + " FIRST_NAME VARCHAR(100))", "test::TestRuntime");
        Seed.sql(duckModel, "INSERT INTO T_PERSON VALUES (1, 'Alice')",
                "test::TestRuntime");
    }

    /** {@code query} over the file-backed model on the server's route, written by lite's CSV or JSON rows writer. */
    private static byte[] written(String query, boolean csv) throws java.io.IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        java.io.Writer w = new java.io.OutputStreamWriter(out, java.nio.charset.StandardCharsets.UTF_8);
        com.legend.Execution.run(duckModel, query, "test::TestRuntime", ConnectionResolver.SOURCE,
                com.legend.exec.PlanSessions.shared(), plan -> csv
                        ? new com.legend.result.CsvWriter(w, plan.answerShape())
                        : new com.legend.result.JsonRowsWriter(w, plan.answerShape()));
        w.flush();
        return out.toByteArray();
    }

    @Test
    @DisplayName("the server's auto-resolving routes execute (two routes, two writers on one)")
    void allFour() throws Exception {
        String query = "|model::Person.all()->project(~[name: p|$p.firstName])";

        // 1. the service's own route with no caller's connection (QueryService.execute(model, query, runtime, sinkOf),
        //    the route its format door rides): the runtime's connection leased
        Lean.Result leased = Lean.service(duckModel, query, "test::TestRuntime");
        assertEquals(com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION, leased.shape());
        assertEquals("name", leased.columns().get(0).name(), String.valueOf(leased));

        // 2. pure/v1 execute (E8) — PureV1Api.execute leases the runtime's connection
        String lambda = PureV1Api.grammarToJsonLambda(query, false).json();
        PureV1Api.Answer executed = PureV1Api.execute("{\"clientVersion\":\"vX_X_X\",\"function\":"
                + lambda + ",\"model\":" + com.legend.json.Json.toCompact(
                        java.util.Map.of("_type", "text", "code", duckModel))
                + ",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"test::TestRuntime\"}"
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}", ConnectionResolver.RUNNER);
        assertEquals(200, executed.status(), executed.json());

        // 3. the CSV writer on the server's route (QueryService.execute(..., out, CSV) until D67, 2026-10-30: the
        //    route is Execution.run on the resolver's sessions, the writer the caller's)
        var csv = written(query, true);
        assertTrue(csv.length > 0, "wire execute wrote nothing");

        // (executeSql and /engine/sql were deleted by plan W0.1: raw SQL is not a product surface)

        // 5. the JSON rows writer on the same route (QueryService.stream(..., out) until D67)
        var streamed = written(query, false);
        assertTrue(streamed.length > 0, "stream wrote nothing");
    }

    @Test
    @DisplayName("the lease RELEASES a caller-owned connection and never a store-owned one")
    void leaseReleasesOwnedKeepsBorrowed() throws Exception {
        // The contract itself, asserted directly rather than through a proxy
        // signal. This test lives in com.legend.server so it can call the
        // package-private resolver; the earlier witness for this defect counted
        // open file descriptors, which works only where a database opens one
        // per connection (SQLite does, DuckDB keeps ONE instance per path and
        // hides it) and only on POSIX. isClosed() is exact, is the same on
        // every platform including the Windows runner where the leak first
        // showed as an undeletable file, and says what we actually mean.
        com.legend.exec.Sessions.Session owned;
        try (com.legend.exec.Sessions.Session session =
                ConnectionResolver.session(com.legend.Compiler.compileModel(duckModel), "test::TestRuntime")) {
            owned = session;
            assertTrue(!com.legend.exec.Sessions.closed(owned), "a session must hand out a live connection");
        }
        assertTrue(com.legend.exec.Sessions.closed(owned),
                "a file-backed connection is CALLER-OWNED: the lease must close"
                        + " it, or every auto-resolving call leaks"
                        + " one (docs/CONNECTION_LEASE_DESIGN_2026_09_09.md)");

        com.legend.exec.Sessions.Session borrowed;
        try (com.legend.exec.Sessions.Session session =
                ConnectionResolver.session(com.legend.Compiler.compileModel(memModel), "test::TestRuntime")) {
            borrowed = session;
        }
        assertTrue(!com.legend.exec.Sessions.closed(borrowed),
                "an in-memory connection is STORE-OWNED: closing it would drop"
                        + " the tables HandleStore exists to keep, and break the"
                        + " persistence ConnectionIsolationTest pins");
    }

    @Test
    @DisplayName("the server RELEASES the connection it resolved (end to end)")
    void theServerReleasesWhatItResolves() throws Exception {
        // THE REGRESSION WITNESS, and the only test here that would fail
        // against the unfixed product rather than fail to COMPILE against it:
        // it names no lease type and goes through the public seeding entry
        // (Seed.sql), so it also catches a call site that stops closing.
        //
        // The signal is DuckDB's own: a file-backed database keeps a
        // write-ahead log while any connection is open, and checkpoints it
        // away when the last one closes (measured). So a leaked connection
        // leaves a .wal behind and a released one does not — true on every
        // platform, because it is the database's behaviour and not the OS's.
        Path own = Files.createTempFile("lease-wal-", ".duckdb");
        Files.delete(own);
        String ownModel = memModel.replace("specification: DuckDB { };",
                "specification: DuckDB { path: '"
                        + own.toString().replace("\\", "/") + "'; };");
        Path wal = Path.of(own + ".wal");
        try {
            Seed.sql(ownModel, "CREATE TABLE T_PERSON (ID INTEGER PRIMARY KEY,"
                    + " FIRST_NAME VARCHAR(100))", "test::TestRuntime");
            Seed.sql(ownModel, "INSERT INTO T_PERSON VALUES (1, 'Alice')",
                    "test::TestRuntime");
            assertTrue(!Files.exists(wal),
                    "the server did not release the connection it resolved:"
                            + " DuckDB left its write-ahead log at " + wal
                            + ", which it checkpoints away only when the last"
                            + " connection closes. Every auto-resolving call"
                            + " leaks one connection"
                            + " (docs/CONNECTION_LEASE_DESIGN_2026_09_09.md).");
            // and the Windows symptom, asserted where it is load-bearing:
            // POSIX unlinks open files happily, so this is the real check on
            // the runner and passes trivially here.
            assertTrue(Files.deleteIfExists(own),
                    "the database file could not be deleted, so something"
                            + " still holds it open: " + own);
        } finally {
            Files.deleteIfExists(wal);
            Files.deleteIfExists(own);
        }
    }
}
