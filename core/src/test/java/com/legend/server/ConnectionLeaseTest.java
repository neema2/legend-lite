// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The AUTO-RESOLVING entry points of {@link QueryService} — the methods
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

    private static QueryService qs;
    /** DuckDB, file-backed: the caller-owned (unpooled) arm. */
    private static Path duckFile;
    private static String duckModel;
    /** DuckDB, in-memory: the store-owned (pooled) arm. */
    private static String memModel;

    @BeforeAll
    static void setup() throws Exception {
        qs = new QueryService();
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

    @Test
    @DisplayName("all four auto-resolving entry points execute")
    void allFour() throws Exception {
        String query = "|model::Person.all()->project(~[name: p|$p.firstName])";

        // 1. execute(pureSource, query, runtimeName) — QueryService:81
        assertNotNull(qs.execute(duckModel, query, "test::TestRuntime"),
                "3-arg execute returned no result");

        // 2. pure/v1 execute (E8) — PureV1Api.execute leases the runtime's connection
        String lambda = PureV1Api.grammarToJsonLambda(query, false).json();
        PureV1Api.Answer executed = PureV1Api.execute("{\"clientVersion\":\"vX_X_X\",\"function\":"
                + lambda + ",\"model\":" + com.legend.json.Json.toCompact(
                        java.util.Map.of("_type", "text", "code", duckModel))
                + ",\"runtime\":{\"_type\":\"runtimePointer\",\"runtime\":\"test::TestRuntime\"}"
                + ",\"context\":{\"_type\":\"BaseExecutionContext\"}}");
        assertEquals(200, executed.status(), executed.json());

        // 3. execute(…, OutputStream, OutputFormat) — QueryService:138
        var csv = new ByteArrayOutputStream();
        qs.execute(duckModel, query, "test::TestRuntime", csv, OutputFormat.CSV);
        assertTrue(csv.size() > 0, "wire execute wrote nothing");

        // (executeSql and /engine/sql were deleted by plan W0.1: raw SQL is not a product surface)

        // 5. stream(…, OutputStream) — QueryService:193
        var streamed = new ByteArrayOutputStream();
        qs.stream(duckModel, query, "test::TestRuntime", streamed);
        assertTrue(streamed.size() > 0, "stream wrote nothing");
    }

    @Test
    @DisplayName("an in-memory H2 database SURVIVES the lease closing its connection")
    void h2InMemorySurvivesRelease() throws Exception {
        // The lease CLOSES every H2 connection (H2 has no store-owned arm), so
        // this design is only sound because an in-memory H2 database outlives
        // its last connection — the engine spells DB_CLOSE_DELAY=-1 into the
        // URL for exactly that reason. Pinned HERE, through the product path,
        // rather than left as a claim in a design document: if the URL ever
        // loses that setting, closing becomes silently destructive and this
        // test is what says so.
        String h2Model = """
                ###Pure
                Class model::Thing { name: String[1]; }

                ###Relational
                Database store::H2DB ( Table T_THING ( ID INTEGER PRIMARY KEY, NAME VARCHAR(100) ) )

                ###Mapping
                Mapping model::ThingMapping (
                    model::Thing: Relational {
                        ~mainTable [store::H2DB] T_THING
                        name: [store::H2DB] T_THING.NAME
                    }
                )

                ###Connection
                RelationalDatabaseConnection store::H2Conn {
                    type: H2;
                    specification: LocalH2 {};
                    auth: Test;
                }

                ###Runtime
                Runtime test::H2Runtime {
                    mappings: [ model::ThingMapping ];
                    connections: [ store::H2DB: [ environment: store::H2Conn ] ];
                }
                """;
        Seed.sql(h2Model, "CREATE TABLE T_THING (ID INTEGER PRIMARY KEY,"
                + " NAME VARCHAR(100))", "test::H2Runtime");
        Seed.sql(h2Model, "INSERT INTO T_THING VALUES (1, 'kept')",
                "test::H2Runtime");
        // a SEPARATE auto-resolve: the previous call's connection is closed by
        // its lease, so the row survives only if the database does
        var back = qs.execute(h2Model,
                "|model::Thing.all()->project(~[n: t|$t.name])", "test::H2Runtime");
        assertTrue(back.rows().size() == 1,
                "the in-memory H2 database did not survive its connection being"
                        + " released — closing it is destructive, and the lease"
                        + " must stop treating H2 as caller-owned");
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
        Connection owned;
        try (ConnectionResolver.Lease lease =
                ConnectionResolver.lease(com.legend.Compiler.compileModel(duckModel), "test::TestRuntime")) {
            owned = lease.connection();
            assertTrue(!owned.isClosed(), "a lease must hand out a live connection");
        }
        assertTrue(owned.isClosed(),
                "a file-backed connection is CALLER-OWNED: the lease must close"
                        + " it, or every auto-resolving QueryService call leaks"
                        + " one (docs/CONNECTION_LEASE_DESIGN_2026_09_09.md)");

        Connection borrowed;
        try (ConnectionResolver.Lease lease =
                ConnectionResolver.lease(com.legend.Compiler.compileModel(memModel), "test::TestRuntime")) {
            borrowed = lease.connection();
        }
        assertTrue(!borrowed.isClosed(),
                "an in-memory connection is STORE-OWNED: closing it would drop"
                        + " the tables HandleStore exists to keep, and break the"
                        + " persistence ConnectionIsolationTest pins");
    }

    @Test
    @DisplayName("QueryService RELEASES the connection it resolved (end to end)")
    void queryServiceReleasesWhatItResolves() throws Exception {
        // THE REGRESSION WITNESS, and the only test here that would fail
        // against the unfixed product rather than fail to COMPILE against it:
        // it names no lease type and goes through the public QueryService
        // entry point, so it also catches a call site that stops closing.
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
                    "QueryService did not release the connection it resolved:"
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
