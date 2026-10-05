// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Connection-cache isolation witnesses (type audit D100): the resolver's
 * cache key must include the model's STORE declarations, not just the
 * connection definition — a connection-text-only key handed two
 * unrelated models each other's tables (the audit's one cross-caller
 * data leak; A31-F1's repro declared the same column VARCHAR in one
 * model and INTEGER in the other and model B received model A's string
 * under an Integer[1] label). The persistence FEATURE stays: same
 * stores + same definition keep their database across requests — which
 * is what keeps the interactive HTTP flow working, where /engine/sql
 * seeds with a model text and a later request carries the same stores
 * inside a different model text (different SOURCE, same stores).
 */
class ConnectionIsolationTest {

    private static final String RUNTIME_TAIL = """
            ###Mapping
            Mapping test::M ( )
            ###Connection
            RelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
            """;

    private static final String MODEL_A = """
            ###Pure
            Class test::A { x: Integer[1]; }
            ###Relational
            Database store::DB ( Table T_A (ID INTEGER) )
            """ + RUNTIME_TAIL;

    // SAME stores + connection as A, different ###Pure content — the
    // interactive model+query-blob shape; must SHARE A's database
    private static final String MODEL_A_PRIME = """
            ###Pure
            Class test::A { x: Integer[1]; }
            Class test::Extra { y: String[1]; }
            ###Relational
            Database store::DB ( Table T_A (ID INTEGER) )
            """ + RUNTIME_TAIL;

    // DIFFERENT store declaration (the audit repro's shape), identical
    // ###Connection/###Runtime blocks — must get its OWN database
    private static final String MODEL_B = """
            ###Pure
            Class test::B { y: String[1]; }
            ###Relational
            Database store::DB ( Table T_A (ID VARCHAR(50)) )
            """ + RUNTIME_TAIL;

    /** Nothing outlives the method: LEAK_T sits in model A's in-memory database, which the server's connection store
     *  keeps for the JVM (Bazel workplan P3-03). */
    @org.junit.jupiter.api.AfterEach
    void dropLeakTable() throws Exception {
        Seed.sql(MODEL_A, "DROP TABLE IF EXISTS LEAK_T", "test::RT");
    }

    @Test
    void distinctStoresSharingAConnectionDefinitionDoNotShareADatabase()
            throws Exception {
        Seed.sql(MODEL_A, "CREATE TABLE LEAK_T (ID INTEGER)", "test::RT");
        Seed.sql(MODEL_A, "INSERT INTO LEAK_T VALUES (42)", "test::RT");
        // the persistence FEATURE, and the interactive-blob flow: same
        // stores + definition (different non-store source) SHARE
        assertDoesNotThrow(() -> Seed.sql(MODEL_A,
                "SELECT * FROM LEAK_T", "test::RT"));
        assertDoesNotThrow(() -> Seed.sql(MODEL_A_PRIME,
                "SELECT * FROM LEAK_T", "test::RT"));
        // the LEAK (pre-fix): a model with a DIFFERENT store
        // declaration but identical connection text saw model A's
        // tables — now it must get its own database
        SQLException leak = assertThrows(SQLException.class,
                () -> Seed.sql(MODEL_B,
                        "SELECT * FROM LEAK_T", "test::RT"));
        assertTrue(String.valueOf(leak.getMessage()).contains("LEAK_T"),
                "expected a missing-table error for the OTHER model: "
                        + leak.getMessage());
    }
}
