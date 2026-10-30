// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testing.Lean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The connection lease on H2 ({@link ConnectionLeaseTest}'s case that is H2's own; moved here from it by the dialect
 * program's Phase 6, 2026-10-16): an in-memory H2 database outlives the connection the lease closes.
 */
class ConnectionLeaseH2Test {

    @Test
    @DisplayName("an in-memory H2 database SURVIVES the lease closing its connection")
    void anInMemoryDatabaseSurvivesRelease() throws Exception {
        // The lease CLOSES every H2 connection (H2 has no store-owned arm), so
        // this design is only sound because an in-memory H2 database outlives
        // its last connection — the engine spells DB_CLOSE_DELAY=-1 into the
        // URL for exactly that reason. Pinned HERE, through the product path,
        // rather than left as a claim in a design document: if the URL ever
        // loses that setting, closing becomes silently destructive and this
        // test is what says so.
        String model = """
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
        Seed.sql(model, "CREATE TABLE T_THING (ID INTEGER PRIMARY KEY,"
                + " NAME VARCHAR(100))", "test::H2Runtime");
        Seed.sql(model, "INSERT INTO T_THING VALUES (1, 'kept')",
                "test::H2Runtime");
        // a SEPARATE auto-resolve: the previous call's connection is closed by
        // its lease, so the row survives only if the database does
        var back = Lean.service(model,
                "|model::Thing.all()->project(~[n: t|$t.name])", "test::H2Runtime");
        assertTrue(back.rows().size() == 1,
                "the in-memory H2 database did not survive its connection being"
                        + " released — closing it is destructive, and the lease"
                        + " must stop treating H2 as caller-owned");
    }
}
