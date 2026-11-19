// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * PURE'S substring AND indexOf, IN THE DATABASE (A3 of the compatibility program, 2026-11-19; the user's D1, "Pure's way,
 * LOUD, no translator"; register S130, S134, S383): {@code substring} is 0-based and end-exclusive, {@code indexOf}
 * 0-based with -1 on a miss, and an out-of-range {@code substring} RAISES as legend-pure's (Java's String.substring)
 * does -- a start below 0, an end past the text's length, an end before the start -- each the database's check over
 * the row's text, on every in-process database.
 */
class SubstringRangeTest {

    private static final String MODEL = """
            ###Pure
            Class p::Party { id: Integer[1]; name: String[1]; }
            ###Relational
            Database p::DB ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(64)) )
            ###Mapping
            Mapping p::M ( p::Party: Relational { ~mainTable [p::DB] PARTY id: PARTY.ID, name: PARTY.NAME } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 {
                    testDataSetupCSV: 'default\\nPARTY\\nID,NAME\\n1,Meridian\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    @Test
    void substringAndIndexOfArePures() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            try (Connection c = backend.open()) {
                Lean.Result r = Lean.established(model, "|p::Party.all()->project(~[a: p|$p.name->substring(0, 3),"
                        + " b: p|$p.name->substring(1), c: p|$p.name->substring(8), d: p|$p.name->indexOf('r'),"
                        + " e: p|$p.name->indexOf('zz'), f: p|$p.name->indexOf('i', 4)])", "p::RT", c);
                assertEquals(List.of(List.of("Mer", "eridian", "", 2L, -1L, 5L)), r.rows(), backend.type() + ": " + r.text());
            }
        }
    }

    @Test
    void anOutOfRangeSubstringRaises() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            // a start below 0; an end past the length (8); an end before the start; the 1-argument form past the end
            for (String call : List.of("substring(-1)", "substring(0, 9)", "substring(4, 2)", "substring(9)")) {
                try (Connection c = backend.open()) {
                    var e = assertThrows(com.legend.error.DataError.class, () -> Lean.established(model,
                            "|p::Party.all()->project(~[s: p|$p.name->" + call + "])", "p::RT", c), backend.type() + ": " + call);
                    assertEquals(com.legend.error.ErrorKind.SUBSTRING_OUT_OF_RANGE, e.kind(), backend.type() + ": " + call);
                }
            }
        }
    }
}
