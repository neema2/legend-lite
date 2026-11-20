// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.ExecuteOptions;
import com.legend.platform.Feature;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import com.legend.testing.Program;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * PURE'S substring AND indexOf, IN THE DATABASE (A3 of the compatibility program, 2026-11-19; the user's D1, "Pure's way,
 * LOUD, no translator"; register S130, S134, S383): {@code substring} is 0-based and end-exclusive, {@code indexOf}
 * 0-based with -1 on a miss, and an out-of-range {@code substring} RAISES as legend-pure's (Java's String.substring)
 * does -- a start below 0, an end past the text's length, an end before the start -- each the database's check over
 * the row's text, on every in-process database. One rule answers {@code substring} with or without
 * CORRECT_SQL_SUBSTRING_INDEXING and {@code substr}, its synonym (the A3 review's P2-M1, 2026-11-20).
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

    private static final ExecuteOptions FLAGGED =
            ExecuteOptions.NONE.withFeatures(Set.of(Feature.CORRECT_SQL_SUBSTRING_INDEXING));

    /** The four out-of-range calls: a start below 0; an end past the length (8); an end before the start; the
     *  1-argument form past the end. */
    private static final List<String> OUT_OF_RANGE = List.of("(-1)", "(0, 9)", "(4, 2)", "(9)");

    @Test
    void substrAndTheFlaggedSubstringRaiseToo() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (String range : OUT_OF_RANGE) {
                // substr, Pure's synonym of substring: the same rule
                try (Connection c = backend.open()) {
                    var e = assertThrows(com.legend.error.DataError.class, () -> Lean.established(model,
                            "|p::Party.all()->project(~[s: p|$p.name->substr" + range + "])", "p::RT", c),
                            backend.type() + ": substr" + range);
                    assertEquals(com.legend.error.ErrorKind.SUBSTRING_OUT_OF_RANGE, e.kind(), backend.type() + ": substr" + range);
                }
                // substring under CORRECT_SQL_SUBSTRING_INDEXING: the flag selects the same rule
                try (Connection c = backend.open()) {
                    var e = assertThrows(com.legend.error.DataError.class, () -> Program.last(model,
                            "|'Meridian'->substring" + range, "p::RT", c, FLAGGED),
                            backend.type() + ": flagged substring" + range);
                    assertEquals(com.legend.error.ErrorKind.SUBSTRING_OUT_OF_RANGE, e.kind(),
                            backend.type() + ": flagged substring" + range);
                }
            }
            try (Connection c = backend.open()) {
                assertEquals("eri", Program.last(model, "|'Meridian'->substring(1, 4)", "p::RT", c, FLAGGED).one(),
                        String.valueOf(backend.type()));
            }
        }
    }

    @Test
    void aComputedIndexIsCheckedOnEachRow() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            // a second row whose id (9) is past its text's length (2): the index is the row's own column
            String model = MODEL.formatted(backend.type()).replace("1,Meridian\\n", "1,Meridian\\n9,Al\\n");
            try (Connection c = backend.open()) {
                Lean.Result r = Lean.established(model, "|p::Party.all()->filter(p|$p.id == 1)->project(~[a: p|$p.name"
                        + "->substring($p.id), b: p|$p.name->substring(0, $p.id)])", "p::RT", c);
                assertEquals(List.of(List.of("eridian", "M")), r.rows(), backend.type() + ": " + r.text());
            }
            for (String call : List.of("substring($p.id)", "substring(0, $p.id)")) {
                try (Connection c = backend.open()) {
                    var e = assertThrows(com.legend.error.DataError.class, () -> Lean.established(model,
                            "|p::Party.all()->project(~[s: p|$p.name->" + call + "])", "p::RT", c), backend.type() + ": " + call);
                    assertEquals(com.legend.error.ErrorKind.SUBSTRING_OUT_OF_RANGE, e.kind(), backend.type() + ": " + call);
                }
            }
        }
    }

    @Test
    void anEmptySearchPastTheEndFindsTheLength() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            try (Connection c = backend.open()) {
                // Java's String.indexOf clamps from to [0, the length]: 'Meridian' is 8 long
                Lean.Result r = Lean.established(model, "|p::Party.all()->project(~[a: p|$p.name->indexOf('', 20),"
                        + " b: p|$p.name->indexOf('', 3), c: p|$p.name->indexOf('', -2), d: p|$p.name->indexOf('n', 20),"
                        + " e: p|$p.name->indexOf('', $p.id + 10)])", "p::RT", c);
                assertEquals(List.of(List.of(8L, 3L, 0L, -1L, 8L)), r.rows(), backend.type() + ": " + r.text());
            }
        }
    }
}
