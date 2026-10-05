// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Execution;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pure's {@code first()} over a group is any non-empty value of the group (Pure empties vanish from a collection,
 * and the engine's own spelling is unordered too). Lite spells it {@code ANY_VALUE}, which the product's H2
 * (2.1.214) does not have: {@code groupBy(…, y|$y->first())} failed on H2 with "function not found" (rebuild W0.6
 * push 13, homework 4 D, E5). On H2 a comparable scalar takes {@code MIN}, a valid "any non-empty value".
 */
class H2FirstInGroupTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB ( Table t ( k INTEGER, s VARCHAR(32), n INTEGER ) )
            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    private static List<String> run(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:firstgrp" + System.nanoTime())) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t (k INTEGER, s VARCHAR(32), n INTEGER)");
                st.execute("INSERT INTO t VALUES (1, NULL, NULL), (1, 'b', 2), (2, 'z', 9)");
            }
            var r = Execution.execute(MODEL, query, "local::RT", c);
            return r.rows().stream().map(row -> row.get(0) + "|" + row.get(1)).toList();
        }
    }

    @Test
    void firstOfAGroupRunsOnAPlainH2AndSkipsEmpties() throws Exception {
        // was: "Function "ANY_VALUE" not found" on H2 2.1.214
        assertEquals(List.of("1|b", "2|z"), run("|#>{local::DB.t}#->groupBy(~[k], ~[f: x|$x.s : y|$y->first()])"
                + "->sort([~k->ascending()])"));
        assertEquals(List.of("1|2", "2|9"), run("|#>{local::DB.t}#->groupBy(~[k], ~[f: x|$x.n : y|$y->first()])"
                + "->sort([~k->ascending()])"));
    }
}
