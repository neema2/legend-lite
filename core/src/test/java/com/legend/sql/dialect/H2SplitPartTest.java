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
 * Pure's {@code splitPart} on a PLAIN H2 session, one with none of the engine's extension functions installed —
 * which is what a product user's H2 is. Before execution plan W0.2(a) the product H2 dialect spelled it
 * {@code legend_h2_extension_split_part(…)}, a function only the engine's H2 and our corpus harness install, so
 * this query failed with "function not found"; the corpus could not see it because its H2 lane installs the alias
 * on the product session. The expected values are the engine's meaning (commons split): the separator is a set of
 * characters, adjacent separators collapse, the index is 0-based, and a part past the end has no value.
 */
class H2SplitPartTest {

    private static final String MODEL = """
            ###Relational
            Database local::DB ( Table t ( id INTEGER, s VARCHAR(32) ) )
            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime local::RT
            { mappings: []; connections: [ local::DB: [ c1: local::Conn ] ]; }
            """;

    private static List<String> run(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:splitpart" + System.nanoTime())) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE t (id INTEGER, s VARCHAR(32))");
                st.execute("INSERT INTO t VALUES (1, 'a,,b'), (2, ',x,'), (3, 'solo'), (4, 'p;q,r')");
            }
            var r = Execution.execute(MODEL, query, "local::RT", c);
            return r.rows().stream().map(row -> row.get(0) + "|" + row.get(1) + "|" + row.get(2)).toList();
        }
    }

    @Test
    void splitPartRunsOnAPlainH2WithTheEnginesMeaning() throws Exception {
        assertEquals(List.of("a|b|null", "x|null|null", "solo|null|null", "p;q|r|null"), run(
                "|#>{local::DB.t}#->extend(~[p0: x|$x.s->toOne()->splitPart(',', 0),"
                        + " p1: x|$x.s->toOne()->splitPart(',', 1), p2: x|$x.s->toOne()->splitPart(',', 2)])"
                        + "->sort([~id->ascending()])->select(~[p0, p1, p2])"));
    }

    @Test
    void aMultiCharacterSeparatorIsASetOfCharacters() throws Exception {
        assertEquals(List.of("a|b|null", "x|null|null", "solo|null|null", "p|q|r"), run(
                "|#>{local::DB.t}#->extend(~[p0: x|$x.s->toOne()->splitPart(',;', 0),"
                        + " p1: x|$x.s->toOne()->splitPart(',;', 1), p2: x|$x.s->toOne()->splitPart(',;', 2)])"
                        + "->sort([~id->ascending()])->select(~[p0, p1, p2])"));
    }
}
