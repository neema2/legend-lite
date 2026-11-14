// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.Lean;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link StoreTypesTest}'s case that is DuckDB's own (moved here from it by the dialect program's Phase 6,
 * 2026-10-16): a column of DuckDB's UUID type, declared OTHER.
 */
class StoreTypesDuckDbTest {

    @Test
    void aUuidDeclaredOtherFiltersAndGroupsAsTheTextItShows() throws Exception {
        String model = """
                ###Relational
                Database s::DB ( Table HOSTS (ID INTEGER PRIMARY KEY, REF OTHER) )
                ###Connection
                RelationalDatabaseConnection s::Conn { store: s::DB; type: DuckDB;
                  specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """;
        try (Connection c = TestBackends.of(DatabaseType.DuckDB).open()) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE HOSTS (ID INTEGER, REF UUID)");
                st.execute("INSERT INTO HOSTS VALUES (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),"
                        + " (2, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), (3, 'b1ffcd00-0000-4000-8000-000000000001')");
            }
            var r = Lean.run(model, "|#>{s::DB.HOSTS}#->filter(x|$x.REF->toOne()->startsWith('a0ee'))"
                    + "->groupBy(~[REF], ~[n: x|$x.ID : y|$y->count()])", "s::RT", c);
            assertEquals(List.of("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|2"),
                    r.rows().stream().map(row -> row.get(0) + "|" + row.get(1)).toList());
        }
    }
}
