// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.testing.Lean;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A project's query on legend-lite's server answers its own Data rows (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5):
 * the in-memory database the server keeps for the project's connection is made holding them, as a tab's and
 * Python's project database are -- so Query and Studio on the server show the rows a tab shows -- and an edited row is
 * a fresh database, never the earlier rows.
 */
class ProjectRowsTest {

    private static final String PARTY = """
            Class proj::Party { id: Integer[1]; name: String[1]; }
            ###Relational
            Database proj::PartyDB ( Schema PARTY ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(200)) ) )
            ###Mapping
            Mapping proj::PartyMapping
            (
              *proj::Party: Relational
              {
                ~primaryKey ([proj::PartyDB] PARTY.PARTY.ID)
                ~mainTable [proj::PartyDB] PARTY.PARTY
                id: [proj::PartyDB] PARTY.PARTY.ID,
                name: [proj::PartyDB] PARTY.PARTY.NAME
              }
            )
            ###Connection
            RelationalDatabaseConnection proj::Conn { store: proj::PartyDB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime proj::RT { mappings: [ proj::PartyMapping ]; connections: [ proj::PartyDB: [ db: proj::Conn ] ]; }
            ###Data
            Data proj::PartyData
            {
              Relational
              #{
                PARTY.PARTY:
                  'ID,NAME\\n1,Meridian Capital\\n2,Halberd Securities\\n';
              }#
            }
            """;

    private static final String QUERY = "|proj::Party.all()->project(~[name: p|$p.name])->sort(~name->ascending())";

    private static List<Object> names(String model) throws Exception {
        return Lean.service(model, QUERY, "proj::RT").rows().stream().map(r -> r.get(0)).toList();
    }

    @Test
    void aProjectsQueryAnswersItsOwnRows() throws Exception {
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(PARTY));
        // asked again: the same rows
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(PARTY));
    }

    /** A Data element over a table no Database declares: every query on the model refused by name (the database the
     *  server made for it let go), where before C5 it ran on an empty database. */
    @Test
    void aDataElementOverAnUndeclaredTableIsRefusedByName() {
        String bad = PARTY.replace("PARTY.PARTY:\n", "PARTY.NOPE:\n");
        IllegalArgumentException refused = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> names(bad));
        assertEquals("the Data element proj::PartyData fills PARTY.NOPE, which no Database of the model declares",
                refused.getMessage());
    }

    @Test
    void anEditedRowIsAFreshDatabase() throws Exception {
        String edited = PARTY.replace("2,Halberd Securities", "3,Kestrel Partners");
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(PARTY));
        assertEquals(List.of("Kestrel Partners", "Meridian Capital"), names(edited));
        assertEquals(List.of("Halberd Securities", "Meridian Capital"), names(PARTY));
    }
}
