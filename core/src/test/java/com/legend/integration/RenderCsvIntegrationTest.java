// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * F4.1/F4.2 (RENDER): {@code toCSV} is a platform lowering — the CSV
 * text is CONSTRUCTED BY THE DATABASE (header + cells + RFC4180
 * escaping + row joining all in SQL); Java carries the bytes. Pinned
 * against the engine's {@code meta::relational::tests::csv::toCSV}
 * semantics (helperFunctions.pure:198-232): header line, one line per
 * row each '\n'-terminated, empty cells for NULL ({@code TDSNull}
 * under the flag), dates as {@code yyyy-MM-dd}, RFC4180 double-quote
 * escaping for comma/quote-bearing text.
 */
@DisplayName("RENDER toCSV Integration")
class RenderCsvIntegrationTest extends AbstractDatabaseTest {



    @BeforeEach
    void setUp() throws SQLException {
        connection = backend().open();
        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE T_PERSON (ID INTEGER, NAME VARCHAR,"
                    + " NICK VARCHAR, DOB DATE)");
            st.execute("INSERT INTO T_PERSON VALUES"
                    + " (1, 'Ann', 'The, Ann', '2001-02-03'),"
                    + " (2, 'Bob \"B\"', NULL, '1999-12-31')");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private static final String MODEL = """
            Class test::Person { id: Integer[1]; name: String[1];
                nick: String[0..1]; dob: StrictDate[1]; }
            ###Relational
            Database test::DB ( Table T_PERSON ( ID INTEGER PRIMARY KEY,
                NAME VARCHAR(50), NICK VARCHAR(50), DOB DATE ) )
            ###Mapping
            Mapping test::M ( test::Person: Relational { ~mainTable [DB] T_PERSON
                id: [DB] T_PERSON.ID, name: [DB] T_PERSON.NAME,
                nick: [DB] T_PERSON.NICK, dob: [DB] T_PERSON.DOB } )
            ###Connection
            RelationalDatabaseConnection store::Conn { type: DuckDB;
                specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime test::RT { mappings: [ test::M ];
                connections: [ test::DB: [ environment: store::Conn ] ]; }
            """;

    private String csv(String query) throws SQLException {
        Lean.Result r = Lean.run(MODEL, query, "test::RT", connection);
        return r.one().toString();
    }

    @Test
    void csvTextIsBuiltByTheDatabase() throws SQLException {
        // sorted rows; a comma-bearing cell quotes; a quote-bearing cell
        // doubles its quotes; NULL renders empty; dates spell yyyy-MM-dd
        assertEquals("""
                id,name,nick,dob
                1,Ann,"The, Ann",2001-02-03
                2,"Bob ""B""\",,1999-12-31
                """,
                csv("|test::Person.all()->project([p | $p.id, p | $p.name,"
                        + " p | $p.nick, p | $p.dob],"
                        + " ['id', 'name', 'nick', 'dob'])"
                        + "->sort(asc('id'))"
                        + "->meta::relational::tests::csv::toCSV()"));
    }

    @Test
    void renderTdsNullFlagSpellsNulls() throws SQLException {
        assertEquals("""
                id,nick
                1,"The, Ann"
                2,TDSNull
                """,
                csv("|test::Person.all()->project([p | $p.id, p | $p.nick],"
                        + " ['id', 'nick'])->sort(asc('id'))"
                        + "->meta::relational::tests::csv::toCSV(true)"));
    }

    @Test
    void tdsToStringIsBuiltByTheDatabase() throws SQLException {
        // engine toString.pure: '#TDS' + 3-space header + 3-space rows
        assertEquals("""
                #TDS
                   id,name
                   1,Ann
                   2,Bob "B"
                #""",
                csv("|test::Person.all()->project([p | $p.id, p | $p.name],"
                        + " ['id', 'name'])->sort(asc('id'))"
                        + "->meta::pure::functions::relation::toString()"));
    }

    @Test
    void tdsToStringEmptyRelation() throws SQLException {
        assertEquals("#TDS\n   id\n\n#",
                csv("|test::Person.all()->filter(p | $p.id > 99)"
                        + "->project([p | $p.id], ['id'])"
                        + "->meta::pure::functions::relation::toString()"));
    }

    @Test
    void emptyRelationRendersHeaderAndBlank() throws SQLException {
        // engine joinStrings('', '\n', '\n') over zero rows = '\n'
        assertEquals("id\n\n",
                csv("|test::Person.all()->filter(p | $p.id > 99)"
                        + "->project([p | $p.id], ['id'])"
                        + "->meta::relational::tests::csv::toCSV()"));
    }
}
