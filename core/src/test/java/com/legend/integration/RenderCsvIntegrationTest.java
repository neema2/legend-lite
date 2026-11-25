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

/**
 * F4.1/F4.2 (RENDER): {@code toCSV} and a relation's {@code toString} over a store's rows, lite's own texts CONSTRUCTED
 * BY THE DATABASE (header + cells + RFC4180 escaping + row joining all in SQL; stage B's B2b S5a and S5b,
 * lowering.ProductRelationText); Java carries the bytes. The CSV's layout is the helper's
 * {@code meta::relational::tests::csv::toCSV} (helperFunctions.pure:198-232): header line, one line per row each
 * '\n'-terminated, empty cells for NULL (under the flag the word {@code null}, every String quoted: register S420),
 * RFC4180 double-quote escaping for
 * comma/quote-bearing text; each cell the product's text of its value (a StrictDate {@code yyyy-MM-dd}).
 */
@DisplayName("RENDER toCSV Integration")
class RenderCsvIntegrationTest extends AbstractDatabaseTest {



    @BeforeEach
    void setUp() throws SQLException {
        // every text here is the product's (stage B's B2b S5b, 2026-11-25: until then the edge's, and the session the
        // edge's database, the audit's J22)
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

    /** A relation's text in the product: lite's own, computed by the database (stage B's B2b S5a and S5b; until then
     *  the edge's, each text here planned in the edge's style). */
    private String csv(String query) throws SQLException {
        return Lean.run(MODEL, query, "test::RT", connection).one().toString();
    }

    @Test
    void aTdsCsvIsLitesCsvAsToCsvWritesIt() throws SQLException {
        // the user's ruling of 2026-11-25, ".csv in product, yes same as toCSV" (register S405): a TDS's csv, over a
        // stored relation cast to a TDS or a #TDS literal, is lite's own CSV computed by the database, exactly as
        // toCSV(t) writes it (refused by name from stage B's B2b S5c until the ruling; a last step's csv at
        // legend-pure's and legend-engine's doors is legend-engine's text, the doors' Java printer's)
        String stored = "|test::Person.all()->project([p | $p.id, p | $p.name], ['id', 'name'])->sort(asc('id'))";
        assertEquals(csv(stored + "->meta::relational::tests::csv::toCSV()"),
                csv(stored + "->cast(@meta::pure::metamodel::relation::TDS<Any>).csv"));
        assertEquals("i,s\n1,a\n2,\n", csv("|#TDS\n  i:Integer, s:String\n  1, a\n  2, \n#.csv"));
        assertEquals(csv("|#TDS\n  i:Integer, s:String\n  1, a\n  2, \n#->meta::relational::tests::csv::toCSV()"),
                csv("|#TDS\n  i:Integer, s:String\n  1, a\n  2, \n#.csv"));
        // the review of d6e94cdfe (2026-11-25): a relation's column named csv is that column's cells, never the
        // relation's text (the TDS class's csv is String[1], a column read [*]: PlatformTypes.isTdsCsvRead)
        assertEquals("x|y", csv("|#TDS\n  csv:String, a:Integer\n  x, 1\n  y, 2\n#->filter(r|$r.a > 0)"
                + "->sort(~a->ascending()).csv->joinStrings('|')"));
        // a value whose columns the compiler does not know (a cast to TDS<Any> the resolver does not peel) is refused
        // by name, by toCSV, by a TDS's csv and by toString alike (until then toCSV, and until B2b's design audit of
        // 2026-11-25 toString, reached an internal error there)
        for (String q : java.util.List.of(
                "|#TDS\n  i:Integer\n  1\n#->cast(@meta::pure::metamodel::relation::TDS<Any>).csv",
                "|#TDS\n  i:Integer\n  1\n#->cast(@meta::pure::metamodel::relation::TDS<Any>)"
                        + "->meta::relational::tests::csv::toCSV()",
                "|#TDS\n  i:Integer\n  1\n#->cast(@meta::pure::metamodel::relation::TDS<Any>)->toString()")) {
            Exception refused = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> csv(q));
            String chain = "";
            for (Throwable t = refused; t != null; t = t.getCause()) {
                chain += t.getMessage() + " | ";
            }
            org.junit.jupiter.api.Assertions.assertTrue(chain.contains("over a value whose columns the compiler does"
                    + " not know"), q + ": " + chain);
        }
    }

    /** A stored relation's toString: lite's own text, computed by the database (stage B's B2b S5a, 2026-11-24; the
     *  review of S5a asked for the stored relation whose refusal this class pinned), in the product since S5b: a comma
     *  and a quote
     *  inside a cell stay as they are (a table's text quotes no cell but a braced one), an empty cell is null, a
     *  StrictDate its day. */
    @Test
    void aStoredRelationsToStringIsLitesOwnTextComputedByTheDatabase() throws SQLException {
        assertEquals("#TDS\n   id,name,nick,dob\n   1,Ann,The, Ann,2001-02-03\n   2,Bob \"B\",null,1999-12-31\n#",
                csv("|test::Person.all()->project([p | $p.id, p | $p.name, p | $p.nick, p | $p.dob],"
                        + " ['id', 'name', 'nick', 'dob'])->sort(asc('id'))->meta::pure::functions::relation::toString()"));
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

    /** A Float cell of the CSV text, the product's text of a Float (six decimals, the trailing zeros trimmed, a whole
     *  value keeping one, S311):
     *  never an in-query cast's exponent (PR #118 fix round, 2026-11-18: DuckDB's wrote 1e-05, H2's 1.0E-5) -- a DOUBLE
     *  the database computes. */
    @Test
    void csvTextPrintsAFloatCellAsLitesText() throws SQLException {
        assertEquals("""
                x
                0.00001
                0.00002
                """,
                csv("|test::Person.all()->project([p | $p.id->toFloat() / 100000.0], ['x'])->sort(asc('x'))"
                        + "->meta::relational::tests::csv::toCSV()"));
    }

    /** The flag in the product (the user's ruling of 2026-11-25, register S420): a null the word null, every String
     *  quoted; legend-engine's TDSNull is its doors' (edge.PrintedRelations). */
    @Test
    void renderTdsNullFlagWritesLitesExplicitNull() throws SQLException {
        assertEquals("""
                id,nick
                1,"The, Ann"
                2,null
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
    void emptyRelationRendersTheHeaderLineAlone() throws SQLException {
        // lite's CSV: every row ends with its own newline, so no rows is the header line alone (the user's ruling of
        // 2026-11-25, Q-C, "Okay to both csv ones"; register S420) -- where legend-engine's helper writes a second
        // newline (joinStrings('', '\n', '\n') over zero rows is '\n')
        assertEquals("id\n",
                csv("|test::Person.all()->filter(p | $p.id > 99)"
                        + "->project([p | $p.id], ['id'])"
                        + "->meta::relational::tests::csv::toCSV()"));
    }
}
