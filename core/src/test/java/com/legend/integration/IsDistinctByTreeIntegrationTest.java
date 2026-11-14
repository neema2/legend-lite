package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pure's by-tree {@code isDistinct(collection, #{T{a, b}}#)} — "no
 * duplicates comparing the elements by the tree's leaves" (engine
 * collectionExtension.pure; implemented upstream only in the generated-Java
 * plan binding). Ours: a struct of the leaves under the collection
 * reducer, COUNT(DISTINCT row) = COUNT(row), on the database.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class IsDistinctByTreeIntegrationTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = LaneFloor.backend().open();
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))");
            s.execute("CREATE TABLE T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100),"
                    + " LAST_NAME VARCHAR(100), AGE INTEGER, FIRM_ID INTEGER)");
            s.execute("INSERT INTO T_FIRM VALUES (1, 'Dupes'), (2, 'Clean'), (3, 'Empty')");
            // Dupes: two 'Ann Smith' rows differing only in age; Clean: distinct names
            s.execute("INSERT INTO T_PERSON VALUES (1, 'Ann', 'Smith', 30, 1), (2, 'Ann', 'Smith', 41, 1),"
                    + " (3, 'Bob', 'Jones', 22, 1), (4, 'Ann', 'Smith', 30, 2), (5, 'Ann', 'Stone', 30, 2)");
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    private static final String MODEL = """
            ###Pure
            Class test::Person { firstName: String[1]; lastName: String[1]; age: Integer[1]; }
            Class test::Firm { legalName: String[1]; employees: test::Person[*]; }
            ###Relational
            Database store::DB
            (
                Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))
                Table T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100), AGE INTEGER, FIRM_ID INTEGER)
                Join FirmPerson (T_FIRM.ID = T_PERSON.FIRM_ID)
            )
            ###Mapping
            Mapping test::M
            (
                test::Person: Relational
                {
                    ~mainTable [store::DB] T_PERSON
                    firstName: [store::DB] T_PERSON.FIRST_NAME,
                    lastName: [store::DB] T_PERSON.LAST_NAME,
                    age: [store::DB] T_PERSON.AGE
                }
                test::Firm: Relational
                {
                    ~mainTable [store::DB] T_FIRM
                    legalName: [store::DB] T_FIRM.LEGAL_NAME,
                    employees: [store::DB] @FirmPerson
                }
            )
            ###Connection
            RelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
            """;

    @Test
    void distinctByTheTreesLeaves() throws SQLException {
        Lean.Result r = Lean.run(MODEL, """
                test::Firm.all()->project([
                    f | $f.legalName,
                    f | $f.employees->isDistinct(#{test::Person {firstName, lastName}}#),
                    f | $f.employees->isDistinct(#{test::Person {firstName, lastName, age}}#)
                ], ['firm', 'byName', 'byNameAndAge'])->sort('firm')
                """, "test::RT", conn);
        // Clean: distinct by name; Dupes: 'Ann Smith' twice by name, distinct once age joins the key;
        // Empty: no employees — vacuously distinct
        assertEquals("[Clean, true, true] [Dupes, false, true] [Empty, true, true]",
                String.join(" ", r.rows().stream().map(row -> row.toString()).toList()),
                "by-tree distinct per firm");
    }
}
