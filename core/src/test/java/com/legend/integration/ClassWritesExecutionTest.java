// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.exec.ExecutionResult;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The class writes over direct mappings executed end to end (docs/WRITES_DESIGN_2026_10_18.md §4, L2): a class's
 * instances written, merged and deleted by key, and an association end held by a foreign key linked and unlinked
 * from either end, each the L1 statement the store resolver writes over the mapping's table (resolver/ClassWrites).
 * A mapping that is not direct is refused by name. Each database the writes are measured on runs {@link #writes} in
 * a test named for it (DatabaseOwners: a database is named only by its owners).
 */
class ClassWritesExecutionTest {

    private static final String MODEL = """
            ###Relational
            Database c::DB (
              Table PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER, FIRM_ID INTEGER)
              Table FIRM (ID INTEGER PRIMARY KEY, LEGAL VARCHAR(20))
              Join PersonFirm(PERSON.FIRM_ID = FIRM.ID)
              Filter Grown(PERSON.AGE > 17)
            )
            ###Pure
            Class c::Person { name: String[0..1]; age: Integer[0..1]; }
            Class c::Firm { legal: String[0..1]; }
            Class c::Adult { name: String[0..1]; }
            Association c::Employment { firm: c::Firm[0..1]; employees: c::Person[*]; }
            ###Mapping
            Mapping c::M (
              c::Person: Relational { ~mainTable [c::DB]PERSON name: [c::DB]PERSON.NAME, age: [c::DB]PERSON.AGE }
              c::Firm: Relational { ~mainTable [c::DB]FIRM legal: [c::DB]FIRM.LEGAL }
              c::Adult: Relational { ~filter [c::DB]Grown ~mainTable [c::DB]PERSON name: [c::DB]PERSON.NAME }
              c::Employment: Relational { AssociationMapping ( firm: [c::DB]@PersonFirm, employees: [c::DB]@PersonFirm ) }
            )
            ###Connection
            RelationalDatabaseConnection c::Conn {
              store: c::DB;
              %s
            }
            ###Runtime
            Runtime c::RT { mappings: [ c::M ]; connections: [ c::DB: [ k: c::Conn ] ]; }
            """;

    /** A class whose mapping is not direct, a column that is no property: refused by name. */
    @Test
    void whatIsNotDirectIsRefusedByName() throws Exception {
        var backend = TestBackends.platform();
        try (Connection c = backend.openWorkspace()) {
            run(c, "CREATE TABLE PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER, FIRM_ID INTEGER)");
            Exception filtered = assertThrows(Exception.class, () -> count(backend, c,
                    "|#TDS\nk, name\n1, a\n#->meta::legend::lite::writeInstances(c::Adult, ~k)"));
            assertTrue(String.valueOf(filtered.getMessage()).contains("c::Adult is not writable"),
                    filtered.getMessage());
            Exception noProperty = assertThrows(Exception.class, () -> count(backend, c,
                    "|#TDS\nk, nope\n1, a\n#->meta::legend::lite::writeInstances(c::Person, ~k)"));
            assertTrue(String.valueOf(noProperty.getMessage()).contains("nope is no property"),
                    noProperty.getMessage());
        }
    }

    /** A class with an embedded property (a value class's sub-properties, each a column of the class's table). */
    private static final String EMBEDDED = """
            ###Relational
            Database e::DB (
              Table EVENT (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), SPAN_FROM INTEGER, SPAN_TO INTEGER)
            )
            ###Pure
            Class e::Span { start: Integer[1]; stop: Integer[1]; }
            Class e::Event { name: String[0..1]; span: e::Span[0..1]; }
            ###Mapping
            Mapping e::M (
              e::Event: Relational { ~mainTable [e::DB]EVENT name: [e::DB]EVENT.NAME,
                span ( start: [e::DB]EVENT.SPAN_FROM, stop: [e::DB]EVENT.SPAN_TO ) }
            )
            ###Connection
            RelationalDatabaseConnection e::Conn {
              store: e::DB;
              %s
            }
            ###Runtime
            Runtime e::RT { mappings: [ e::M ]; connections: [ e::DB: [ k: e::Conn ] ]; }
            """;

    /** An embedded property's sub-properties written by name ({@code span.start}, 2026-10-20: the Cypher island's
     *  zoned values and durations over a domain model): each the column the embedded mapping reads it from; a
     *  sub-property the value class lacks refused by name. */
    @Test
    void anEmbeddedPropertysSubPropertiesAreItsColumns() throws Exception {
        var backend = TestBackends.platform();
        try (Connection c = backend.openWorkspace()) {
            run(c, "CREATE TABLE EVENT (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), SPAN_FROM INTEGER, SPAN_TO INTEGER)");
            String model = EMBEDDED.formatted(backend.connectionBody());
            assertEquals(1L, ((ExecutionResult.Scalar) java.util.Objects.requireNonNull(com.legend.Execution.execute(
                    model, "|#TDS\nk, name\n1, a\n#->meta::legend::lite::writeInstances(e::Event, ~k)", "e::RT", c)))
                    .value());
            assertEquals(1L, ((ExecutionResult.Scalar) java.util.Objects.requireNonNull(com.legend.Execution.execute(
                    model, "|#TDS\nk, a, b\n1, 3, 4\n#->rename(~a, ~'span.start')->rename(~b, ~'span.stop')"
                    + "->meta::legend::lite::mergeInstances(e::Event, ~k)", "e::RT", c))).value());
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(
                    "SELECT ID, NAME, SPAN_FROM, SPAN_TO FROM EVENT")) {
                assertTrue(rs.next());
                assertEquals("1|a|3|4", rs.getInt(1) + "|" + rs.getString(2) + "|" + rs.getInt(3) + "|"
                        + rs.getInt(4));
            }
            Exception lacking = assertThrows(Exception.class, () -> com.legend.Execution.execute(model,
                    "|#TDS\nk, a\n1, 3\n#->rename(~a, ~'span.nope')->meta::legend::lite::mergeInstances(e::Event,"
                    + " ~k)", "e::RT", c));
            assertTrue(String.valueOf(lacking.getMessage()).contains("span.nope is no property"),
                    lacking.getMessage());
        }
    }

    /** Every check, on {@code backend}'s own workspace. */
    static void writes(TestBackend backend) throws Exception {
        try (Connection c = backend.openWorkspace()) {
            run(c, "CREATE TABLE PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(20), AGE INTEGER, FIRM_ID INTEGER)");
            run(c, "CREATE TABLE FIRM (ID INTEGER PRIMARY KEY, LEGAL VARCHAR(20))");
            String name = backend.name();
            // instances created: the key column into the primary key, each property into its column
            assertEquals(2L, count(backend, c, "|#TDS\nk, name, age\n1, a, 10\n2, b, 20\n#"
                    + "->meta::legend::lite::writeInstances(c::Person, ~k)"), name);
            assertEquals(1L, count(backend, c, "|#TDS\nk, legal\n7, F\n#"
                    + "->meta::legend::lite::writeInstances(c::Firm, ~k)"), name);
            assertEquals(List.of("1|a|10|null", "2|b|20|null"), people(c), name);
            // merged by key: the named properties set, the others as they were
            assertEquals(1L, count(backend, c, "|#TDS\nk, age\n1, 11\n#"
                    + "->meta::legend::lite::mergeInstances(c::Person, ~k)"), name);
            assertEquals(List.of("1|a|11|null", "2|b|20|null"), people(c), name);
            // an end the class's own table holds by foreign key, linked from the class's end
            assertEquals(1L, count(backend, c, "|#TDS\nk, firm\n1, 7\n#"
                    + "->meta::legend::lite::link(c::Person, ~firm, ~k)"), name);
            assertEquals(List.of("1|a|11|7", "2|b|20|null"), people(c), name);
            // and from the other end: the other table's row holds the key
            assertEquals(1L, count(backend, c, "|#TDS\nk, employees\n7, 2\n#"
                    + "->meta::legend::lite::link(c::Firm, ~employees, ~k)"), name);
            assertEquals(List.of("1|a|11|7", "2|b|20|7"), people(c), name);
            // unlinked: the foreign key none
            assertEquals(1L, count(backend, c, "|#TDS\nk, firm\n1, 7\n#"
                    + "->meta::legend::lite::unlink(c::Person, ~firm, ~k)"), name);
            assertEquals(List.of("1|a|11|null", "2|b|20|7"), people(c), name);
            // deleted by key
            assertEquals(1L, count(backend, c, "|#TDS\nk\n2\n#->meta::legend::lite::deleteInstances(c::Person, ~k)"),
                    name);
            assertEquals(List.of("1|a|11|null"), people(c), name);
            // a program's class writes in one transaction: the second's failure takes the first back
            assertThrows(RuntimeException.class, () -> count(backend, c, "{|#TDS\nk, name\n3, c\n#"
                    + "->meta::legend::lite::writeInstances(c::Person, ~k); #TDS\nk, name\n1, d\n#"
                    + "->meta::legend::lite::writeInstances(c::Person, ~k);}"), name);
            assertEquals(List.of("1|a|11|null"), people(c), name);
        }
    }

    private static Object count(TestBackend backend, Connection c, String query) {
        ExecutionResult r = com.legend.Execution.execute(MODEL.formatted(backend.connectionBody()), query, "c::RT", c);
        return ((ExecutionResult.Scalar) java.util.Objects.requireNonNull(r)).value();
    }

    private static void run(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static List<String> people(Connection c) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(
                "SELECT ID, NAME, AGE, FIRM_ID FROM PERSON ORDER BY ID")) {
            while (rs.next()) {
                out.add(rs.getInt(1) + "|" + rs.getString(2) + "|" + (rs.getObject(3) == null ? "null" : rs.getInt(3))
                        + "|" + (rs.getObject(4) == null ? "null" : rs.getInt(4)));
            }
        }
        return out;
    }
}
