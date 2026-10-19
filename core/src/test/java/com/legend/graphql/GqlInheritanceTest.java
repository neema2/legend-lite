// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.Property;
import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackends;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Inheritance (docs/GRAPHQL_STUDY_2026_10_18.md G8 to G10, §5.8; PR3): the companion's {@code __typename()} and
 * {@code as<Sub>()} members, subtype fragments as the tree's subtype entries, {@code asSub} in {@code where} and
 * {@code orderBy}, over a union of a class's and its subclasses' sets (the SQL twin: a table per class).
 * Rows: Alice (Person, 45), Bob (Employee, 32, 90k), Carol (Manager, 51, 150k, budget 1M), Dan (Employee, 28,
 * 60k), all of Acme but Dan (Globex).
 */
class GqlInheritanceTest {

    static final String MODEL = SchemaGeneratorTest.resource("models/inheritance.pure");
    static Connection conn;
    static final QueryService qs = new QueryService();

    @BeforeAll
    static void open() throws SQLException {
        conn = TestBackends.platform().open();
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100))");
            s.execute("CREATE TABLE T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100),"
                    + " AGE INTEGER, FIRM_ID INTEGER)");
            s.execute("CREATE TABLE T_EMPLOYEE (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100),"
                    + " AGE INTEGER, SALARY DOUBLE, FIRM_ID INTEGER)");
            s.execute("CREATE TABLE T_MANAGER (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100),"
                    + " AGE INTEGER, SALARY DOUBLE, BUDGET DOUBLE, FIRM_ID INTEGER)");
            s.execute("INSERT INTO T_FIRM VALUES (1, 'Acme'), (2, 'Globex')");
            s.execute("INSERT INTO T_PERSON VALUES (1, 'Alice', 'Smith', 45, 1)");
            s.execute("INSERT INTO T_EMPLOYEE VALUES (2, 'Bob', 'Jones', 32, 90000.0, 1), (4, 'Dan', 'Brown', 28, 60000.0, 2)");
            s.execute("INSERT INTO T_MANAGER VALUES (3, 'Carol', 'White', 51, 150000.0, 1000000.0, 1)");
        }
    }

    @AfterAll
    static void close() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    static String gql(String document) throws SQLException {
        ExecutionResult r = qs.execute(MODEL, "#GQL{ " + document + " }#", "demo::Runtime", conn);
        assertInstanceOf(ExecutionResult.Graph.class, r, r.toString());
        return r.asGraph().json();
    }

    static String pure(String query) throws SQLException {
        ExecutionResult r = qs.execute(MODEL, query, "demo::Runtime", conn);
        assertInstanceOf(ExecutionResult.Graph.class, r, r.toString());
        return r.asGraph().json();
    }

    @Test
    void theCompanionsMembersAreOnTheClass() {
        ModelContext ctx = Compiler.compileModel(MODEL);
        assertTrue(ctx.findProperty("demo::Person", "__typename").orElseThrow() instanceof Property.Derived);
        assertTrue(ctx.findProperty("demo::Person", "asEmployee").orElseThrow() instanceof Property.Derived);
        assertTrue(ctx.findProperty("demo::Person", "asManager").isPresent());
        assertTrue(ctx.findProperty("demo::Employee", "asManager").isPresent());
        assertTrue(ctx.findProperty("demo::Employee", "asEmployee").isEmpty(), "a class is not its own downcast");
        assertTrue(ctx.findProperty("demo::Firm", "__typename").isPresent());
        // the schema lists none of them as fields
        String sdl = com.legend.compiler.graphql.SdlPrinter.print(com.legend.compiler.graphql.GqlIslands.schemaOf(ctx));
        assertTrue(!sdl.contains("asEmployee: ") && !sdl.contains("__typename"), sdl);
        assertTrue(sdl.contains("asEmployee: EmployeeWhere"), sdl);
    }

    @Test
    void typenameInPure() throws SQLException {
        String json = pure("demo::Person.all()->sortBy(p|$p.firstName)"
                + "->graphFetch(#{ demo::Person { firstName, 'kind': __typename } }#)"
                + "->serialize(#{ demo::Person { firstName, 'kind': __typename } }#)");
        assertTrue(json.contains("\"kind\":\"Person\"") && json.contains("\"kind\":\"Employee\"")
                && json.contains("\"kind\":\"Manager\""), json);
    }

    @Test
    void typenameAndSubtypeFragments() throws SQLException {
        String json = gql("{ Person(orderBy: [{ firstName: ASC }]) { __typename firstName"
                + " ... on EmployeeInterface { salary } ... on Manager { budget } } }");
        assertTrue(json.contains("\"__typename\":\"Person\""), json);
        assertTrue(json.contains("\"__typename\":\"Manager\""), json);
        assertTrue(json.contains("\"budget\":1000000.0") || json.contains("\"budget\":1000000"), json);
        assertTrue(json.contains("\"salary\":90000.0") || json.contains("\"salary\":90000"), json);
        assertTrue(json.indexOf("Alice") < json.indexOf("Bob") && json.indexOf("Bob") < json.indexOf("Carol"), json);
    }

    @Test
    void asSubtypeInWhere() throws SQLException {
        String members = gql("{ Person(where: { asEmployee: { isNot: null } }, orderBy: [{ firstName: ASC }]) { firstName } }");
        assertTrue(!members.contains("Alice") && members.contains("Bob") && members.contains("Carol")
                && members.contains("Dan"), members);
        String rich = gql("{ Person(where: { asEmployee: { salary: { gt: 80000 } } }, orderBy: [{ firstName: ASC }]) { firstName } }");
        assertTrue(!rich.contains("Alice") && rich.contains("Bob") && rich.contains("Carol") && !rich.contains("Dan"), rich);
        String not = gql("{ Person(where: { asEmployee: { is: null } }) { firstName } }");
        assertTrue(not.contains("Alice") && !not.contains("Bob"), not);
    }

    @Test
    void asSubtypeInOrderBy() throws SQLException {
        String json = gql("{ Person(orderBy: [{ asEmployee: { salary: DESC } }, { firstName: ASC }]) { firstName } }");
        // Carol 150k, Bob 90k, Dan 60k; Alice (no salary) where the database puts a null under DESC
        assertTrue(json.indexOf("Carol") < json.indexOf("Bob") && json.indexOf("Bob") < json.indexOf("Dan"), json);
    }

    @Test
    void theExactTypeTrapIsRefused() {
        java.util.Map<String, String> walls = Compiler.compileAllBodies(Compiler.compileModel(MODEL + """
                ###Pure
                function demo::q(): String[1]
                {
                  #GQL{ { Person { ... on Person { firstName } } } }#
                }
                """));
        String wall = walls.values().stream().findFirst().orElse("");
        assertTrue(wall.contains("PersonInterface"), wall);
    }

    @Test
    void aClassNamedLikeACompanionMemberIsRefused() {
        try {
            Compiler.compileModel("Class x::A { __typename: String[1]; }");
            assertEquals("refused", "compiled");
        } catch (RuntimeException e) {
            assertTrue(String.valueOf(e.getMessage()).contains("companion"), e.getMessage());
        }
    }
}
