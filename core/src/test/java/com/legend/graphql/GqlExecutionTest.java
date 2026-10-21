// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackends;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * GraphQL execution on DuckDB (docs/GRAPHQL_STUDY_2026_10_18.md §10, PR2). The store-backed demo model is
 * {@code graphql-parity/models/store.pure}; the rows are the ones below.
 */
class GqlExecutionTest {

    static final String MODEL = SchemaGeneratorTest.resource("models/store.pure");
    static Connection conn;
    static final QueryService qs = new QueryService();

    @BeforeAll
    static void open() throws SQLException {
        conn = TestBackends.platform().open();
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE TABLE T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100), FOUNDED DATE)");
            s.execute("CREATE TABLE T_ADDRESS (ID INTEGER PRIMARY KEY, CITY VARCHAR(100), COUNTRY VARCHAR(10))");
            s.execute("CREATE TABLE T_PERSON (ID INTEGER PRIMARY KEY, FIRST_NAME VARCHAR(100), LAST_NAME VARCHAR(100),"
                    + " AGE INTEGER, SALARY DOUBLE, FIRM_ID INTEGER, ADDRESS_ID INTEGER)");
            s.execute("INSERT INTO T_FIRM VALUES (1, 'Acme', DATE '1990-01-01'), (2, 'Globex', DATE '2005-06-15'),"
                    + " (3, 'Initech', DATE '2010-03-03')");
            s.execute("INSERT INTO T_ADDRESS VALUES (1, 'London', 'UK'), (2, 'New York', 'US'), (3, 'Tokyo', 'JP')");
            s.execute("INSERT INTO T_PERSON VALUES"
                    + " (1, 'Alice', 'Smith', 45, 120000.0, 1, 1),"
                    + " (2, 'Bob', 'Jones', 32, 90000.0, 1, 2),"
                    + " (3, 'Carol', 'White', 51, 150000.0, 1, 1),"
                    + " (4, 'Dan', 'Brown', 28, 60000.0, 1, NULL),"
                    + " (5, 'Eve', 'Black', 39, 110000.0, 2, 3),"
                    + " (6, 'Frank', 'Green', 61, NULL, 2, 2),"
                    + " (7, 'Grace', 'Hall', 24, 70000.0, NULL, 3)");
        }
    }

    @AfterAll
    static void close() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    static String pure(String query) throws SQLException {
        ExecutionResult r = qs.execute(MODEL, query, "demo::Runtime", conn);
        assertInstanceOf(ExecutionResult.Graph.class, r, r.toString());
        return r.asGraph().json();
    }

    /** The study's first probe (§10 PR2): a sort and a take inside a derived to-many body, fetched in a tree. */
    @Test
    void probeDerivedSortAndTakeInATree() throws SQLException {
        String json = pure("""
                demo::Firm.all()->filter(f|$f.legalName == 'Acme')
                  ->graphFetch(#{ demo::Firm { legalName, top3 { firstName, salary } } }#)
                  ->serialize(#{ demo::Firm { legalName, top3 { firstName, salary } } }#)
                """);
        assertTrue(json.contains("Carol"), json);
        assertTrue(json.contains("Alice"), json);
        assertTrue(json.contains("Bob"), json);
        assertTrue(!json.contains("Dan"), json);
        assertTrue(json.indexOf("Carol") < json.indexOf("Alice") && json.indexOf("Alice") < json.indexOf("Bob"),
                "descending by salary: " + json);
    }

    /** drop and take over a sorted derived body: Bob, Carol (Alice dropped; Dan after the take). */
    @Test
    void derivedSortDropTakeInATree() throws SQLException {
        String json = pure("""
                demo::Firm.all()->filter(f|$f.legalName == 'Acme')
                  ->graphFetch(#{ demo::Firm { legalName, paged { firstName } } }#)
                  ->serialize(#{ demo::Firm { legalName, paged { firstName } } }#)
                """);
        assertTrue(json.contains("Bob") && json.contains("Carol"), json);
        assertTrue(!json.contains("Alice") && !json.contains("Dan"), json);
    }

    /** The shape the derived-child lowering already accepts: a filtered navigation head. */
    @Test
    void derivedFilterInATree() throws SQLException {
        String json = pure("""
                demo::Firm.all()->filter(f|$f.legalName == 'Acme')
                  ->graphFetch(#{ demo::Firm { legalName, seniors { firstName } } }#)
                  ->serialize(#{ demo::Firm { legalName, seniors { firstName } } }#)
                """);
        assertTrue(json.contains("Carol"), json);
        assertTrue(json.contains("Alice"), json);
        assertTrue(!json.contains("Bob"), json);
    }

    /** A GraphQL document run as the island {@code #GQL{...}#}: the query text is the island itself. */
    static String gql(String document) throws SQLException {
        return pure("#GQL{ " + document + " }#");
    }

    static void inOrder(String json, String... names) {
        int at = -1;
        for (String n : names) {
            int i = json.indexOf(n);
            assertTrue(i > at, n + " after the previous in " + json);
            at = i;
        }
    }

    @Test
    void rootWhereAndOrderBy() throws SQLException {
        String json = gql("{ Person(where: { age: { gt: 40 } }, orderBy: [{ lastName: ASC }]) { firstName lastName age } }");
        inOrder(json, "Frank", "Alice", "Carol");
        assertTrue(!json.contains("Bob"), json);
        assertTrue(json.contains("\"age\":61") || json.contains("\"age\": 61"), json);
    }

    @Test
    void rootLimitAndOffset() throws SQLException {
        String json = gql("{ Person(orderBy: [{ age: ASC }], limit: 2, offset: 1) { firstName } }");
        inOrder(json, "Dan", "Bob");
        assertTrue(!json.contains("Grace") && !json.contains("Eve"), json);
    }

    @Test
    void twoSortKeysThePrimaryFirst() throws SQLException {
        // by lastName's first letter group? no: by age descending within the same firm, firms ascending
        String json = gql("{ Person(where: { firm: { isNot: null } }, orderBy: [{ firm: { legalName: ASC } }, { age: DESC }]) { firstName } }");
        inOrder(json, "Carol", "Alice", "Bob", "Dan", "Frank", "Eve");
    }

    @Test
    void toOneAndToManyNodesWithAliases() throws SQLException {
        String json = gql("{ firms: Firm(where: { legalName: { equals: \"Acme\" } }) { name: legalName employees { firstName address { city } } } }");
        assertTrue(json.contains("\"name\":\"Acme\"") || json.contains("\"name\": \"Acme\""), json);
        assertTrue(json.contains("London") && json.contains("New York"), json);
        assertTrue(json.contains("Dan"), json);
        assertTrue(!json.contains("Eve"), json);
    }

    @Test
    void someEveryNone() throws SQLException {
        String some = gql("{ Firm(where: { employees: { some: { age: { gt: 50 } } } }, orderBy: [{ legalName: ASC }]) { legalName } }");
        inOrder(some, "Acme", "Globex");
        assertTrue(!some.contains("Initech"), some);
        String none = gql("{ Firm(where: { employees: { none: { age: { gt: 50 } } } }, orderBy: [{ legalName: ASC }]) { legalName } }");
        assertTrue(none.contains("Initech") && !none.contains("Acme") && !none.contains("Globex"), none);
        String every = gql("{ Firm(where: { employees: { every: { age: { gte: 39 } } } }, orderBy: [{ legalName: ASC }]) { legalName } }");
        assertTrue(every.contains("Globex") && every.contains("Initech") && !every.contains("Acme"), every);
    }

    @Test
    void stringOperationsAndInsensitiveMode() throws SQLException {
        String json = gql("{ Person(where: { firstName: { startsWith: \"a\", mode: insensitive } }) { firstName } }");
        assertTrue(json.contains("Alice") && !json.contains("Bob"), json);
        String contains = gql("{ Person(where: { lastName: { contains: \"o\" } }, orderBy: [{ lastName: ASC }]) { lastName } }");
        inOrder(contains, "Brown", "Jones");
        assertTrue(!contains.contains("Smith"), contains);
        String in = gql("{ Person(where: { firstName: { in: [\"Bob\", \"Eve\"] } }, orderBy: [{ firstName: ASC }]) { firstName } }");
        inOrder(in, "Bob", "Eve");
        assertTrue(!in.contains("Alice"), in);
        String notIn = gql("{ Person(where: { firstName: { notIn: [\"Bob\", \"Eve\", \"Alice\", \"Carol\", \"Dan\"] } }, orderBy: [{ firstName: ASC }]) { firstName } }");
        inOrder(notIn, "Frank", "Grace");
    }

    @Test
    void nullTestsAndNot() throws SQLException {
        String isNull = gql("{ Person(where: { salary: { equals: null } }) { firstName } }");
        assertTrue(isNull.contains("Frank") && !isNull.contains("Alice"), isNull);
        String notNull = gql("{ Person(where: { salary: { not: null }, age: { gt: 50 } }) { firstName } }");
        assertTrue(notNull.contains("Carol") && !notNull.contains("Frank"), notNull);
        String not = gql("{ Person(where: { NOT: { age: { lt: 60 } } }) { firstName } }");
        assertTrue(not.contains("Frank") && !not.contains("Alice"), not);
        String or = gql("{ Person(where: { OR: [{ firstName: { equals: \"Bob\" } }, { firstName: { equals: \"Eve\" } }] }, orderBy: [{ firstName: ASC }]) { firstName } }");
        inOrder(or, "Bob", "Eve");
        assertTrue(!or.contains("Alice"), or);
    }

    @Test
    void enumAndDateComparisons() throws SQLException {
        String uk = gql("{ Address(where: { country: { equals: UK } }) { city country } }");
        assertTrue(uk.contains("London") && !uk.contains("Tokyo"), uk);
        String founded = gql("{ Firm(where: { founded: { gt: \"2000-01-01\" } }, orderBy: [{ founded: DESC }]) { legalName founded } }");
        inOrder(founded, "Initech", "Globex");
        assertTrue(!founded.contains("Acme"), founded);
    }

    @Test
    void derivedPropertyLeaf() throws SQLException {
        String json = gql("{ Person(where: { fullName: { equals: \"Alice Smith\" } }) { fullName } }");
        assertTrue(json.contains("Alice Smith") && !json.contains("Bob"), json);
    }

    @Test
    void fragmentsAndDirectives() throws SQLException {
        String json = gql("query Q { Person(where: { firstName: { equals: \"Bob\" } }) { ...P age @include(if: false) lastName @skip(if: false) } }"
                + " fragment P on Person { firstName }");
        assertTrue(json.contains("Bob") && json.contains("Jones") && !json.contains("age"), json);
    }

    /** The island's variables are the enclosing Pure scope's: a function's parameters. */
    @Test
    void variablesFromTheEnclosingScope() throws SQLException {
        ExecutionResult r = qs.execute(MODEL + """
                ###Pure
                function demo::smiths(n: Integer[1], who: String[1]): String[1]
                {
                  #GQL{ query($n: Int, $who: String) { Person(where: { lastName: { equals: $who } }, orderBy: [{ firstName: ASC }], limit: $n) { firstName } } }#
                }
                """, "demo::smiths(1, 'Smith')", "demo::Runtime", conn);
        assertInstanceOf(ExecutionResult.Graph.class, r, r.toString());
        String json = r.asGraph().json();
        assertTrue(json.contains("Alice") && !json.contains("Bob"), json);
    }

    /** A clause on a nested collection: the composed body on the tree node (§6.2). */
    @Test
    void nestedWhereOrderByLimitOffset() throws SQLException {
        String json = gql("{ Firm(orderBy: [{ legalName: ASC }]) { legalName"
                + " employees(where: { age: { gt: 30 } }, orderBy: [{ salary: DESC }], limit: 2) { firstName salary } } }");
        // Acme: Carol 150k, Alice 120k (Bob 90k cut by the limit; Dan 28 by the filter); Globex: Eve and Frank
        // (Frank's null salary sits where the database puts a null under DESC: Pure's sortBy stamps no placement)
        inOrder(json, "Acme", "Carol", "Alice", "Globex", "Initech");
        assertTrue(json.contains("Eve") && json.contains("Frank"), json);
        assertTrue(!json.contains("Bob") && !json.contains("Dan"), json);
        String paged = gql("{ Firm(where: { legalName: { equals: \"Acme\" } }) { legalName"
                + " page: employees(orderBy: [{ firstName: ASC }], offset: 1, limit: 2) { firstName } } }");
        inOrder(paged, "\"page\"", "Bob", "Carol");
        assertTrue(!paged.contains("Alice") && !paged.contains("Dan"), paged);
    }

    /** Two keys on a nested collection, and a nested clause under a nested node. */
    @Test
    void nestedTwoKeysAndDeeperNesting() throws SQLException {
        String json = gql("{ Firm(where: { legalName: { equals: \"Acme\" } }) { legalName"
                + " employees(orderBy: [{ lastName: DESC }, { age: ASC }]) { firstName address { city } } } }");
        inOrder(json, "Carol", "Alice", "Bob", "Dan");
        String deep = gql("{ Address(where: { city: { equals: \"London\" } }) { city"
                + " residents(where: { age: { gt: 50 } }) { firstName firm { legalName } } } }");
        assertTrue(deep.contains("Carol") && !deep.contains("Alice"), deep);
        assertTrue(deep.contains("Acme"), deep);
    }

    /** The same query as Pure text: the composed node is ordinary Pure in a tree position, parsed back. */
    @Test
    void theComposedNodeSpelledInPure() throws SQLException {
        String json = pure("""
                demo::Firm.all()->filter(f|$f.legalName == 'Acme')
                  ->graphFetch(#{ demo::Firm { legalName, 'top2': $this.employees->sortByReversed(e|$e.salary)->limit(2) { firstName } } }#)
                  ->serialize(#{ demo::Firm { legalName, 'top2': $this.employees->sortByReversed(e|$e.salary)->limit(2) { firstName } } }#)
                """);
        inOrder(json, "\"top2\"", "Carol", "Alice");
        assertTrue(!json.contains("Bob"), json);
    }

    /** The RELATION NODE (register S118) as Pure text: a node whose body is a table over $this; its tree names the
     *  table's columns. One row per firm here (a whole-collection aggregate has exactly one), so one object. */
    @Test
    void theRelationNodeSpelledInPure() throws SQLException {
        String tree = "#{ demo::Firm { legalName, 'payroll': $this.employees->project(~[s: e|$e.salary])"
                + "->aggregate(~[n: x|1 : y|$y->count(), total: x|$x.s : y|$y->sum()]) { n, total } } }#";
        String json = pure("demo::Firm.all()->filter(f|$f.legalName == 'Acme')->graphFetch(" + tree + ")->serialize("
                + tree + ")");
        assertTrue(json.replace(" ", "").contains("\"payroll\":{\"n\":4,\"total\":420000.0}"), json);
    }

    /** Groups as a relation node: a key, a count, a sort; an object of the row's columns under its own key; a firm
     *  with no employees answers no groups. */
    @Test
    void theRelationNodeGroupsPerParent() throws SQLException {
        String tree = "#{ demo::Firm { legalName, 'byAge': $this.employees->project(~[old: e|$e.age > 40, s: e|$e.salary])"
                + "->groupBy(~[old], ~[n: x|1 : y|$y->count(), top: x|$x.s : y|$y->max()])->sort([~old->ascending()])"
                + " { key { old }, n, 'best': top } } }#";
        String json = pure("demo::Firm.all()->sortBy(f|$f.legalName)->graphFetch(" + tree + ")->serialize(" + tree
                + ")").replace(" ", "");
        assertTrue(json.contains("\"legalName\":\"Acme\",\"byAge\":[{\"key\":{\"old\":false},\"n\":2,"
                + "\"best\":90000.0},{\"key\":{\"old\":true},\"n\":2,\"best\":150000.0}]"), json);
        assertTrue(json.contains("\"legalName\":\"Initech\",\"byAge\":[]"), json);
    }

    /** A SHAPE CLASS the modeler writes, constructed in a node's body: the constructor fuses with the reads of its
     *  members (a reducer over the navigation per field, a derived member over the fields); the instance is never
     *  built. The fusion's gate was the companion's generated classes until 2026-10-21; it is any constructed class. */
    @Test
    void aConstructedShapeClassFusesWithItsMembers() throws SQLException {
        String model = MODEL.replace("  seniors() {", "  payroll() { ^demo::Payroll(headcount ="
                + " $this.employees->size(), total = $this.employees.salary->sum()) }: demo::Payroll[1];\n  seniors() {")
                + "\n###Pure\nClass demo::Payroll { headcount: Integer[1]; total: Float[1];"
                + " average() { $this.total / $this.headcount }: Float[1]; }\n";
        String tree = "#{ demo::Firm { legalName, payroll { headcount, total, average } } }#";
        ExecutionResult r = qs.execute(model, "demo::Firm.all()->filter(f|$f.legalName == 'Acme')->graphFetch(" + tree
                + ")->serialize(" + tree + ")", "demo::Runtime", conn);
        assertInstanceOf(ExecutionResult.Graph.class, r, r.toString());
        String json = r.asGraph().json().replace(" ", "");
        assertTrue(json.contains("\"payroll\":{\"headcount\":4,\"total\":420000.0,\"average\":105000.0}"), json);
    }

    /** A relation node's tree names columns, nothing else: an unknown column and a call are refused by name. */
    @Test
    void aRelationNodesTreeNamesItsColumns() {
        String body = "$this.employees->project(~[s: e|$e.salary])->aggregate(~[n: x|1 : y|$y->count()])";
        for (String[] c : new String[][] {{"{ nope }", "has no column 'nope'"},
                {"{ n(1) }", "never a call"}}) {
            String tree = "#{ demo::Firm { legalName, 'p': " + body + " " + c[0] + " } }#";
            RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                    () -> pure("demo::Firm.all()->graphFetch(" + tree + ")->serialize(" + tree + ")"));
            assertTrue(String.valueOf(e.getMessage()).contains(c[1]), c[0] + ": " + e.getMessage());
        }
    }

    /** Nested groups through GraphQL: each firm's employees grouped by a to-one path, sorted (the absent country
     *  last: S93), averaged; a firm with none answers []. */
    @Test
    void groupsUnderAParentThroughGraphql() throws SQLException {
        String json = gql("{ Firm(orderBy: [{ legalName: ASC }]) { legalName employeesGroups(orderBy: [{ key: {"
                + " address: { country: ASC } } }]) { key { address { country } } count avg { salary } } } }")
                .replace(" ", "");
        assertTrue(json.contains("\"legalName\":\"Acme\",\"employeesGroups\":["
                + "{\"key\":{\"address\":{\"country\":\"UK\"}},\"count\":2,\"avg\":{\"salary\":135000.0}},"
                + "{\"key\":{\"address\":{\"country\":\"US\"}},\"count\":1,\"avg\":{\"salary\":90000.0}},"
                + "{\"key\":{\"address\":{\"country\":null}},\"count\":1,\"avg\":{\"salary\":60000.0}}]"), json);
        assertTrue(json.contains("\"legalName\":\"Initech\",\"employeesGroups\":[]"), json);
    }

    /** An enum list in `in` and `notIn` (the trading probe of 2026-10-19 found `in: [A, B]` refused as one value). */
    @Test
    void anEnumListInAndNotIn() throws SQLException {
        String in = gql("{ Person(where: { address: { country: { in: [UK, JP] } } }, orderBy: [{ firstName: ASC }]) { firstName } }");
        inOrder(in, "Alice", "Carol", "Eve", "Grace");
        assertTrue(!in.contains("Bob") && !in.contains("Frank"), in);
        String notIn = gql("{ Person(where: { address: { country: { notIn: [UK, JP] } } }, orderBy: [{ firstName: ASC }]) { firstName } }");
        inOrder(notIn, "Bob", "Frank");
        assertTrue(!notIn.contains("Alice"), notIn);
    }

    /** A qualifier calling a qualifier, and one with an argument (the trading probe of 2026-10-19 found
     *  `notionalAt(rate:)`, whose body calls `notional()`, refused as not inlinable). */
    @Test
    void aQualifierCallingAQualifierAndOneWithAnArgument() throws SQLException {
        String json = gql("{ Person(where: { firstName: { equals: \"Alice\" } }) { shouted salaryAt(rate: 0.5) } }");
        assertTrue(json.contains("ALICE SMITH"), json);
        assertTrue(json.contains("60000"), json);
    }
}
