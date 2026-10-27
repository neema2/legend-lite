// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.error.LegendCompileException;
import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code #Cypher{...}#} end to end on DuckDB (docs/CYPHER_STUDY_2026_10_15.md §5, docs/CYPHER_PLAN_2026_10_15.md
 * PR1): a Cypher query over a class model with an association and a class-typed property returns the rows Neo4j
 * returns for the same graph (the rows here are derived by hand from Cypher's semantics; the Neo4j-judged harness is
 * the plan's next landing), the island composes with Pure, and a refusal is the compile error its kind says, placed
 * in the source.
 */
class CypherIslandIntegrationTest {

    /** The platform's own engine: the island's session, its tables and the connection its model declares. */
    private static final TestBackend PLATFORM = TestBackends.platform();

    private static final String MODEL = """
            ###Pure
            Class test::Person { name: String[1]; age: Integer[0..1]; active: Boolean[0..1]; }
            Class test::Firm { legalName: String[1]; founded: Integer[0..1]; hq: test::City[0..1]; }
            Class test::City { name: String[1]; }
            Association test::PersonFirm { firm: test::Firm[0..1]; employees: test::Person[*]; }
            ###Relational
            Database store::DB
            (
                Table T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER, ACTIVE BIT, FIRM_ID INTEGER)
                Table T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100), FOUNDED INTEGER, CITY_ID INTEGER)
                Table T_CITY (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))
                Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)
                Join FirmCity(T_FIRM.CITY_ID = T_CITY.ID)
            )
            ###Mapping
            Mapping test::M
            (
                test::Person: Relational
                {
                    ~mainTable [store::DB] T_PERSON
                    name: [store::DB] T_PERSON.NAME,
                    age: [store::DB] T_PERSON.AGE,
                    active: [store::DB] T_PERSON.ACTIVE
                }
                test::Firm: Relational
                {
                    ~mainTable [store::DB] T_FIRM
                    legalName: [store::DB] T_FIRM.LEGAL_NAME,
                    founded: [store::DB] T_FIRM.FOUNDED,
                    hq: [store::DB] @FirmCity
                }
                test::City: Relational
                {
                    ~mainTable [store::DB] T_CITY
                    name: [store::DB] T_CITY.NAME
                }
                test::PersonFirm: Relational { AssociationMapping (
                    employees: [store::DB]@PersonFirm,
                    firm: [store::DB]@PersonFirm
                ) }
            )
            ###Connection
            RelationalDatabaseConnection store::Conn { %s }
            ###Runtime
            Runtime test::RT { mappings: [ test::M ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
            """.formatted(PLATFORM.connectionBody());

    private static Connection session;

    @BeforeAll
    static void seed() throws SQLException {
        session = PLATFORM.open();
        try (Statement s = session.createStatement()) {
            s.execute("CREATE TABLE T_PERSON (ID INTEGER PRIMARY KEY, NAME VARCHAR(100), AGE INTEGER, ACTIVE BOOLEAN,"
                    + " FIRM_ID INTEGER)");
            s.execute("CREATE TABLE T_FIRM (ID INTEGER PRIMARY KEY, LEGAL_NAME VARCHAR(100), FOUNDED INTEGER,"
                    + " CITY_ID INTEGER)");
            s.execute("CREATE TABLE T_CITY (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))");
            s.execute("INSERT INTO T_CITY VALUES (1, 'NYC'), (2, 'LA')");
            s.execute("INSERT INTO T_FIRM VALUES (1, 'Acme', 1990, 1), (2, 'Globex', NULL, NULL)");
            s.execute("INSERT INTO T_PERSON VALUES (1, 'Alice', 30, true, 1), (2, 'Bob', 40, NULL, 1),"
                    + " (3, 'Charlie', 25, false, 2), (4, 'Diana', NULL, true, NULL)");
        }
    }

    @AfterAll
    static void close() throws SQLException {
        session.close();
    }

    private static ExecutionResult run(String cypher) throws SQLException {
        return new QueryService().execute(MODEL, "#Cypher{" + cypher + "}#", "test::RT", session);
    }

    /** The model with a graph (the study's §12.45): a DataSpace of Person and Firm, open. */
    private static final String GRAPH_MODEL = MODEL + """
            ###Pure
            Profile graph::graph
            {
               stereotypes: [Anonymous, Edge, Open, Native, from, to, type, nanoseconds, timeOfDay,
                   listOfDate, listOfLocalTime, listOfZonedTime, listOfLocalDateTime, listOfZonedDateTime, listOfDuration];
            }
            ###DataSpace
            DataSpace <<graph::graph.Open>> test::G
            {
              executionContexts:
              [
                {
                  name: 'default';
                  mapping: test::M;
                  defaultRuntime: test::RT;
                }
              ];
              defaultExecutionContext: 'default';
              elements: [ test::Person, test::Firm ];
            }
            """;

    private static ExecutionResult runOver(String model, String cypher) throws SQLException {
        return new QueryService().execute(model, "#Cypher{" + cypher + "}#", "test::RT", session);
    }

    private static ExecutionResult pure(String model, String query) throws SQLException {
        return new QueryService().execute(model, query, "test::RT", session);
    }

    @Test
    void theGraphAUseNamesIsADataSpaceOpenByItsStereotype() throws SQLException {
        // the study's §12.45: USE names the graph, a DataSpace; its elements are the labels (City is outside), its
        // <<graph::graph.Open>> the open reading (a label no class has: no rows; a property no class declares: null)
        assertEquals(List.of(), rows(runOver(GRAPH_MODEL, "USE test::G MATCH (p:Nobody) RETURN p.name AS n")));
        List<String> people = rows(runOver(GRAPH_MODEL, "USE test::G MATCH (p:Person) RETURN p.nam AS x"));
        assertEquals(rows(run("MATCH (p:Person) RETURN p.name AS n")).size(), people.size());
        assertTrue(people.stream().allMatch("null"::equals), people.toString());
        long persons = Long.parseLong(rows(run("MATCH (p:Person) RETURN count(*) AS c")).get(0));
        long firms = Long.parseLong(rows(run("MATCH (f:Firm) RETURN count(*) AS c")).get(0));
        assertEquals(List.of(String.valueOf(persons + firms)),
                rows(runOver(GRAPH_MODEL, "USE test::G MATCH (n) RETURN count(*) AS c")));
        // without a USE the whole model is the graph, closed: C4 stays (aQueryNeo4jRejectsIsATypeError)
        LegendCompileException closed = assertThrows(LegendCompileException.class,
                () -> runOver(GRAPH_MODEL, "MATCH (p:Nobody) RETURN p.name"));
        assertTrue(closed.getMessage().contains("the label Nobody names no class"), closed.getMessage());
        // a USE naming no DataSpace, and a USE that is not the first clause, are errors at their place
        LegendCompileException none = assertThrows(LegendCompileException.class,
                () -> runOver(GRAPH_MODEL, "USE test::Nope MATCH (p:Person) RETURN p.name"));
        assertTrue(none.getMessage().contains("USE test::Nope: no DataSpace of the model"), none.getMessage());
        LegendCompileException late = assertThrows(LegendCompileException.class,
                () -> runOver(GRAPH_MODEL, "MATCH (p:Person) USE test::G RETURN p.name"));
        assertTrue(late.getMessage().contains("USE clause must be"), late.getMessage());
    }

    @Test
    void theIdentityOfAMappedInstanceIsItsKey() throws SQLException {
        // docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md W2: a mapped instance's identity is its class's key (the
        // mapping's ~primaryKey, else the main table's PRIMARY KEY) as '<class>:<key>', one native, elementId
        assertEquals(List.of("test::Person:1|test::Firm:1", "test::Person:2|test::Firm:1", "test::Person:3|test::Firm:2",
                        "test::Person:4|null"),
                rows(pure(MODEL, "test::Person.all()->project(~[id:p|$p->meta::legend::lite::elementId(),"
                        + " f:p|$p.firm->meta::legend::lite::elementId()])->sort(~id->ascending())")));
        // several instances (a to-many navigation): each its own identity
        assertEquals(List.of("Acme|test::Person:1", "Acme|test::Person:2", "Globex|test::Person:3"),
                rows(pure(MODEL, "test::Firm.all()->project(~[n:f|$f.legalName,"
                        + " ids:f|$f.employees->meta::legend::lite::elementId()])->sort(~ids->ascending())")));
        // the mapping's ~primaryKey takes precedence over the table's PRIMARY KEY (the engine's rule)
        String keyedByName = MODEL.replace("~mainTable [store::DB] T_PERSON\n",
                "~primaryKey ([store::DB] T_PERSON.NAME)\n                    ~mainTable [store::DB] T_PERSON\n");
        assertEquals(List.of("test::Person:Alice", "test::Person:Bob", "test::Person:Charlie", "test::Person:Diana"),
                rows(pure(keyedByName, "test::Person.all()->project(~[id:p|$p->meta::legend::lite::elementId()])"
                        + "->sort(~id->ascending())")));
        // Cypher compares nodes by it: a cartesian product joined on identity, an absent node without one
        assertEquals(List.of("4"), rows(run("MATCH (a:Person), (b:Person) WHERE a = b RETURN count(*) AS c")));
        assertEquals(List.of("12"), rows(run("MATCH (a:Person), (b:Person) WHERE a <> b RETURN count(*) AS c")));
        assertEquals(List.of("Alice|true", "Bob|true", "Charlie|true", "Diana|null"),
                rows(run("MATCH (p:Person) OPTIONAL MATCH (p)-[:firm]->(f:Firm) RETURN p.name AS n, f = f AS same"
                        + " ORDER BY n")));
        assertEquals(List.of("Alice|Bob", "Bob|Alice"), rows(run("MATCH (a:Person)-[:firm]->(f:Firm),"
                + " (b:Person)-[:firm]->(g:Firm) WHERE f = g AND a <> b RETURN a.name AS an, b.name AS bn"
                + " ORDER BY an")));
        // (2026-10-18, the writes judge's read-back, the study's §12.94) elementId(x) returns it: a node's, an
        // association relationship's ordered pair of its ends' (S74), null for a node an OPTIONAL MATCH never found
        assertEquals(List.of("test::Person:1|firm:test::Person:1>test::Firm:1|test::Firm:1",
                        "test::Person:3|firm:test::Person:3>test::Firm:2|test::Firm:2"),
                rows(run("MATCH (p:Person)-[r:firm]->(f:Firm) WHERE p.name IN ['Alice', 'Charlie'] RETURN"
                        + " elementId(p) AS p, elementId(r) AS r, elementId(f) AS f ORDER BY p")));
        assertEquals(List.of("Diana|null"), rows(run("MATCH (p:Person {name: 'Diana'}) OPTIONAL MATCH"
                + " (p)-[:firm]->(f:Firm) RETURN p.name AS n, elementId(f) AS f")));
        // a class with neither key has no identity: a compile error naming it
        String keyless = MODEL.replace("Table T_CITY (ID INTEGER PRIMARY KEY, NAME VARCHAR(100))",
                "Table T_CITY (ID INTEGER, NAME VARCHAR(100))");
        LegendCompileException none = assertThrows(LegendCompileException.class,
                () -> runOver(keyless, "MATCH (a:City), (b:City) WHERE a = b RETURN a.name AS n"));
        assertTrue(none.getMessage().contains("the class test::City has no identity under the mapping test::M"),
                none.getMessage());
    }

    private static List<String> columns(ExecutionResult result) {
        return result.columns().stream().map(c -> c.name()).toList();
    }

    private static List<String> rows(ExecutionResult result) {
        List<String> rows = new ArrayList<>();
        for (var row : result.rows()) {
            List<String> cells = new ArrayList<>();
            for (Object value : row.values()) {
                cells.add(value == null ? "null" : value instanceof Number n && !(value instanceof Double)
                        ? String.valueOf(n.longValue()) : String.valueOf(value));
            }
            rows.add(String.join("|", cells));
        }
        return rows;
    }

    private static List<String> expect(String... rows) {
        return Arrays.asList(rows);
    }

    @Test
    void aMatchReturnsEachNodesProperty() throws SQLException {
        ExecutionResult r = run("MATCH (p:Person) RETURN p.name ORDER BY p.name");
        assertEquals(List.of("p.name"), columns(r));
        assertEquals(expect("Alice", "Bob", "Charlie", "Diana"), rows(r));
    }

    @Test
    void whereKeepsTheRowsWhereTheConditionIsTrueNullIsNotTrue() throws SQLException {
        assertEquals(expect("Alice", "Bob"), rows(run("MATCH (p:Person) WHERE p.age > 26 RETURN p.name AS name"
                + " ORDER BY name")));
        // NOT null is null: Bob (age known, active unknown) is not kept by NOT p.active
        assertEquals(expect("Charlie", "Diana"), rows(run("MATCH (p:Person) WHERE NOT p.active OR p.age IS NULL"
                + " RETURN p.name ORDER BY p.name")));
        assertEquals(expect("Alice", "Diana"), rows(run("MATCH (p:Person) WHERE p.active RETURN p.name"
                + " ORDER BY p.name")));
    }

    @Test
    void aRelationshipIsAnAssociationEndInEitherDirection() throws SQLException {
        ExecutionResult r = run("MATCH (p:Person)-[:firm]->(f:Firm) RETURN p.name, f.legalName ORDER BY p.name");
        assertEquals(List.of("p.name", "f.legalName"), columns(r));
        assertEquals(expect("Alice|Acme", "Bob|Acme", "Charlie|Globex"), rows(r));
        assertEquals(expect("Alice", "Bob"), rows(run("MATCH (f:Firm)-[:employees]->(p:Person)"
                + " WHERE f.legalName = 'Acme' RETURN p.name ORDER BY p.name")));
        assertEquals(expect("Acme", "Acme", "Globex"), rows(run("MATCH (f:Firm)<-[:firm]-(p:Person)"
                + " RETURN f.legalName ORDER BY f.legalName")));
    }

    @Test
    void aChainOfHopsNavigatesAClassTypedProperty() throws SQLException {
        assertEquals(expect("Alice|NYC", "Bob|NYC"), rows(run("MATCH (p:Person)-[:firm]->(f:Firm)-[:hq]->(c:City)"
                + " RETURN p.name, c.name ORDER BY p.name")));
        assertEquals(expect("Acme"), rows(run("MATCH (p:Person {name: 'Bob'})-[:firm]->(f) RETURN f.legalName")));
    }

    @Test
    void aggregatesGroupImplicitlyAndASumOfNothingIsZero() throws SQLException {
        ExecutionResult r = run("MATCH (f:Firm)<-[:firm]-(p:Person) RETURN f.legalName, count(*) AS n ORDER BY n DESC");
        assertEquals(List.of("f.legalName", "n"), columns(r));
        assertEquals(expect("Acme|2", "Globex|1"), rows(r));
        ExecutionResult all = run("MATCH (p:Person) RETURN count(*), sum(p.age), avg(p.age), min(p.age), max(p.age)");
        assertEquals(List.of("count(*)", "sum(p.age)", "avg(p.age)", "min(p.age)", "max(p.age)"), columns(all));
        assertEquals(expect("4|95|31.666666666666668|25|40"), rows(all));
        assertEquals(expect("0|0|null"), rows(run("MATCH (p:Person) WHERE p.age > 100 RETURN count(p), sum(p.age),"
                + " avg(p.age)")));
    }

    @Test
    void withProjectsAndFiltersAndReturnReadsItsColumns() throws SQLException {
        assertEquals(expect("Bob|80", "Alice|60"), rows(run("MATCH (p:Person) WITH p.name AS name, p.age AS age"
                + " WHERE age IS NOT NULL RETURN name, age * 2 AS twice ORDER BY twice DESC LIMIT 2")));
        assertEquals(expect("Alice", "Bob"), rows(run("MATCH (p:Person) WITH p WHERE p.age > 26 RETURN p.name"
                + " ORDER BY p.name")));
    }

    @Test
    void caseCoalesceDistinctAndSlicing() throws SQLException {
        assertEquals(expect("Alice|senior", "Bob|senior", "Charlie|junior", "Diana|unknown"),
                rows(run("MATCH (p:Person) RETURN p.name, CASE WHEN p.age >= 30 THEN 'senior' WHEN p.age < 30"
                        + " THEN 'junior' ELSE 'unknown' END AS band ORDER BY p.name")));
        assertEquals(expect("0", "25", "30", "40"), rows(run("MATCH (p:Person) RETURN coalesce(p.age, 0) AS age"
                + " ORDER BY age")));
        assertEquals(expect("Bob", "Charlie"), rows(run("MATCH (p:Person) RETURN p.name ORDER BY p.name SKIP 1"
                + " LIMIT 2")));
        assertEquals(expect("Acme", "Globex"), rows(run("MATCH (p:Person)-[:firm]->(f:Firm) RETURN DISTINCT"
                + " f.legalName AS firm ORDER BY firm")));
    }

    @Test
    void nullSortsLastAscendingAndFirstDescending() throws SQLException {
        assertEquals(expect("Charlie|25", "Alice|30", "Bob|40", "Diana|null"),
                rows(run("MATCH (p:Person) RETURN p.name AS n, p.age AS a ORDER BY p.age, p.name")));
        assertEquals(expect("Diana|null", "Bob|40", "Alice|30", "Charlie|25"),
                rows(run("MATCH (p:Person) RETURN p.name AS n, p.age AS a ORDER BY a DESC")));
    }

    @Test
    void arithmeticAndStringPredicates() throws SQLException {
        ExecutionResult r = run("RETURN 7 / 2 AS q, -7 / 2 AS nq, 7 % 3 AS m, 2 ^ 3 AS p, 7 / 2.0 AS f,"
                + " 'a' + 'b' AS s");
        assertEquals(List.of("q", "nq", "m", "p", "f", "s"), columns(r));
        assertEquals(expect("3|-3|1|8.0|3.5|ab"), rows(r));
        assertEquals(expect("Alice", "Charlie"), rows(run("MATCH (p:Person) WHERE p.name STARTS WITH 'A'"
                + " OR p.name ENDS WITH 'e' RETURN p.name ORDER BY p.name")));
        assertEquals(expect("Bob", "Charlie"), rows(run("MATCH (p:Person) WHERE p.age IN [25, 40] RETURN p.name"
                + " ORDER BY p.name")));
    }

    @Test
    void theIslandComposesWithPure() throws SQLException {
        ExecutionResult r = new QueryService().execute(MODEL, "#Cypher{MATCH (p:Person) RETURN p.name AS name,"
                + " p.age AS age}#->filter(x|$x.age > 26)->select(~[name])->sort(~name->ascending())", "test::RT",
                session);
        assertEquals(expect("Alice", "Bob"), rows(r));
    }

    @Test
    void aSyntaxErrorIsAParseErrorAtItsPlaceInTheSource() {
        LegendCompileException e = assertThrows(LegendCompileException.class,
                () -> run("MATCH (p:Person RETURN p.name"));
        assertEquals(LegendCompileException.Phase.PARSE, e.phase());
        assertEquals("#Cypher{...}# at line 1, column 25: expected ')' but found 'RETURN'", e.getMessage());
    }

    @Test
    void aQueryNeo4jRejectsIsATypeError() {
        LegendCompileException e = assertThrows(LegendCompileException.class, () -> run("MATCH (p:Person)"
                + " RETURN p.nam"));
        assertEquals(LegendCompileException.Phase.TYPE, e.phase());
        assertTrue(e.getMessage().contains("the class test::Person has no property nam"), e.getMessage());
        LegendCompileException label = assertThrows(LegendCompileException.class, () -> run("MATCH (p:Nobody)"
                + " RETURN p.name"));
        assertTrue(label.getMessage().contains("the label Nobody names no class"), label.getMessage());
    }

    @Test
    void whatIsNotReadYetIsRefusedByName() {
        com.legend.error.NotImplementedException e = assertThrows(com.legend.error.NotImplementedException.class,
                () -> run("MATCH (p:Person)-[r:firm]->(f:Firm) RETURN r.since"));
        assertEquals("#Cypher{...}# at line 1, column 53: a property of a relationship over a property or association"
                + " (no edge class, the study's §5.6) is not supported yet", e.getMessage());
        // a label changed on a domain model: its node's labels are its class (the writes design's §3 A)
        com.legend.error.NotImplementedException relabel = assertThrows(
                com.legend.error.NotImplementedException.class, () -> run("MATCH (p:Person) SET p:Admin"));
        assertTrue(relabel.getMessage().contains("SET of a label over a graph of a domain model is not supported"),
                relabel.getMessage());
    }

    /** A write over a domain model goes through its classes' direct mappings (the writes design's leg 5, L2): the
     *  node created is the class's row, read back by the statement and by the next; the caller's rollback takes it
     *  back. */
    @Test
    void aWriteOverADomainModelGoesThroughItsMappings() throws SQLException {
        session.setAutoCommit(false);
        try {
            assertEquals(List.of("Eve|22"), rows(run("CREATE (p:Person {name: 'Eve', age: 22})"
                    + " RETURN p.name AS n, p.age AS a")));
            assertEquals(List.of("Eve"), rows(run("MATCH (p:Person) WHERE p.age = 22 RETURN p.name AS n")));
            run("MATCH (p:Person {name: 'Eve'}) SET p.age = 23");
            assertEquals(List.of("23"), rows(run("MATCH (p:Person {name: 'Eve'}) RETURN p.age AS a")));
            // (a node of a class with associations: its links are no edge class's, refused by name)
            com.legend.error.NotImplementedException delete = assertThrows(
                    com.legend.error.NotImplementedException.class, () -> run("MATCH (p:Person {name: 'Eve'})"
                    + " DELETE p"));
            assertTrue(delete.getMessage().contains("whose relationships include associations"),
                    delete.getMessage());
        } finally {
            session.rollback();
            session.setAutoCommit(true);
        }
        assertEquals(4, rows(run("MATCH (p:Person) RETURN p.name AS n")).size());
    }

    /** A model of one class whose Variant properties the graph profile's list marks mark (2026-10-20,
     *  docs/WRITES_DESIGN_2026_10_18.md §5): each a stored list of temporals of one kind, each element its text. */
    private static final String LISTS_MODEL = """
            ###Pure
            Profile graph::graph
            {
               stereotypes: [Anonymous, Edge, Open, Native, from, to, type, nanoseconds, timeOfDay,
                   listOfDate, listOfLocalTime, listOfZonedTime, listOfLocalDateTime, listOfZonedDateTime, listOfDuration];
            }
            Class test::Log
            {
                <<graph::graph.listOfDate>> dates: meta::pure::metamodel::variant::Variant[0..1];
                <<graph::graph.listOfLocalTime>> times: meta::pure::metamodel::variant::Variant[0..1];
                <<graph::graph.listOfZonedDateTime>> zoned: meta::pure::metamodel::variant::Variant[0..1];
                <<graph::graph.listOfDuration>> durs: meta::pure::metamodel::variant::Variant[0..1];
                plain: meta::pure::metamodel::variant::Variant[0..1];
            }
            ###Relational
            Database store::LDB
            (
                Table T_LOG (ID INTEGER PRIMARY KEY, DATES SEMISTRUCTURED, TIMES SEMISTRUCTURED, ZONED SEMISTRUCTURED,
                             DURS SEMISTRUCTURED, PLAIN SEMISTRUCTURED)
            )
            ###Mapping
            Mapping test::LM
            (
                test::Log: Relational
                {
                    ~mainTable [store::LDB] T_LOG
                    dates: [store::LDB] T_LOG.DATES,
                    times: [store::LDB] T_LOG.TIMES,
                    zoned: [store::LDB] T_LOG.ZONED,
                    durs: [store::LDB] T_LOG.DURS,
                    plain: [store::LDB] T_LOG.PLAIN
                }
            )
            ###Connection
            RelationalDatabaseConnection store::LConn { %s }
            ###Runtime
            Runtime test::LRT { mappings: [ test::LM ]; connections: [ store::LDB: [ environment: store::LConn ] ]; }
            """.formatted(PLATFORM.connectionBody());

    private static ExecutionResult runLists(String model, String cypher) throws SQLException {
        return new QueryService().execute(model, "#Cypher{" + cypher + "}#", "test::LRT", session);
    }

    @Test
    void aMarkedListOfTemporalsReadsItsElementsBackOrIsReadWhole() throws SQLException {
        try (Statement s = session.createStatement()) {
            s.execute("CREATE TABLE T_LOG (ID INTEGER PRIMARY KEY, DATES JSON, TIMES JSON, ZONED JSON, DURS JSON,"
                    + " PLAIN JSON)");
            s.execute("INSERT INTO T_LOG VALUES (1, '[\"Date(''1984-10-12'')\",\"Date(''1984-10-14'')\"]',"
                    + " '[\"Time(''13:00:00.000000'')\"]', '[\"DateTime(''1913-01-01T00:00:00.000000000+01:00'')\"]',"
                    + " '[\"Duration(''PT13S'')\"]', '[1,2]')");
        }
        try {
            // a list of dates or local times: its elements read back as the values (an index, IN, UNWIND)
            assertEquals(expect("true"), rows(runLists(LISTS_MODEL,
                    "MATCH (l:Log) RETURN l.dates[1] = date('1984-10-14') AS same")));
            assertEquals(expect("true"), rows(runLists(LISTS_MODEL,
                    "MATCH (l:Log) RETURN date('1984-10-12') IN l.dates AS found")));
            assertEquals(expect("12", "14"), rows(runLists(LISTS_MODEL,
                    "MATCH (l:Log) UNWIND l.dates AS d RETURN d.day AS day ORDER BY day")));
            assertEquals(expect("13"), rows(runLists(LISTS_MODEL, "MATCH (l:Log) RETURN l.times[0].hour AS h")));
            assertEquals(expect("13"), rows(runLists(LISTS_MODEL, "MATCH (l:Log) UNWIND l.times AS t RETURN t.hour AS h")));
            assertEquals(expect("[1984,1984]"), rows(runLists(LISTS_MODEL,
                    "MATCH (l:Log) RETURN [x IN l.dates | x.year] AS ys")));
            assertEquals(expect("1984-10-14"), rows(runLists(LISTS_MODEL,
                    "MATCH (l:Log) WITH l.dates AS ds RETURN ds[1] AS d")));
            // a list of zoned values or durations: read whole, as the store holds it
            assertEquals(1, rows(runLists(LISTS_MODEL, "MATCH (l:Log) RETURN l.zoned AS z, l.durs AS d")).size());
            assertEquals(expect("1"), rows(runLists(LISTS_MODEL, "MATCH (l:Log) RETURN size(l.durs) AS n")));
            // an element of one read alone: refused by name, as a read the island does not make yet
            for (String read : List.of("RETURN l.zoned[0] AS z", "UNWIND l.durs AS d RETURN d",
                    "RETURN [x IN l.zoned | x] AS xs", "RETURN l.durs[0] AS d", "RETURN head(l.zoned) AS z",
                    "WITH l.zoned AS zs UNWIND zs AS z RETURN z")) {
                com.legend.error.NotImplementedException refused = assertThrows(
                        com.legend.error.NotImplementedException.class,
                        () -> runLists(LISTS_MODEL, "MATCH (l:Log) " + read));
                assertTrue(refused.getMessage().contains("an element of a stored list of"), read + ": "
                        + refused.getMessage());
            }
            // a list mark on a property of another type is the model's error
            LegendCompileException misplaced = assertThrows(LegendCompileException.class,
                    () -> runLists(LISTS_MODEL.replace("plain: meta", "<<graph::graph.listOfDate>> plain: meta")
                            .replace("plain: meta::pure::metamodel::variant::Variant[0..1]", "plain: String[0..1]")
                            .replace("PLAIN SEMISTRUCTURED", "PLAIN VARCHAR(10)"),
                            "MATCH (l:Log) RETURN l.plain AS p"));
            assertTrue(misplaced.getMessage().contains("is marked listOfDate but is of type String"),
                    misplaced.getMessage());
        } finally {
            try (Statement s = session.createStatement()) {
                s.execute("DROP TABLE T_LOG");
            }
        }
    }

    @Test
    void anOrderByOnAnExpressionNotReturnedRendersItsHiddenKey() throws SQLException {
        // the study's §12.53: the sort key is not an output column; the result render carries it as a hidden
        // output of the ordered relation (Render.withSortOutputs); Cypher's null last ascending
        assertEquals(expect("Charlie", "Alice", "Bob", "Diana"),
                rows(run("MATCH (p:Person) RETURN p.name AS name ORDER BY p.age")));
        assertEquals(expect("Bob", "Alice", "Charlie"),
                rows(run("MATCH (p:Person) WHERE p.age IS NOT NULL WITH p.name AS name, p.age * 2 AS twice"
                        + " RETURN name ORDER BY twice DESC")));
        // the streamed JSON rows carry it too, outside the row objects (Render.jsonWireRows read its sort keys among
        // the outputs it shows, and refused the hidden one: the independent review, 2026-10-18)
        java.io.StringWriter streamed = new java.io.StringWriter();
        try {
            com.legend.Execution.executeStreaming(MODEL, "#Cypher{MATCH (p:Person) RETURN p.name AS name"
                    + " ORDER BY p.age}#", "test::RT", session, streamed);
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
        assertEquals("[{\"name\":\"Charlie\"},{\"name\":\"Alice\"},{\"name\":\"Bob\"},{\"name\":\"Diana\"}]",
                streamed.toString().replace(" ", "").replace("\n", ""));
    }
}
