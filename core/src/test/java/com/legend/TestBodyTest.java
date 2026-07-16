// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.model.ImportScope;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * NATIVE test-body execution (M1): corpus-shaped {@code <<test.Test>>}
 * bodies — lets, {@code execute(...)}, {@code assert*} — run through the
 * ordinary compile-to-SQL pipeline. The fixtures mirror the real corpus
 * idioms: an execute bound to a let, downstream reads splicing the query,
 * asserts comparing both sides through ONE wire convention.
 */
class TestBodyTest {

    private static final String MODEL = """
            Class test::Person
            {
              name : String[1];
              age  : Integer[1];
            }

            Database test::DB
            (
              Table PERSON ( NAME VARCHAR(64) PRIMARY KEY, AGE INTEGER )
            )

            Mapping test::M
            (
              test::Person : Relational
              {
                ~mainTable [test::DB] PERSON
                name : [test::DB]PERSON.NAME,
                age  : [test::DB]PERSON.AGE
              }
            )

            RelationalDatabaseConnection test::Conn
            { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }

            Runtime test::Rt
            { mappings: [ test::M ]; connections: [ test::DB: [ c: test::Conn ] ] }
            """;

    private static final ImportScope IMPORTS =
            new ImportScope(List.of("test"), Map.of());

    private static Connection seeded() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
        try (var st = conn.createStatement()) {
            st.execute("CREATE TABLE PERSON (NAME VARCHAR, AGE INT)");
            st.execute("INSERT INTO PERSON VALUES ('Bob', 30), ('Alice', 25), ('Cid', 41)");
        }
        return conn;
    }

    private static TestBody.Outcome run(String body) throws Exception {
        ModelContext ctx = Compiler.compileModel(MODEL);
        try (Connection conn = seeded()) {
            return TestBody.run(ctx, body, IMPORTS, "test::Rt", conn);
        }
    }

    private static void assertHeld(TestBody.Outcome o, int verified) {
        assertInstanceOf(TestBody.Outcome.Ran.class, o, String.valueOf(o));
        TestBody.Outcome.Ran ran = (TestBody.Outcome.Ran) o;
        assertEquals(List.of(), ran.failures());
        assertEquals(verified, ran.verified());
    }

    @Test
    void executeBindsLazily_assertEqualsOverSortedChain() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()
                        ->project([p|$p.name], ['name'])
                        ->sort(asc('name')), test::M, ignoredRuntime(), ignoredExt());
                assertEquals(['Alice', 'Bob', 'Cid'],
                        $result.values->at(0)->map(r|$r.name));
                true;
                """), 1);
    }

    @Test
    void unsortedActualComparesAsMultiset_orderPolicy() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.age], ['age']),
                        test::M, r(), e());
                assertEquals([41, 25, 30], $result.values->at(0)->map(r|$r.age));
                """), 1);
    }

    @Test
    void wrongValuesFail() throws Exception {
        TestBody.Outcome o = run("""
                let result = execute(|Person.all()->project([p|$p.age], ['age']),
                        test::M, r(), e());
                assertEquals([1, 2, 3], $result.values->at(0)->map(r|$r.age));
                """);
        TestBody.Outcome.Ran ran = (TestBody.Outcome.Ran) o;
        assertEquals(1, ran.failures().size());
    }

    @Test
    void numericKindIsStrict() throws Exception {
        // 30 (Integer) must NOT equal 30.0 (Float) — pure kind semantics
        TestBody.Outcome o = run("""
                let result = execute(|Person.all()->filter(p|$p.name == 'Bob')
                        ->project([p|$p.age], ['age']), test::M, r(), e());
                assertEquals([30.0], $result.values->at(0)->map(r|$r.age));
                """);
        assertEquals(1, ((TestBody.Outcome.Ran) o).failures().size());
    }

    @Test
    void assertSizeAndEmptyAndScalarAssert() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.name], ['name']),
                        test::M, r(), e());
                assertSize($result.values->at(0), 3);
                assertEmpty($result.values->at(0)->filter(r|$r.name == 'Nobody'));
                assert($result.values->at(0)->filter(r|$r.name == 'Bob')->size() == 1);
                assertFalse($result.values->at(0)->size() == 99);
                """), 4);
    }

    @Test
    void assertSameElementsIsMultiset() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.name], ['name']),
                        test::M, r(), e());
                assertSameElements(['Cid', 'Alice', 'Bob'],
                        $result.values->at(0)->map(r|$r.name));
                """), 1);
    }

    @Test
    void plainLetsInlineIntoQueries() throws Exception {
        assertHeld(run("""
                let cutoff = 28;
                let result = execute(|Person.all()->filter(p|$p.age > $cutoff)
                        ->project([p|$p.name], ['name']), test::M, r(), e());
                assertSize($result.values->at(0), 2);
                """), 1);
    }

    @Test
    void multipleExecutesBindIndependently() throws Exception {
        assertHeld(run("""
                let r1 = execute(|Person.all()->filter(p|$p.age > 28)
                        ->project([p|$p.name], ['n']), test::M, r(), e());
                let r2 = execute(|Person.all()->filter(p|$p.age <= 28)
                        ->project([p|$p.name], ['n']), test::M, r(), e());
                assertSize($r1.values->at(0), 2);
                assertSize($r2.values->at(0), 1);
                assertEquals(['Alice'], $r2.values->at(0)->map(x|$x.n));
                """), 3);
    }

    @Test
    void goldenSqlAssertsAreAdvisory() throws Exception {
        TestBody.Outcome o = run("""
                let result = execute(|Person.all()->project([p|$p.name], ['name']),
                        test::M, r(), e());
                assertEquals('select whatever', $result->sqlRemoveFormatting());
                assertSize($result.values->at(0), 3);
                """);
        TestBody.Outcome.Ran ran = (TestBody.Outcome.Ran) o;
        assertEquals(List.of(), ran.failures());
        assertEquals(1, ran.verified());
        assertEquals(1, ran.advisory());
    }

    @Test
    void tdsSurfaceRowsAndColumns() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.name, p|$p.age],
                        ['name', 'age']), test::M, r(), e());
                let tds = $result.values->at(0);
                assertSize($tds.rows, 3);
                assertEquals(['name', 'age'], $result.values->at(0).columns.name);
                assertEquals(['String', 'Integer'], $result.values->at(0).columns.type);
                assertSize($result.values->at(0).columns, 2);
                """), 4);
    }

    @Test
    void letAliasOfExecuteResultSplices() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.age], ['age']),
                        test::M, r(), e());
                let tds = $result.values->at(0);
                assertSameElements([25, 30, 41], $tds->map(x|$x.age));
                assertSize($tds.rows, 3);
                """), 2);
    }

    @Test
    void toCsvAgainstStringLiteral() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.name], ['who'])
                        ->sort(asc('who')), test::M, r(), e());
                assertEquals('who\\nAlice\\nBob\\nCid\\n',
                        $result.values->at(0)->toCSV());
                """), 1);
    }

    @Test
    void tdsLiteralGridCompare() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.name, p|$p.age],
                        ['name', 'age']), test::M, r(), e());
                assertEquals(#TDS
                              name, age
                              Cid, 41
                              Bob, 30
                              Alice, 25
                            #->toString(),
                        $result.values->at(0)->sort(desc('name'))->toString());
                """), 1);
    }

    @Test
    void makeStringOverMappedColumn() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.name], ['name'])
                        ->sort(asc('name')), test::M, r(), e());
                assertEquals('Alice,Bob,Cid',
                        $result.values->at(0)->map(r|$r.name)->makeString(','));
                """), 1);
    }

    @Test
    void classQueryGraphSizeAndProperties() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->filter(p|$p.age > 28),
                        test::M, r(), e());
                assertSize($result.values, 2);
                assertSameElements(['Bob', 'Cid'], $result.values.name);
                """), 2);
    }

    @Test
    void rowCellsFlattenAndJoin() throws Exception {
        assertHeld(run("""
                let result = execute(|Person.all()->project([p|$p.name], ['name'])
                        ->sort(asc('name')), test::M, r(), e());
                assertEquals('Alice,Bob,Cid',
                        $result.values.rows->map(r|$r.values)->makeString(','));
                assertEquals(['Alice', 'Bob', 'Cid'],
                        $result.values.rows->map(r|$r.values));
                """), 2);
    }

    private static final String FIRM_MODEL = """
            Class test::Person { name : String[1]; age : Integer[1]; }
            Class test::Firm { legal : String[1]; id : Integer[1]; }
            Association test::Employment
            { employer : test::Firm[1]; employees : test::Person[*]; }

            Database test::DB (
              Table FIRM ( ID INTEGER PRIMARY KEY, LEGAL VARCHAR(64) )
              Table PERSON ( NAME VARCHAR(64) PRIMARY KEY, AGE INTEGER,
                             FIRMID INTEGER )
              Join FP ( FIRM.ID = PERSON.FIRMID )
            )

            Mapping test::M (
              test::Person : Relational {
                ~mainTable [test::DB] PERSON
                name : [test::DB]PERSON.NAME, age : [test::DB]PERSON.AGE
              }
              test::Firm : Relational {
                ~mainTable [test::DB] FIRM
                legal : [test::DB]FIRM.LEGAL, id : [test::DB]FIRM.ID
              }
              test::Employment : Relational { AssociationMapping (
                employer : [test::DB]@FP, employees : [test::DB]@FP
              ) }
            )

            RelationalDatabaseConnection test::Conn
            { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }

            Runtime test::Rt
            { mappings: [ test::M ]; connections: [ test::DB: [ c: test::Conn ] ] }
            """;

    private static TestBody.Outcome runFirm(String body) throws Exception {
        ModelContext ctx = Compiler.compileModel(FIRM_MODEL);
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var st = conn.createStatement()) {
                st.execute("CREATE TABLE FIRM (ID INT, LEGAL VARCHAR)");
                st.execute("CREATE TABLE PERSON (NAME VARCHAR, AGE INT, FIRMID INT)");
                st.execute("INSERT INTO FIRM VALUES (1,'X'),(2,'A'),(3,'B')");
                st.execute("""
                        INSERT INTO PERSON VALUES ('Bob',30,1),('Alice',20,1),
                        ('Cid',40,2),('Dee',10,3)""");
            }
            return TestBody.run(ctx, body, IMPORTS, "test::Rt", conn);
        }
    }

    /** The engine subAggregation family: an aggregate over a to-many
     * navigation is a grouped-subselect join — one value PER SOURCE ROW,
     * never a row explosion, and the aggregate runs in the database. */
    @Test
    void aggregateOverToManyNavigation() throws Exception {
        assertHeld(runFirm("""
                let result = execute(|test::Firm.all()
                        ->map(f|$f.employees.age->average()),
                        test::M, r(), e());
                assertSize($result.values, 3);
                assertSameElements([25.0, 40.0, 10.0], $result.values);
                """), 2);
    }

    @Test
    void aggregateOverToManyInProjectColumns() throws Exception {
        assertHeld(runFirm("""
                let result = execute(|test::Firm.all()->project(
                        [f|$f.legal, f|$f.employees.age->max(),
                         f|$f.employees.name->joinStrings('*')],
                        ['legal', 'oldest', 'names']), test::M, r(), e());
                assertSize($result.values->at(0), 3);
                assertSameElements(['X|30|Bob*Alice', 'A|40|Cid', 'B|10|Dee'],
                        $result.values->at(0)->map(r|
                                $r.legal + '|' + $r.oldest->toOne()->toString()
                                + '|' + $r.names->toOne()));
                """), 2);
    }

    @Test
    void unknownAssertFormIsLoudUnsupported() throws Exception {
        TestBody.Outcome o = run("""
                let result = execute(|Person.all()->project([p|$p.name], ['name']),
                        test::M, r(), e());
                assertContains($result.values, 'Bob');
                """);
        assertInstanceOf(TestBody.Outcome.Unsupported.class, o);
    }

    @Test
    void unsupportedStatementIsLoud() throws Exception {
        // audit 17: an unknown function is a real pipeline failure and
        // PROPAGATES (the runner scores it ERROR) — only a
        // NotImplementedException vocabulary gap reports Unsupported/SHAPE.
        // Either way: never silent.
        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                () -> run("""
                        mysteryHarnessCall('side effect');
                        """));
    }

    @org.junit.jupiter.api.Test
    void printlnIsHarnessNoiseAndSkips() throws Exception {
        TestBody.Outcome o = run("""
                println('debug noise');
                assertEquals(1, 1);
                """);
        assertInstanceOf(TestBody.Outcome.Ran.class, o);
        assertTrue(((TestBody.Outcome.Ran) o).failures().isEmpty());
    }
}
