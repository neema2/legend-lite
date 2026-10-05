// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * V7 batch 2, the SPLICE PIN (V7_ASSERT_VERDICT_CHARTER §5-4): a
 * statement-root assert whose ARGUMENTS read an {@code execute()}
 * handle adjudicates through the production verdict path — the
 * result-envelope splice ({@code ResultEnvelopeSplice}) serves the
 * verdict side evaluation exactly as it serves ordinary statements
 * (audit 19d B2: the platform's result frame owns the envelope).
 * Before this leg the verdict lane compiled {@code $result.values}
 * reads as raw variable reads and walled ("no row scope") — the
 * corpus dual-channel census's largest decline class.
 */
class AssertVerdictSpliceTest {

    private static final String MODEL = """
            Class e::Person { name: String[1]; age: Integer[1]; nick: String[0..1]; }
            function meta::pure::functions::asserts::assertEquals(expected:Any[*], actual:Any[*]):Boolean[1]
            {
                if(eq($expected->size(), 1) && eq($actual->size(), 1),
                   | assertEquals($expected, $actual, '\\nexpected: %r\\nactual:   %r', [$expected->toOne(), $actual->toOne()]),
                   | assertEquals($expected, $actual, '\\nexpected: %s\\nactual:   %s', [$expected->map(x | $x->toRepresentation())->joinStrings('[', ', ', ']'), $actual->map(x | $x->toRepresentation())->joinStrings('[', ', ', ']')]));
            }
            function meta::pure::functions::asserts::assertEquals(expected:Any[*], actual:Any[*], formatString:String[1], formatArgs:Any[*]):Boolean[1]
            {
                assert(equal($expected, $actual), $formatString, $formatArgs);
            }
            function meta::pure::functions::asserts::assert(condition:Boolean[1], formatString:String[1], formatArgs:Any[*]):Boolean[1]
            {
                assert($condition, | format($formatString, $formatArgs));
            }
            function meta::pure::functions::asserts::assertSize(collection:Any[*], size:Integer[1]):Boolean[1]
            {
                assertSize($collection, $size, '%s', ['size']);
            }
            function meta::pure::functions::asserts::assertSize(collection:Any[*], size:Integer[1], formatString:String[1], formatArgs:Any[*]):Boolean[1]
            {
                assert($collection->size() == $size, $formatString, $formatArgs);
            }
            function meta::pure::functions::asserts::assertContains(collection:Any[*], value:Any[1]):Boolean[1]
            {
                assertContains($collection, $value, 'does not contain');
            }
            function meta::pure::functions::asserts::assertContains(collection:Any[*], value:Any[1], message:String[1]):Boolean[1]
            {
                assert($collection->contains($value), $message);
            }
            function meta::relational::mapping::sql(result:meta::pure::mapping::Result<Any|*>[1]):String[1]
            {
                $result->meta::relational::mapping::sql(0)
            }
            function meta::relational::mapping::sql(result:meta::pure::mapping::Result<Any|*>[1], activityNumber:Integer[1]):String[1]
            {
                $result.activities->filter(a | $a->instanceOf(meta::relational::mapping::RelationalActivity))->at($activityNumber)->cast(@meta::relational::mapping::RelationalActivity).sql
            }
            function meta::relational::mapping::sqlRemoveFormatting(result:meta::pure::mapping::Result<Any|*>[1]):String[1]
            {
                $result->meta::relational::mapping::sqlRemoveFormatting(0);
            }
            function meta::relational::mapping::sqlRemoveFormatting(result:meta::pure::mapping::Result<Any|*>[1], activityNumber:Integer[1]):String[1]
            {
                $result->meta::relational::mapping::sql($activityNumber)->meta::relational::mapping::sqlRemoveFormatting()
            }
            function meta::relational::mapping::sqlRemoveFormatting(sql:String[1]):String[1]
            {
                $sql->replace('\\n', '')->replace('\\t', '')
            }
            ###Relational
            Database e::DB (
              Table P (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), AGE INTEGER, NICK VARCHAR(50))
            )
            ###Mapping
            Mapping e::M (
              *e::Person : Relational { ~mainTable [e::DB] P
                name: P.NAME,
                age: P.AGE,
                nick: P.NICK }
            )
            ###Connection
            RelationalDatabaseConnection e::DBDuckDB { store: e::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime e::RT { mappings: [e::M]; connections: [ e::DB: [ c0: e::DBDuckDB ] ]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE P (ID INTEGER, NAME VARCHAR(200),"
                    + " AGE INTEGER, NICK VARCHAR(50))");
            st.execute("INSERT INTO P VALUES (10,'p1',30,NULL),"
                    + "(20,'p2',40,NULL)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static ExecutionResult run(String query) throws SQLException {
        return Execution.execute(MODEL, query, "e::RT", conn);
    }

    @Test
    @DisplayName("assertEquals over $result.values reads adjudicates (splice pin)")
    void assertOverExecuteHandleAdjudicates() throws Exception {
        Object v = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->filter(p|$p.age > 0), e::M, e::RT.runtimeValue, []);"
                + " assertEquals(['p1','p2'],"
                + " $result.values.name->sort());}")).value();
        assertEquals(Boolean.TRUE, v);
    }

    @Test
    @DisplayName("sql(result) reads the activity log — the frame's own"
            + " rendered SQL (engine helperFunctions bodies, verbatim)")
    void sqlReadFoldsToFrameRender() throws Exception {
        Object v = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.name], ['name']), e::M, e::RT.runtimeValue, []);"
                + " $result.activities->filter(a|$a->instanceOf("
                + "meta::relational::mapping::RelationalActivity))->at(0)"
                + "->cast(@meta::relational::mapping::RelationalActivity)"
                + ".sql;}")).value();
        assertEquals("select \"root\".NAME as \"name\" from P as \"root\"", v);
    }

    @Test
    @DisplayName("sqlRemoveFormatting($result) through the VERBATIM helper"
            + " functions folds at the call (exact FQN, pre-inline)")
    void sqlProducerCallFolds() throws Exception {
        Object v = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.name], ['name']), e::M, e::RT.runtimeValue, []);"
                + " meta::relational::mapping::sqlRemoveFormatting($result);}"))
                .value();
        assertEquals("select \"root\".NAME as \"name\" from P as \"root\"", v);
    }

    @Test
    @DisplayName("polarity: a failing spliced assert still fails")
    void splicedAssertKeepsPolarity() {
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|let result = execute(|e::Person.all()"
                + "->filter(p|$p.age > 0), e::M, e::RT.runtimeValue, []);"
                + " assertEquals(['zz'],"
                + " $result.values.name->sort());}"));
        assertTrue(String.valueOf(e.getMessage()).contains("zz"),
                e.getMessage());
    }

    @Test
    @DisplayName("D3 order view: an UNSORTED store read compares as a multiset"
            + " (DB-incidental order); a SORTED read stays strict")
    void incidentalOrderPolicy() throws Exception {
        // golden written in the REVERSED order of arrival — engine
        // goldens encode H2's incidental order, ours is DuckDB's
        Object v = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all(), e::M, e::RT.runtimeValue, []);"
                + " assertEquals([40, 30], $result.values.age);}")).value();
        assertEquals(Boolean.TRUE, v);
        // a SORTED chain pins the order — the reversed golden FAILS
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|let result = execute(|e::Person.all(), e::M, e::RT.runtimeValue, []);"
                + " assertEquals([40, 30], $result.values.age->sort());}"));
        assertTrue(String.valueOf(e.getMessage()).contains("40"),
                e.getMessage());
    }

    @Test
    @DisplayName("D3 grid pair: two relation-stamped sides adjudicate via the"
            + " grid owner (columns + rows), not a byte decline")
    void gridPairVerdict() throws Exception {
        Object v = ((ExecutionResult.Scalar) run(
                "{|assertEquals(#TDS\n  age\n  30\n  40\n#,"
                + " e::Person.all()->project([p|$p.age], ['age'])"
                + "->sort('age')->from(e::M, e::RT));}")).value();
        assertEquals(Boolean.TRUE, v);
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|assertEquals(#TDS\n  age\n  30\n  41\n#,"
                + " e::Person.all()->project([p|$p.age], ['age'])"
                + "->sort('age')->from(e::M, e::RT));}"));
        assertTrue(String.valueOf(e.getMessage()).contains("assertEquals"),
                e.getMessage());
    }

    @Test
    @DisplayName("assertSize envelope rule: $r.values of a relation execute is ONE"
            + " carrier; a rows read counts rows; assertContains judges membership")
    void sizeEnvelopeAndContains() throws Exception {
        Object one = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.age], ['age']), e::M, e::RT.runtimeValue, []);"
                + " assertSize($result.values, 1);}")).value();
        assertEquals(Boolean.TRUE, one);
        Object rows = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.age], ['age']), e::M, e::RT.runtimeValue, []);"
                + " assertSize($result.values.rows, 2);}")).value();
        assertEquals(Boolean.TRUE, rows);
        Object member = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all(), e::M, e::RT.runtimeValue, []);"
                + " assertContains($result.values.age, 30);}")).value();
        assertEquals(Boolean.TRUE, member);
        com.legend.error.AssertFailed miss = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|let result = execute(|e::Person.all(), e::M, e::RT.runtimeValue, []);"
                + " assertContains($result.values.age, 99);}"));
        assertTrue(String.valueOf(miss.getMessage()).contains("99"),
                miss.getMessage());
    }

    @Test
    @DisplayName("§8 leg 1 flat cells: ordered pass, row-swap passes under"
            + " incidental order, CROSS-ROW SHUFFLE FAILS (row cohesion)")
    void flatCellsRowCohesion() throws Exception {
        String prefix = "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.name, p|$p.age], ['name','age']),"
                + " e::M, e::RT.runtimeValue, []);";
        Object ordered = ((ExecutionResult.Scalar) run(prefix
                + " assertEquals(['p1', 30, 'p2', 40],"
                + " $result.values.rows.values);}")).value();
        assertEquals(Boolean.TRUE, ordered);
        // whole-row swap: DB arrival order is incidental — holds
        Object swapped = ((ExecutionResult.Scalar) run(prefix
                + " assertEquals(['p2', 40, 'p1', 30],"
                + " $result.values.rows.values);}")).value();
        assertEquals(Boolean.TRUE, swapped);
        // cross-row CELL shuffle: same loose cells, broken row
        // cohesion — must FAIL (audit 9)
        com.legend.error.AssertFailed e = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(prefix
                + " assertEquals(['p1', 40, 'p2', 30],"
                + " $result.values.rows.values);}"));
        assertTrue(String.valueOf(e.getMessage()).contains("TDSRow.values"),
                e.getMessage());
    }

    @Test
    @DisplayName("§8 leg 1: the golden's TDSNull sentinel matches a NULL"
            + " cell (expected direction, byte + host)")
    void flatCellsTdsNullSentinel() throws Exception {
        Object v = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.name, p|$p.nick], ['name','nick']),"
                + " e::M, e::RT.runtimeValue, []);"
                + " assertEquals(['p1', 'TDSNull', 'p2', 'TDSNull'],"
                + " $result.values.rows.values);}")).value();
        assertEquals(Boolean.TRUE, v);
    }

    @Test
    @DisplayName("§8 leg 1: assertSameElements over flat cells is the"
            + " LOOSE cell pool (cross-row shuffle holds there)")
    void flatCellsSameElementsLoosePool() throws Exception {
        Object v = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.name, p|$p.age], ['name','age']),"
                + " e::M, e::RT.runtimeValue, []);"
                + " assertSameElements(['p1', 40, 'p2', 30],"
                + " $result.values.rows.values);}")).value();
        assertEquals(Boolean.TRUE, v);
        com.legend.error.AssertFailed miss = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.name, p|$p.age], ['name','age']),"
                + " e::M, e::RT.runtimeValue, []);"
                + " assertSameElements(['p1', 41, 'p2', 30],"
                + " $result.values.rows.values);}"));
        assertTrue(String.valueOf(miss.getMessage()).contains("41"),
                miss.getMessage());
    }

    @Test
    @DisplayName("§8 leg 1: emptiness of a TABULAR side is its row count")
    void gridEmptiness() throws Exception {
        Object notEmpty = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->project([p|$p.age], ['age']), e::M, e::RT.runtimeValue, []);"
                + " assertNotEmpty($result.values);}")).value();
        assertEquals(Boolean.TRUE, notEmpty);
        Object emptied = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->filter(p|$p.age > 99)"
                + "->project([p|$p.age], ['age']), e::M, e::RT.runtimeValue, []);"
                + " assertEmpty($result.values);}")).value();
        assertEquals(Boolean.TRUE, emptied);
    }

    @Test
    @DisplayName("§8 leg 2: a serialize execute's Result carries the inner"
            + " query's multiplicity — .values types String[1] and the"
            + " strict JSON signature accepts it")
    void resultMultiplicityRefinesForJsonAssert() throws Exception {
        Object v = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all()"
                + "->graphFetch(#{e::Person{name}}#)"
                + "->serialize(#{e::Person{name}}#), e::M, e::RT.runtimeValue, []);"
                + " assertJsonStringsEqual("
                + "'[{\"name\":\"p1\"},{\"name\":\"p2\"}]',"
                + " $result.values);}")).value();
        assertEquals(Boolean.TRUE, v);
        com.legend.error.AssertFailed miss = assertThrows(
                com.legend.error.AssertFailed.class, () -> run(
                "{|let result = execute(|e::Person.all()"
                + "->graphFetch(#{e::Person{name}}#)"
                + "->serialize(#{e::Person{name}}#), e::M, e::RT.runtimeValue, []);"
                + " assertJsonStringsEqual("
                + "'[{\"name\":\"zz\"},{\"name\":\"p2\"}]',"
                + " $result.values);}"));
        assertTrue(String.valueOf(miss.getMessage()).contains("zz"),
                miss.getMessage());
    }

    @Test
    @DisplayName("assert condition + assertSize over frame reads adjudicate")
    void conditionAndSizeLanesSplice() throws Exception {
        Object c = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all(), e::M, e::RT.runtimeValue, []);"
                + " assert($result.values.name->size() == 2);}")).value();
        assertEquals(Boolean.TRUE, c);
        Object s = ((ExecutionResult.Scalar) run(
                "{|let result = execute(|e::Person.all(), e::M, e::RT.runtimeValue, []);"
                + " assertSize($result.values.age, 2);}")).value();
        assertEquals(Boolean.TRUE, s);
    }
}
