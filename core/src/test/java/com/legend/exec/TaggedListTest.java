// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.plan.QueryPlan;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanRoundTrip;
import com.legend.testing.Lean;
import com.legend.testing.Program;
import com.legend.test.StorelessRuntime;
import com.legend.values.PureDateLiteral;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A mixed list as a list of tagged values (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a's step B; the result boundary
 * program's track L, 2026-10-29; the decisions log D50): the plan's one column is {@code TAGGED<kind slot, ...>}
 * with the list's multiplicity, the statement's rows are each a tag and the slots and spell no value, the typed seam
 * reads each element in its own kind, and the list's operations -- its sort, its dedup, membership, indexing, its
 * print -- read the kinds. The successor of {@link TaggedSelectionTest} for lists. On the platform's own engine.
 */
class TaggedListTest {

    private static final String MODEL = """
            Enum s::Colour { RED, GREEN }
            Enum s::Alpha { Z }
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: %s;
                specification: LocalH2 { };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """.formatted(Databases.PLATFORM.name());

    private static TypedQuery query(String q) {
        return Compiler.query(Compiler.compileModel(MODEL), q);
    }

    /** {@code q}'s typed rows through its plan, run on {@code c}: each row's one cell. */
    private static List<Object> values(Connection c, String q) throws Exception {
        ExecutionPlan plan = query(q).executionPlan("s::RT");
        PlanRoundTrip.assertRoundTrips(plan, q);
        return PlanRunner.rows(plan, java.util.Map.of(), PlanSessions.setUp(com.legend.testbackend.TestSessions.over(c), plan.answerSql().target())).rows()
                .stream().map(r -> r.get(0)).toList();
    }

    private static Object value(Connection c, String q) throws Exception {
        List<Object> vs = values(c, q);
        assertEquals(1, vs.size(), q + " answered " + vs);
        return vs.get(0);
    }

    @Test
    void thePlanCarriesTheKindsAsSlots_andSpellsNoValue() {
        QueryPlan plan = query("|[1, 'a', true, 2.5, %2014-02-01]").plan("s::RT");
        assertEquals(1, plan.columns().size(), plan.sql());
        ExecutionPlan.Column value = plan.columns().get(0);
        assertEquals("TAGGED<Integer BIGINT, String VARCHAR, Boolean BOOLEAN, Float DOUBLE, StrictDate DATE>",
                value.slot());
        String sql = plan.sql();
        for (String column : List.of("value__0", "value__1", "value__2", "value__3", "value__4")) {
            assertTrue(sql.contains(column), sql);
        }
        // the spelling carrier's machinery is gone: no literal cast, no spelled number
        assertFalse(sql.contains("regexp"), sql);
        assertFalse(sql.contains("'1'"), sql);
        assertFalse(sql.contains("'true'"), sql);
    }

    @Test
    void theElementsAreReadInTheirOwnKinds() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(List.of(1L, "a", true, 2.5d, PureDateLiteral.parse("2014-02-01")),
                    values(c, "|[1, 'a', true, 2.5, %2014-02-01]"));
            // a Number mix at a root keeps each element's kind (the Integer stays an Integer)
            assertEquals(List.of(25.0d, 1L), values(c, "|[25.0, 1]"));
            // a one-element mixed collection is the tagged value itself
            assertEquals(1L, value(c, "|[1]->cast(@Any)"));
        }
    }

    @Test
    void sortOrdersByPuresCompare_dedupKeepsTheFirstOfEachKind() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // the PCT cases: sort::testMixedSortNoComparator, removeDuplicates::...MixedTypes, ...Explicit
            assertEquals(List.of(-2.0d, 1L, 5.0d, 171L, 342L), values(c, "|[342, 5.0, -2.0, 171, 1]->sort()"));
            assertEquals(List.of(1L, 2L, 3L, "1", "3"),
                    values(c, "|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates()->sort()"));
            assertEquals(List.of(1L, 2L, "1", "3", 3L),
                    values(c, "|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates()"));
            assertEquals(List.of(1L, 2L, "1", "3", 3L),
                    values(c, "|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates(eq_Any_1__Any_1__Boolean_1_)"));
            // a comparator over the elements: toString equality crosses kinds
            assertEquals(List.of(1L, 2L, "3"),
                    values(c, "|[1, 2, '1', '3', 1, 3, '3', 2]->removeDuplicates({x, y | $x->toString() == $y->toString()})"));
            assertEquals(List.of("1", 2L, "3"),
                    values(c, "|['1', 2, '1', '3', 1, 3, '3', 2]->removeDuplicatesBy({x | $x->toString()})"));
        }
    }

    @Test
    void membershipIsByTheNeedlesKind() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            String col = "[1, 2, 5, 2, 'a', true, %2014-02-01, 'c']";
            assertEquals(true, value(c, "|" + col + "->contains(1)"));
            assertEquals(true, value(c, "|" + col + "->contains('a')"));
            assertEquals(true, value(c, "|" + col + "->contains(true)"));
            assertEquals(true, value(c, "|" + col + "->contains(%2014-02-01)"));
            assertEquals(false, value(c, "|" + col + "->contains(-5)"));
            assertEquals(false, value(c, "|" + col + "->contains('b')"));
            // Pure's equality never crosses kinds: 1 is not 1.0, '1' is not 1
            assertEquals(false, value(c, "|" + col + "->contains(1.0)"));
            assertEquals(false, value(c, "|" + col + "->contains('1')"));
            assertEquals(true, value(c, "|1->in(" + col + ")"));
            assertEquals(false, value(c, "|'1'->in(" + col + ")"));
        }
    }

    @Test
    void indexingSizeAndPrintReadTheKinds() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            assertEquals("a", value(c, "|[1, 'a']->at(1)"));
            assertEquals(1L, value(c, "|[1, 'a']->at(0)"));
            assertEquals(1L, value(c, "|[1, 'a', true]->first()"));
            assertEquals(true, value(c, "|[1, 'a', true]->last()"));
            assertEquals(3L, value(c, "|[1, 'a', true]->size()"));
            assertEquals(List.of(true, "a", 1L), values(c, "|[1, 'a', true]->reverse()"));
            // Pure has no toString over a list: makeString prints each element as its kind
            assertEquals("1, a, true, 2.5", value(c, "|[1, 'a', true, 2.5]->makeString(', ')"));
            assertEquals("fox and 3", value(c, "|format('%s and %d', ['fox', 3])"));
            assertEquals(true, value(c, "|[1, 'a'] == [1, 'a']"));
            assertEquals(false, value(c, "|[1, 'a'] == ['1', 'a']"));
            assertEquals(false, value(c, "|[1, 'a'] == [1.0, 'a']"));
        }
    }

    @Test
    void anEnumerationsValueIsItsOwnKind() throws Exception {
        // the kind is the enumeration's path, the slot its name; Pure's eq never equates it with its name's string
        // (eq.pure: assertFalse(eq('a', TestEnum1.SECOND))) nor with a number (assertFalse(eq(TestEnum1.FIRST, 1)))
        QueryPlan plan = query("|[s::Colour.RED, 1, 'RED']").plan("s::RT");
        assertEquals("TAGGED<s::Colour VARCHAR, Integer BIGINT, String VARCHAR>", plan.columns().get(0).slot());
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(List.of("RED", 1L, "RED"), values(c, "|[s::Colour.RED, 1, 'RED']"));
            assertEquals(true, value(c, "|[s::Colour.RED, 1]->contains(s::Colour.RED)"));
            assertEquals(false, value(c, "|[s::Colour.RED, 1]->contains(s::Colour.GREEN)"));
            assertEquals(false, value(c, "|[s::Colour.RED, 1]->contains('RED')"));
            assertEquals(true, value(c, "|[s::Colour.RED, 1] == [s::Colour.RED, 1]"));
            assertEquals(false, value(c, "|[s::Colour.RED, 1] == ['RED', 1]"));
            // an enumeration's value sorts after every primitive (legend-pure's compareInt: a class outside the
            // primitive comparison order is greater than any in it); two enumerations' values by their NAMES alone
            // (every enum value is one generated class comparing by getName(): EnumProcessor), never by their paths
            assertEquals(List.of(1L, "a", "RED"), values(c, "|['a', s::Colour.RED, 1]->sort()"));
            assertEquals(List.of("RED", "Z"), values(c, "|[s::Alpha.Z, s::Colour.RED]->sort()"));
        }
    }

    @Test
    void equalityIsExactInKindScaleAndWrittenPrecision() throws Exception {
        // the L1 audit's findings 1 and 2 (2026-10-29): a Decimal at another scale and a DateTime at another written
        // precision are other kinds (BigDecimal.equals reads the scale; AbstractPureDate.equals the subsecond text)
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(false, value(c, "|[1.5D, 12.25D] == [1.5D, 99.99D]"));
            assertEquals(true, value(c, "|[1.5D, 12.25D] == [1.5D, 12.25D]"));
            assertEquals(false, value(c, "|[1.5D, 'a'] == [1.50D, 'a']"));
            assertEquals(false, value(c, "|[%2014-01-01T00:00:00.000, 1]->contains(%2014-01-01T00:00:00)"));
            assertEquals(true, value(c, "|[%2014-01-01T00:00:00.000, 1]->contains(%2014-01-01T00:00:00.000)"));
            assertEquals(false, value(c, "|[%2014-01-01T00:00:00.000, 1] == [%2014-01-01T00:00:00, 1]"));
            // a mixed selection's winner is a member by element (finding 4: never a static false)
            assertEquals(true, value(c, "|[1, 2.5]->contains([1, 2.5]->max())"));
            assertEquals(true, value(c, "|[1, 2.5, 'a']->contains([1, 2.5]->max())"));
            // against the empty list (finding 8) and against a computed list of one kind (finding 9)
            assertEquals(false, value(c, "|[1, 'a'] == []"));
            assertEquals(false, value(c, "|[1, 'a'] == ['x', 'y']->map(x | $x)"));
            assertEquals(false, value(c, "|[1, 'a'] == ['1', 'a']->map(x | $x->size())->map(n | $n)"
                    + "->filter(n | $n > 5)")); // the computed list is empty: two elements are not none
        }
    }

    @Test
    void mapAndAnEmptyElement() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // map over a mixed list: the lambda's parameter is the element, in its kind (finding 7)
            assertEquals(List.of("1", "a", "true"), values(c, "|[1, 'a', true]->map(x | $x->toString())"));
            // (an element that may be empty, [$optional, 'a', ...], keeps its present positions
            // (TaggedLists.presentPositions); the typer admits no [0..1] element in a collection literal today --
            // "Collection element must have a multiplicity [1]" -- so no query reaches it: the homework README's
            // findings)
        }
    }

    /** A query on a session the program's own connection literal names, as the corpus's tests do (its rows ordered:
     *  a row's cells read in a lambda are aggregated in the scan's order, and a union's is the planner's). */
    private static final String CONN_LET =
            "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                    + "type=meta::relational::runtime::DatabaseType.DuckDB);\n"
                    + "let rs = meta::relational::metamodel::execute::executeInDb("
                    + "'select * from (select 1 as A, \\'x\\' as B union all select 2, \\'y\\') t order by A', $c);\n";

    private static Object program(Connection c, String body) throws Exception {
        return Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), CONN_LET + body + "}",
                StorelessRuntime.RUNTIME, c).one();
    }

    /** A store, a mapping and a runtime on the platform's DuckDB: the corpus's whole-TDS idiom over TYPED columns
     *  (an executeInDb grid's columns are untyped and take the variant lane: the test above). */
    private static final String STORE_MODEL = """
            Class t::Person { name: String[1]; age: Integer[1]; }
            ###Relational
            Database t::DB ( Table P (ID INTEGER PRIMARY KEY, NAME VARCHAR(200), AGE INTEGER) )
            ###Mapping
            Mapping t::M ( *t::Person : Relational { ~mainTable [t::DB] P name: P.NAME, age: P.AGE } )
            ###Connection
            RelationalDatabaseConnection t::C { store: t::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime t::RT { mappings: [t::M]; connections: [ t::DB: [ c0: t::C ] ]; }
            """;

    private static Object stored(Connection c, String body) throws Exception {
        return Program.last(STORE_MODEL, body, "t::RT", c).one();
    }

    @Test
    void aTypedRelationsCellsKeepTheRelationsOrder() throws Exception {
        // the tagged flatten over TYPED columns (ValueCollections.rowMajorTaggedList; the L2 audit, 2026-10-29):
        // rows numbered by the relation's own sort keys, a sorted star frame's too (a sorted table threw)
        try (Connection c = TestBackends.platform().open()) {
            try (java.sql.Statement st = c.createStatement()) {
                st.execute("CREATE TABLE P (ID INTEGER, NAME VARCHAR(200), AGE INTEGER)");
                st.execute("INSERT INTO P VALUES (10, 'p1', 30), (20, 'p2', 40)");
            }
            String project = "{|let result = execute(|t::Person.all()->project([p|$p.name, p|$p.age], ['name','age'])";
            String tail = ", t::M, t::RT.runtimeValue, []);";
            assertEquals(true, stored(c, project + "->sort(desc('name'))" + tail
                    + " ['p2', 40, 'p1', 30] == $result.values.rows.values;}"));
            assertEquals(false, stored(c, project + "->sort(desc('name'))" + tail
                    + " ['p1', 30, 'p2', 40] == $result.values.rows.values;}"));
            assertEquals(true, stored(c, project + "->sort(asc('name'))" + tail
                    + " ['p1', 30, 'p2', 40] == $result.values.rows.values;}"));
            // a relation of no order of its own: its cells in all-columns order (register S124)
            assertEquals(true, stored(c, project + tail + " ['p1', 30, 'p2', 40] == $result.values.rows.values;}"));
            assertEquals(4L, stored(c, project + tail + " $result.values.rows.values->size();}"));
            // a sorted STAR frame (a table's own columns, no projection): the finding's crash
            assertEquals(true, stored(c, "{|[20, 'p2', 40, 10, 'p1', 30] == #>{t::DB.P}#->sort(~NAME->descending());}"));
            assertEquals(false, stored(c, "{|[10, 'p1', 30, 20, 'p2', 40] == #>{t::DB.P}#->sort(~NAME->descending());}"));
        }
    }

    @Test
    void aMixedListFiltersByItsElements() throws Exception {
        // filter over a mixed list (the L2 audit's finding 12): the predicate's parameter is the element
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(List.of("a"), values(c, "|[1, 'a', 2]->filter(x | $x == 'a')"));
            assertEquals(2L, value(c, "|[1, 'a', 2]->filter(x | $x != 'a')->size()"));
        }
    }

    @Test
    void numbersOfDifferentKindsAreRefusedWherePureEquatesTheirTexts() throws Exception {
        // the L2 audit's finding 4: compiled legend-pure's eq(Number, Number) compares the texts, so 3 == 3D and
        // 1.5 == 1.5D are true there; lite refuses those pairs by name until it lowers the rule (a static false
        // stood); the pairs whose texts never agree keep their false
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(false, value(c, "|[1, 'a']->contains(1.0)"));
            assertEquals(false, value(c, "|[1, 'a']->contains(1.5D)"));
            assertEquals(false, value(c, "|[1, 'a'] == [1.0, 'a']"));
            assertTrue(assertThrows(com.legend.error.NotImplementedException.class,
                    () -> value(c, "|[3, 'a']->contains(3D)")).getMessage().contains("Integer"));
            assertTrue(assertThrows(com.legend.error.NotImplementedException.class,
                    () -> value(c, "|[3, 'a'] == [3D, 'a']")).getMessage().contains("Decimal"));
            assertTrue(assertThrows(com.legend.error.NotImplementedException.class,
                    () -> value(c, "|[1.5, 'a']->contains(1.5D)")).getMessage().contains("Float"));
        }
    }

    @Test
    void anUntypedGridsCellsAreReadOnTheVariantLane() throws Exception {
        // the corpus's whole-TDS idiom (testUsingSameAggFunctionTwice: a literal list against $result.values.rows
        // .values, under == and under assertEquals) over an executeInDb grid, whose columns are UNTYPED: the
        // variant lane, one LIST over the grid in its own order (the typed columns' tagged flatten is the test
        // above; the L2 audit, 2026-10-29, found this test named for the flatten it never reached)
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(true, program(c, "[1, 'x', 2, 'y'] == $rs.rows.values;"));
            // a sorted relation keeps its order (the rows numbered by its own sort keys: finding 5)
            assertEquals(true, Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    CONN_LET.replace("order by A", "order by A desc") + "[2, 'y', 1, 'x'] == $rs.rows.values;}",
                    StorelessRuntime.RUNTIME, c).one());
            assertEquals(false, program(c, "[1.0, 'x', 2, 'y'] == $rs.rows.values;"));
            assertEquals(false, program(c, "[1, 'x', 2] == $rs.rows.values;"));
            assertEquals(4L, program(c, "$rs.rows.values->size();"));
            assertEquals(true, program(c, "$rs.rows.values->contains('y');"));
            assertEquals(false, program(c, "$rs.rows.values->contains('1');"));
            // under both judges (the database judge's canon over the cells as rows of a tagged value)
            Lean.Result both = Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    CONN_LET + "assertEquals([1, 'x', 2, 'y'], $rs.rows.values);}", StorelessRuntime.RUNTIME, c,
                    com.legend.ExecuteOptions.NONE.withJudgeMode(com.legend.ExecuteOptions.JudgeMode.BOTH));
            assertEquals(true, both.one());
            // a row's cells read in a lambda are on the variant carrier: the literal list meets them there
            assertEquals(List.of(true, false), Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    CONN_LET + "$rs.rows->map(r | [1, 'x'] == $r.values);}", StorelessRuntime.RUNTIME, c)
                    .rows().stream().map(r -> r.get(0)).toList());
        }
    }
}
