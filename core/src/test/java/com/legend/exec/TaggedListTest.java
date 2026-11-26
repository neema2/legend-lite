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
import com.legend.testbackend.StorelessRuntime;
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

    static final String MODEL = """
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
        return PlanRunner.rows(plan, java.util.Map.of(), (plan.sendsTheAnswer() ? PlanSessions.acquired(com.legend.testbackend.TestSessions.over(c),
                plan.answerSql().target()) : PlanSessions.of(com.legend.testbackend.TestSessions.over(c))), java.util.UUID.randomUUID()).rows()
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

    /** A list WRITTEN IN THE QUERY needs no list in the SQL (the user, 2026-11-26, on the fold rule's measured SQL):
     *  the compiler knows each element's type and only an element of the value's type can equal it, so membership
     *  is the plain IN over those elements, on every database -- an equality where there is one. The elements are
     *  chosen by type; the database compares every value. (A whole number, a String and a Boolean against a list of
     *  written values are answered by the compiler, the fold rule: no statement at all.) */
    @Test
    void membershipInAWrittenListIsAPlainInOverTheElementsOfTheValuesType() throws Exception {
        String col = "[1, 2, 5, 2, 'a', true, %2014-02-01, %2014-03-01, 2.5, 'c']";
        String sql = query("|%2014-02-01->in(" + col + ")").plan("s::RT").sql();
        assertTrue(sql.contains("DATE '2014-02-01' IN (DATE '2014-02-01', DATE '2014-03-01')"), sql);
        assertFalse(sql.contains("'a'"), sql);
        assertFalse(sql.contains("list_"), sql);
        // one element of the value's type: an equality, never IN over one value
        String one = query("|2.5->in(" + col + ")").plan("s::RT").sql();
        assertTrue(one.contains("2.5E0 = 2.5E0") && !one.contains(" IN ("), one);
        // a list holding a query parameter is planned, never refused (the audit of 2026-11-26 feared a lone parameter
        // read as a LIST parameter; such a list is compared on the variant lane today, its parameter bound once)
        String parameter = query("{s:String[1] | 'a'->in([$s, 1])}").plan("s::RT").sql();
        assertEquals(1, parameter.chars().filter(ch -> ch == '?').count(), parameter);
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(true, value(c, "|%2014-02-01->in(" + col + ")"));
            assertEquals(false, value(c, "|%2014-02-02->in(" + col + ")"));
            assertEquals(true, value(c, "|2.5->in(" + col + ")"));
            assertEquals(false, value(c, "|1.0->in(" + col + ")"));
            // answered by the compiler, and by the value's own kind alone: a String is never a date's text, a
            // whole number never a Float (the audit: a date keyed by its text had matched the String)
            assertEquals(true, value(c, "|1->in(" + col + ")"));
            assertEquals(false, value(c, "|7->in(" + col + ")"));
            assertEquals(false, value(c, "|'2014-02-01'->in(" + col + ")"));
            assertEquals(false, value(c, "|'2014-02-01'->in([%2014-02-01, 1])"));
            assertEquals(false, value(c, "|2->in([2.0, 'a'])"));
            // Decimals of two scales are one type, equal by their number (S139): every Decimal of the list is read
            assertEquals(true, value(c, "|1.5D->in([2.00D, 1.5D, 'a'])"));
            assertEquals(true, value(c, "|1.50D->in([2.00D, 1.5D, 'a'])"));
            assertEquals(false, value(c, "|1.51D->in([2.00D, 1.5D, 'a'])"));
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
    void equalityIsExactInKind_andByTheValueWithinOne() throws Exception {
        // THE EQUALITY RULE within one type: the value, not its spelling (the user's ruling of 2026-11-24; the edition
        // program's step 6, slice 6.1, 2026-11-26) -- a Decimal by its number whatever its scale (register S139), a
        // date-time by its instant whatever precision it was written at (S399). Until then a Decimal at another scale
        // and a DateTime at another written precision were other kinds inside a mixed list (legend-pure's
        // BigDecimal.equals and its date's subsecond text; the L1 audit's findings 1 and 2, 2026-10-29)
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(false, value(c, "|[1.5D, 12.25D] == [1.5D, 99.99D]"));
            assertEquals(true, value(c, "|[1.5D, 12.25D] == [1.5D, 12.25D]"));
            assertEquals(true, value(c, "|[1.5D, 'a'] == [1.50D, 'a']"));
            assertEquals(false, value(c, "|[1.5D, 'a'] == [1.51D, 'a']"));
            assertEquals(true, value(c, "|[%2014-01-01T00:00:00.000, 1]->contains(%2014-01-01T00:00:00)"));
            assertEquals(true, value(c, "|[%2014-01-01T00:00:00.000, 1]->contains(%2014-01-01T00:00:00.000)"));
            assertEquals(false, value(c, "|[%2014-01-01T00:00:00.001, 1]->contains(%2014-01-01T00:00:00)"));
            assertEquals(true, value(c, "|[%2014-01-01T00:00:00.000, 1] == [%2014-01-01T00:00:00, 1]"));
            assertEquals(false, value(c, "|[%2014-01-01T00:00:00.001, 1] == [%2014-01-01T00:00:00, 1]"));
            // a day is never a date-time: two types (the same rule's other half)
            assertEquals(false, value(c, "|[%2014-01-01, 1] == [%2014-01-01T00:00:00, 1]"));
            assertEquals(false, value(c, "|[%2014-01-01, 1]->contains(%2014-01-01T00:00:00)"));
            // membership and the deduplication read every element of the type, whichever scale or precision
            assertEquals(true, value(c, "|[1.0D, 2.00D, 'a']->contains(2.0D)"));
            assertEquals(2L, value(c, "|[1.0D, 1.00D, 'a']->removeDuplicates()->size()"));
            assertEquals(2L, value(c,
                    "|[%2014-01-01T00:00:00, %2014-01-01T00:00:00.000, 'a']->removeDuplicates()->size()"));
            assertEquals(3L, value(c, "|[1.0D, 1.01D, 'a']->removeDuplicates()->size()"));
            // two lists not written out side by side (a reversed one): element by element, the same rule
            assertEquals(true, value(c, "|['a', 1.50D]->reverse() == [1.5D, 'a']"));
            // a written list holding a computed element compares that position null-safe: always a Boolean
            assertEquals(true, value(c, "|['a', 1.5D] == [['a', 'b']->at(0), 1.50D]"));
            assertEquals(false, value(c, "|['a', 1.5D] == [['a', 'b']->at(1), 1.50D]"));
            assertEquals(true, value(c, "|!(['a', 1.5D] == [['a', 'b']->at(1), 1.50D])"));
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
    static final String CONN_LET =
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
    void numbersOfDifferentTypesAreNeverEqualInAMixedList() throws Exception {
        // the equality rule (the user, 2026-11-24): two values are equal only when they have the same type and the
        // same value, so an Integer, a Float and a Decimal in a mixed list never equal one another, whatever their
        // digits. (Compiled legend-pure compares two numbers of different kinds by their texts, so 3 == 3D and
        // 1.5 == 1.5D are true there: lite refused those pairs by name until slice 6.2; they answer false now, a
        // recorded difference.)
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(false, value(c, "|[1, 'a']->contains(1.0)"));
            assertEquals(false, value(c, "|[1, 'a']->contains(1.5D)"));
            assertEquals(false, value(c, "|[1, 'a'] == [1.0, 'a']"));
            assertEquals(false, value(c, "|[3, 'a']->contains(3D)"));
            assertEquals(false, value(c, "|[3, 'a'] == [3D, 'a']"));
            assertEquals(false, value(c, "|[1.5, 'a']->contains(1.5D)"));
            // the shape no longer decides (the audit of slice 6.1, S5: one was refused, the other false)
            assertEquals(false, value(c, "|2->in([2D, 'a'])"));
            assertEquals(false, value(c, "|2->in([1, 2D, 'a'])"));
            // and the same type still compares by its value
            assertEquals(true, value(c, "|[3D, 'a']->contains(3.0D)"));
        }
    }

    @Test
    void aValueTypedOnlyAtRunTimeComparesByTheTypeItHolds() throws Exception {
        // slice 6.2: a value whose static type names no kind (Number, Any, the abstract Date) was refused against a
        // mixed list ("whose kind only its runtime value names"); it compares by the type it holds where that is
        // known -- an upcast is the value as it is, so its kind is the kind of what was cast
        try (Connection c = TestBackends.platform().open()) {
            assertEquals(true, value(c, "|[1, 'a']->contains(1->cast(@Number))"));
            assertEquals(false, value(c, "|[1.0, 'a']->contains(1->cast(@Number))"));
            assertEquals(true, value(c, "|1.5->cast(@Number)->in([1.5, 'a'])"));
            assertEquals(false, value(c, "|1.5->cast(@Number)->in([1.5D, 'a'])"));
            assertEquals(true, value(c, "|[%2014-01-01, 'a']->contains(%2014-01-01->cast(@Date))"));
            assertEquals(false, value(c, "|[%2014-01-01T00:00:00, 'a']->contains(%2014-01-01->cast(@Date))"));
            assertEquals(true, value(c, "|['a', 1]->contains('a'->cast(@Any))"));
            assertEquals(false, value(c, "|['1', 2]->contains(1->cast(@Any))"));
            // over a row: the upcast of a column is the column's value, compared by the database (no list in the SQL)
            String upcast = "|#TDS\n a\n 1\n 2\n#->extend(~hit: x | [1, 'a']->contains($x.a->cast(@Number)))->select(~hit)";
            assertEquals(List.of(true, false), values(c, upcast));
            assertEquals("SELECT coalesce(_tds0.a = 1, FALSE) AS hit", query(upcast).plan("s::RT").sql().split("\\s+FROM\\s+")[0]);
            // a value whose SQL type is a PROMOTION of what it may hold is still refused by name, never guessed: a
            // CASE of an Integer and a Float is a DOUBLE; a relation re-typed to a Number column reads it as a DOUBLE
            for (String promoted : List.of(
                    "|#TDS\n a\n 1\n 2\n#->extend(~hit: x | [1, 'a']->contains(if($x.a > 1, |1, |1.5)))->select(~hit)",
                    "|#TDS\n a\n 1\n 2\n#->cast(@meta::pure::metamodel::relation::Relation<(a:Number)>)"
                            + "->extend(~hit: x | $x.a->toOne()->in([1, 'a']))->select(~hit)")) {
                assertTrue(assertThrows(com.legend.error.NotImplementedException.class, () -> values(c, promoted))
                        .getMessage().contains("whose kind only its runtime value names"), promoted);
            }
        }
    }

    @Test
    void equalNumbersOfDifferentTypesSortInOneOrder() throws Exception {
        // slice 6.2 (the user's ruling of 2026-11-25): 3 and 3.0 are two values of two types, so a sort orders them
        // one way whatever order they were written in -- by value, then Integer, Float, Decimal (until then a tie
        // kept the list's order: [3.0, 3, 1]->sort() answered [1, 3.0, 3])
        try (Connection c = TestBackends.platform().open()) {
            assertEquals("[1, 3, 3.0]", String.valueOf(values(c, "|[3, 3.0, 1]->sort()")));
            assertEquals("[1, 3, 3.0]", String.valueOf(values(c, "|[3.0, 3, 1]->sort()")));
            assertEquals("[3, 3.0, a]", String.valueOf(values(c, "|[3.0, 'a', 3]->sort()")));
            // one number type: the order by value alone, as before
            assertEquals("[1, 2, 3]", String.valueOf(values(c, "|[3, 1, 2]->sort()")));
            assertEquals("[1.5, 2.5]", String.valueOf(values(c, "|[2.5, 1.5]->sort()")));
            // the sorted list read further, not returned: the same order (the positions themselves are placed so)
            assertEquals("3", String.valueOf(value(c, "|[3.0, 3, 5]->sort()->at(0)->toString()")));
            assertEquals("3.0", String.valueOf(value(c, "|[3.0, 3, 5]->sort()->at(1)->toString()")));
            // and reversed: the whole order the other way
            assertEquals("[3.0, 3, 1]", String.valueOf(values(c, "|[3, 3.0, 1]->sort()->reverse()")));
        }
    }

    /** A failing multiset over a raw grid fails under each judge, the database judge deciding it (2026-11-04, the core
     *  program: a keyed verdict step; until then the database judge refused the body and, under BOTH, the host decided
     *  alone). A raw grid's order is incidental, so each comparison is a multiset. The words agree where the canon's
     *  text order is the host's canonical order (Strings); where it is not (numbers, mixed kinds) the database judge
     *  prints its side in canon-text order, {@code [11, 2]} where the host prints {@code [2, 11]} -- an open finding
     *  (docs/GATES.md, 2026-11-04): the host's order needs each element's kind, which the lowering knows and the canon
     *  text does not carry (a date and an Integer both spell {@code 2014}). */
    @Test
    void aFailingMultisetOverARawGridFailsUnderEachJudge() throws Exception {
        String[][] cases = {
                {"select * from (select 1 as A, \\'x\\' as B union all select 2, \\'y\\') t order by A",
                        "[1, 'x', 2, 'z']", "expected the elements [1, \"x\", 2, \"z\"] in any order, got [1, \"x\", 2, \"y\"]"},
                {"select 10 as A union all select 2", "[2, 11]", "expected the elements [2, 11] in any order, got [10, 2]"},
                {"select \\'b\\' as A union all select \\'a\\'", "['a', 'c']",
                        "expected the elements [\"a\", \"c\"] in any order, got [\"b\", \"a\"]"},
                {"select 10 as A, \\'b\\' as B union all select 2, \\'a\\'", "[2, 'a', 11, 'b']",
                        "expected the elements [2, \"a\", 11, \"b\"] in any order, got [10, \"b\", 2, \"a\"]"}};
        try (Connection c = TestBackends.platform().open()) {
            for (String[] q : cases) {
                String let = "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                        + "type=meta::relational::runtime::DatabaseType.DuckDB);\n"
                        + "let rs = meta::relational::metamodel::execute::executeInDb('" + q[0] + "', $c);\n";
                for (com.legend.testplan.PlanningOptions.JudgeMode mode : List.of(com.legend.testplan.PlanningOptions.JudgeMode.HOST,
                        com.legend.testplan.PlanningOptions.JudgeMode.DATABASE, com.legend.testplan.PlanningOptions.JudgeMode.BOTH)) {
                    Exception failed = assertThrows(Exception.class, () -> Program.last(
                            StorelessRuntime.with(MODEL, Databases.PLATFORM),
                            // assertSameElements (B3 step 4, 2026-11-24: a raw grid's order is the SQL text's, which the
                            // planner does not read, so an in-order assertEquals over it is refused)
                            let + "assertSameElements(" + q[1] + ", $rs.rows.values);}", StorelessRuntime.RUNTIME, c,
                            com.legend.ExecuteOptions.NONE.withJudgeMode(mode)), mode + " " + q[1]);
                    // lite's own sentence, each side in its own order, the same under every judge (the user's judge
                    // model and the orchestrator's ruling, 2026-11-17: neither judge sorts for the message)
                    assertEquals(q[2], failed.getMessage(), mode + " " + q[1]);
                }
            }
        }
    }

    /** A raw grid's cell (an {@code executeInDb} grid's, its kind the wire's: no slot) as an assert's side beside a
     *  fused assert, under BOTH (G10, 2026-11-16, the corpus's {@code ddl::dropAndCreateTable}): the side keeps its own
     *  statement, decided by its shape at planning (DatabaseAsserts.attachHost). Fused, its column in the verdict
     *  statement took the other member's NULL pad of no type (DuckDB's JSON), which no Pure type names. */
    @Test
    void aRawGridCellBesideAFusedAssertHoldsUnderBoth() throws Exception {
        String body = "{| let c = ^meta::external::store::relational::runtime::TestDatabaseConnection("
                + "type=meta::relational::runtime::DatabaseType.DuckDB);\n"
                + "let rs = meta::relational::metamodel::execute::executeInDb("
                + "'select count(*) as N from (select 1 union all select 2) t', $c);\n"
                + "assertSize([1, 2], 2);\n"
                + "assertEquals(2, $rs.rows->at(0).getInteger('N'));}";
        try (Connection c = TestBackends.platform().open()) {
            for (com.legend.testplan.PlanningOptions.JudgeMode mode : java.util.List.of(com.legend.testplan.PlanningOptions.JudgeMode.HOST, com.legend.testplan.PlanningOptions.JudgeMode.BOTH)) {
                Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), body, StorelessRuntime.RUNTIME, c,
                        com.legend.ExecuteOptions.NONE.withJudgeMode(mode));
            }
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
            // under both judges (the database judge's canon over the cells as rows of a tagged value); order-free, as a
            // raw grid's order is the SQL text's, which the planner does not read (B3 step 4, 2026-11-24)
            Lean.Result both = Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    CONN_LET + "assertSameElements([1, 'x', 2, 'y'], $rs.rows.values);}", StorelessRuntime.RUNTIME, c,
                    com.legend.ExecuteOptions.NONE.withJudgeMode(com.legend.testplan.PlanningOptions.JudgeMode.BOTH));
            assertEquals(true, both.one());
            // and under the database judge alone (the core program's 100% check, 2026-11-04: under BOTH a database
            // decline is the host's to decide, so the BOTH case alone does not show the database judge decides it)
            Lean.Result database = Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    CONN_LET + "assertSameElements([1, 'x', 2, 'y'], $rs.rows.values);}", StorelessRuntime.RUNTIME, c,
                    com.legend.ExecuteOptions.NONE.withJudgeMode(com.legend.testplan.PlanningOptions.JudgeMode.DATABASE));
            assertEquals(true, database.one());
            // a row's cells read in a lambda are on the variant carrier: the literal list meets them there
            assertEquals(List.of(true, false), Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    CONN_LET + "$rs.rows->map(r | [1, 'x'] == $r.values);}", StorelessRuntime.RUNTIME, c)
                    .rows().stream().map(r -> r.get(0)).toList());
            // and under any operation over the rows, as any column is read (the planner-driver seam program's S3c,
            // 2026-10-31: the rows are the lists of their cells, the grid's report): a filter keeps the one row
            assertEquals(1L, program(c, "$rs.rows->filter(r | [2, 'y'] == $r.values)->size();"));
            // a row read both as its cells and by a column's name: refused by name, never a guess
            Exception mixed = assertThrows(Exception.class, () -> program(c,
                    "$rs.rows->map(r | ([1, 'x'] == $r.values) && ($r.A == 1));"));
            assertTrue(String.valueOf(mixed.getMessage()).contains("both as its cells"), mixed.toString());
            // the S3c audit (2026-10-31): one grid's names and cells in one statement are one grid's reports (until
            // then two reports' grids were compared by their rendered text, each with an alias of its own, and refused
            // as two grids)
            assertEquals(6L, program(c, "$rs.columnNames->size() + $rs.rows.values->size();"));
            // a second grid read by a column's name keeps its columns beside one read as cells (until then the
            // statement-wide decision made every raw grid of the statement its rows' cells)
            assertEquals(10L, program(c, "let rs2 = meta::relational::metamodel::execute::executeInDb("
                    + "'select 9 as Z', $c);\n"
                    + "$rs.rows->filter(r | [1, 'x'] == $r.values)->size()"
                    + " + $rs2.rows->map(x | $x.Z)->toOne()->cast(@Integer);"));
            // a raw grid's text: the product writes a relation's toString in the database (stage B's B2b S5a,
            // 2026-11-24), but a raw grid's columns exist only when the plan runs, so its text is refused by name (until
            // then the product refused any relation's toString at lowering, B17)
            Exception text = assertThrows(Exception.class, () -> program(c, "$rs->toString();"));
            assertTrue(String.valueOf(text.getMessage()).contains("a relation's toString over a raw grid (executeInDb):"
                    + " its columns exist only when the plan runs"), text.toString());
            // its CSV (stage B's B2b S5b, 2026-11-25; the review of S5b): the grid's report read when the plan runs, lite's
            // own CSV -- an empty text a String's, written "" (register S420) -- and under renderTdsNull refused by name,
            // the flag quoting a String's cell alone and the grid's types existing only when the plan runs
            assertEquals("A,B\n1,x\n2,y\n", program(c, "$rs->meta::relational::tests::csv::toCSV();"));
            // (a body of its own: a grid let and never read is a step before the last, which the program road refuses)
            assertEquals("E,F\n\"\",a b\n", Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM),
                    CONN_LET.substring(0, CONN_LET.indexOf("let rs")) + "meta::relational::metamodel::execute::executeInDb("
                    + "'select \\'\\' as E, \\'a b\\' as F', $c)->meta::relational::tests::csv::toCSV();}",
                    StorelessRuntime.RUNTIME, c).one());
            // no rows: the header line alone; one row whose one cell is null: an empty field and its own newline (the
            // user's ruling of 2026-11-25, Q-C, "Okay to both csv ones"; the review of e4bfa8677 asked for the grid's pins)
            String noConn = CONN_LET.substring(0, CONN_LET.indexOf("let rs"));
            assertEquals("A\n", Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), noConn
                    + "meta::relational::metamodel::execute::executeInDb('select 1 as A where 1 = 0', $c)"
                    + "->meta::relational::tests::csv::toCSV();}", StorelessRuntime.RUNTIME, c).one());
            assertEquals("A\n\n", Program.last(StorelessRuntime.with(MODEL, Databases.PLATFORM), noConn
                    + "meta::relational::metamodel::execute::executeInDb('select cast(null as varchar) as A', $c)"
                    + "->meta::relational::tests::csv::toCSV();}", StorelessRuntime.RUNTIME, c).one());
            Exception flagged = assertThrows(Exception.class, () -> program(c,
                    "$rs->meta::relational::tests::csv::toCSV(true);"));
            assertTrue(String.valueOf(flagged.getMessage()).contains("a relation's toCSV with renderTdsNull over a raw"
                    + " grid (executeInDb): its columns' types exist only when the plan runs"), flagged.toString());
            // (on the edge a raw grid's text is refused by name: TaggedListEdgeTest, the edge's test -- F's census
            // line A5, 2026-11-17: a product test names no edge class)
        }
    }
}
