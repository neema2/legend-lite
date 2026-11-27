// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * A COMPARISON WRITTEN IN A FUNCTION OVER VALUES WHOSE TYPES ITS CALLER GIVES (the edition program's step 6,
 * 2026-11-27; the equality rule's run-time item and the user's rulings of 2026-11-26 on decisions D3 and D5): on
 * lite's own route two values are equal only when they have the same type and the same value -- numbers by their
 * family, an exact one (an Integer, a Decimal) never a Float, two exact ones by value (the user's ruling of
 * 2026-11-27) -- so a function written for {@code Number} or {@code Any} answers by the types its arguments have; an
 * ordering is by value, then by family (an exact number below a Float); and a number literal the function's author
 * wrote reads as the type of the value it is written against.
 */
class RunTimeComparisonTest {

    private static final String MODEL = """
            ###Pure
            Class p::Party { id: Integer[1]; ratio: Float[1]; }
            function x::same(a: Number[1], b: Number[1]): Boolean[1] { $a == $b }
            function x::less(a: Number[1], b: Number[1]): Boolean[1] { $a < $b }
            function x::lessEq(a: Number[1], b: Number[1]): Boolean[1] { $a <= $b }
            function x::greater(a: Number[1], b: Number[1]): Boolean[1] { $a > $b }
            function x::greaterEq(a: Number[1], b: Number[1]): Boolean[1] { $a >= $b }
            function x::amongWholes(a: Number[1]): Boolean[1] { $a->in([0, 1]) }
            function x::amongFloats(a: Number[1]): Boolean[1] { $a->in([0.0, 1.0]) }
            function x::isOneAndAHalf(a: Number[1]): Boolean[1] { $a == 1.5 }
            function x::sameAny(a: Any[1], b: Any[1]): Boolean[1] { $a == $b }
            function x::sameAsOne(a: Number[1]): Boolean[1] { x::same($a, 1) }
            function x::whereIn(a: Number[1], b: Number[*]): Integer[1] { $b->indexOf($a) }
            function x::sameNums(a: Number[*], b: Number[*]): Boolean[1] { $a == $b }
            function x::sameAnys(a: Any[*], b: Any[*]): Boolean[1] { $a == $b }
            function x::among(a: Number[1], b: Number[*]): Boolean[1] { $a->in($b) }
            function x::sameOpt(a: Number[0..1], b: Number[0..1]): Boolean[1] { $a == $b }
            function x::sameT<T,U>(a: T[1], b: U[1]): Boolean[1] { $a == $b }
            function x::isZeroT<T>(a: T[1]): Boolean[1] { $a == 0 }
            ###Relational
            Database p::DB ( Table PARTY (ID INTEGER PRIMARY KEY, RATIO DOUBLE) )
            ###Mapping
            Mapping p::M ( p::Party: Relational { ~mainTable [p::DB] PARTY id: PARTY.ID, ratio: PARTY.RATIO } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 {
                    testDataSetupCSV: 'default\\nPARTY\\nID,RATIO\\n1,1.0\\n2,2.5\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    private static Map<String, Object> cases() {
        Map<String, Object> out = new LinkedHashMap<>();
        // an exact number and a Float are never equal; two exact numbers (an Integer, a Decimal), and two values of
        // one type, by value (the user's ruling of 2026-11-27)
        out.put("|x::same(1, 1.0)", false);
        out.put("|x::same(1.0, 1)", false);
        out.put("|x::same(1, 1D)", true);
        out.put("|x::same(1.00D, 1)", true);
        out.put("|x::same(1, 1.5D)", false);
        out.put("|x::same(1.5, 1.5D)", false);
        out.put("|x::less(1, 1.5D)", true);
        out.put("|x::less(1, 1D)", false);
        out.put("|x::lessEq(1D, 1)", true);
        out.put("|x::same(1, 1)", true);
        out.put("|x::same(1.5, 1.5)", true);
        out.put("|x::sameAny(1, 1.0)", false);
        out.put("|x::sameAny('a', 1)", false);
        out.put("|x::sameAny('a', 'a')", true);
        // a literal handed through another function is a value there, not a literal written against the parameter
        out.put("|x::sameAsOne(1.0)", false);
        out.put("|x::sameAsOne(1)", true);
        // ordering: by value, then by type (decision D3)
        out.put("|x::less(1, 1.0)", true);
        out.put("|x::less(1.0, 1)", false);
        out.put("|x::lessEq(1, 1.0)", true);
        out.put("|x::lessEq(1.0, 1)", false);
        out.put("|x::greater(1.0, 1)", true);
        out.put("|x::greater(1, 1.0)", false);
        out.put("|x::greaterEq(1.0, 1)", true);
        out.put("|x::greaterEq(1, 1.0)", false);
        out.put("|x::less(1, 2.0)", true);
        out.put("|x::less(2, 1.0)", false);
        out.put("|x::lessEq(1, 1)", true);
        out.put("|x::greater(2.5, 2)", true);
        // a number literal the function's author wrote reads as the value's type (decision D5)
        out.put("|x::amongWholes(1.0)", true);
        out.put("|x::amongWholes(2.0)", false);
        out.put("|x::amongFloats(1)", true);
        out.put("|x::amongFloats(2)", false);
        out.put("|x::isOneAndAHalf(1.5)", true);
        out.put("|x::isOneAndAHalf(1)", false);
        // a position among values of another type: none (answered by the types, so on every database; a position
        // among values of the value's own type is the database's list position, which not every database here has)
        out.put("|x::whereIn(1.0, [1, 2])", -1L);
        return out;
    }

    @Test
    void aFunctionOverNumbersAnswersByTheTypesItsArgumentsHave() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Object> c : cases().entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    assertEquals(List.of(List.of(c.getValue())), r.rows(), backend.type() + ": " + c.getKey() + "\n"
                            + r.text());
                }
            }
        }
    }

    @Test
    void anUpcastValueIsComparedByTheTypeItHolds() throws Exception {
        // a value upcast to Number or Any in the query is the value it was (a widening cast changes nothing): it is
        // compared by the type it holds, as a function's Number parameter is -- until the edition program's step 6
        // the database compared the two by value, and [1, 2]->cast(@Number)->contains(1.0) answered true
        Map<String, Object> cases = new LinkedHashMap<>();
        cases.put("|1->cast(@Number) == 1.0->cast(@Number)", false);
        cases.put("|1->cast(@Number) == 1->cast(@Number)", true);
        cases.put("|1->cast(@Number) == 1D->cast(@Number)", true);
        cases.put("|1->cast(@Number) == 1", true);
        cases.put("|'a'->cast(@Any) == 1", false);
        cases.put("|'a'->cast(@Any) == 'a'", true);
        cases.put("|1->cast(@Number) < 1.0->cast(@Number)", true);
        cases.put("|1.0->cast(@Number) < 1->cast(@Number)", false);
        // a number literal written against it still reads as the type the value holds (decision D5): by value
        cases.put("|1->cast(@Number) == 1.0", true);
        cases.put("|1->cast(@Number) == 1.5", false);
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Object> c : cases.entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    assertEquals(List.of(List.of(c.getValue())), r.rows(), backend.type() + ": " + c.getKey() + "\n"
                            + r.text());
                }
            }
        }
        Map<String, Object> lists = new LinkedHashMap<>();
        lists.put("|[1, 2]->cast(@Number)->contains(1.0->cast(@Number))", false);
        lists.put("|[1, 2]->cast(@Number)->contains(1)", true);
        lists.put("|[1, 2]->cast(@Number)->contains(2D)", true);
        lists.put("|1.0->cast(@Number)->in([1, 2]->cast(@Number))", false);
        // (a written literal reads as the type the list's values hold: by value, decision D5)
        lists.put("|[1, 2]->cast(@Number)->contains(1.0)", true);
        String model = MODEL.formatted(TestBackends.platform().type());
        for (Map.Entry<String, Object> c : lists.entrySet()) {
            try (Connection connection = TestBackends.platform().open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(List.of(List.of(c.getValue())), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
    }

    @Test
    void whereTheCallFixesTheTypesTwoTypesAreThatCallsMistake() throws Exception {
        // the user's ruling of 2026-11-27 ("the split with compile error when we know"): a side declared Number, Date
        // or Any is compared when the query runs (above); a type variable's values are whatever each call passes, so
        // the compiler knows both types at the call -- one type compares, an Integer and a Decimal compare by value,
        // a written literal reads as the call's type, and two types are refused, naming the function
        Map<String, Object> holds = new LinkedHashMap<>();
        holds.put("|x::sameT(1, 1)", true);
        holds.put("|x::sameT('a', 'b')", false);
        holds.put("|x::sameT(1, 1D)", true);
        holds.put("|x::isZeroT(0.0)", true);
        holds.put("|x::isZeroT(0)", true);
        holds.put("|x::isZeroT(2.5D)", false);
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Object> c : holds.entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    assertEquals(List.of(List.of(c.getValue())), r.rows(), backend.type() + ": " + c.getKey() + "\n"
                            + r.text());
                }
            }
        }
        String model = MODEL.formatted(TestBackends.platform().type());
        Map<String, String> refused = new LinkedHashMap<>();
        refused.put("|x::sameT(1, 1.0)", "== compares Integer with Float in x::sameT");
        refused.put("|x::sameT(1, 'a')", "== compares Integer with String in x::sameT");
        refused.put("|x::isZeroT('a')", "== compares String with Integer in x::isZeroT");
        refused.put("|p::Party.all()->project(~[same: p|x::sameT($p.id, $p.ratio)])",
                "== compares Integer with Float in x::sameT");
        for (Map.Entry<String, String> c : refused.entrySet()) {
            try (Connection connection = TestBackends.platform().open()) {
                com.legend.error.ModelException e = org.junit.jupiter.api.Assertions.assertThrows(
                        com.legend.error.ModelException.class,
                        () -> Lean.established(model, c.getKey(), "p::RT", connection), c.getKey());
                org.junit.jupiter.api.Assertions.assertTrue(e.getMessage().startsWith(c.getValue())
                        && e.getMessage().contains("between values whose types this call gives"),
                        c.getKey() + ": " + e.getMessage());
            }
        }
    }

    @Test
    void overColumnsTheTypesAreTheColumns() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            try (Connection connection = backend.open()) {
                // id is an Integer column and ratio a Float one: never equal, whatever their values (row 1: 1 and 1.0);
                // ordered by value, then by type; a written literal reads as the column's type
                Lean.Result r = Lean.established(model, "|p::Party.all()->project(~[same: p|x::same($p.id, $p.ratio),"
                        + " less: p|x::less($p.id, $p.ratio), greater: p|x::greater($p.id, $p.ratio),"
                        + " whole: p|x::amongWholes($p.ratio)])->sort(~less->descending())", "p::RT", connection);
                assertEquals(List.of(List.of(false, true, false, true), List.of(false, true, false, false)), r.rows(),
                        backend.type() + "\n" + r.text());
            }
        }
    }

    /** Two types are never equal, and a side that would raise still raises (the user, 2026-11-27: "agreed"; the
     *  outside audit's finding): Pure evaluates both sides before it compares them, so the sixth element of a list of
     *  two is the error it is, never a quiet false. A side that raises nothing answers false, as before. */
    @Test
    void aComparisonOfTwoTypesStillRaisesWhatItsSideRaises() throws Exception {
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        try (Connection connection = backend.open()) {
            Lean.Result r = Lean.established(model, "|[1, 2]->at(1)->cast(@Number) == 2.5", "p::RT", connection);
            assertEquals(List.of(List.of(false)), r.rows(), r.text());
        }
        // the sixth element of a list of two is the database's error, never a refusal to plan (the third review of
        // 2026-11-27, L9: any error had passed)
        for (String raises : List.of("|[1, 2]->at(5)->cast(@Number) == 1.5",
                // the same in a function whose author declared the sides Number
                "|x::same([1, 2]->at(5), 1.5)")) {
            try (Connection connection = backend.open()) {
                RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                        () -> Lean.established(model, raises, "p::RT", connection), raises);
                org.junit.jupiter.api.Assertions.assertFalse(
                        e instanceof com.legend.error.NotImplementedException
                                || e instanceof com.legend.error.ModelException, raises + ": " + e);
            }
        }
    }

    /** The same for a membership and an index of two types (the user, 2026-11-27: "why cant we do the work we
     *  promised to get to one equality?"): never among them, and a single value that would raise still raises, as
     *  Pure evaluates it before it looks. Until then the answer was the constant, whatever the value's computation. */
    @Test
    void aMembershipOfTwoTypesStillRaisesWhatItsValueRaises() throws Exception {
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        java.util.Map<String, Object> answered = new java.util.LinkedHashMap<>();
        answered.put("|[1, 2]->at(1)->cast(@Number)->in([1.5, 2.5])", false);
        answered.put("|[1.5, 2.5]->contains([1, 2]->at(1)->cast(@Number))", false);
        answered.put("|[1.5, 2.5]->indexOf([1, 2]->at(1)->cast(@Number))", -1L);
        for (java.util.Map.Entry<String, Object> c : answered.entrySet()) {
            try (Connection connection = backend.open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(List.of(List.of(c.getValue())), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
        for (String raises : List.of("|[1, 2]->at(5)->cast(@Number)->in([1.5, 2.5])",
                "|[1.5, 2.5]->contains([1, 2]->at(5)->cast(@Number))",
                "|[1.5, 2.5]->indexOf([1, 2]->at(5)->cast(@Number))")) {
            try (Connection connection = backend.open()) {
                RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                        () -> Lean.established(model, raises, "p::RT", connection), raises);
                org.junit.jupiter.api.Assertions.assertFalse(
                        e instanceof com.legend.error.NotImplementedException
                                || e instanceof com.legend.error.ModelException, raises + ": " + e);
            }
        }
    }

    /** A COMPUTED LIST on a side of a comparison of two types is still computed (the closing batch; the user,
     *  2026-11-27: "Build 3"). Pure evaluates both sides before it compares them, so a list whose computation fails
     *  raises, whatever the types say. Until then the types alone answered -- false, or -1 for an index -- and the
     *  list was never computed (measured 2026-11-27: each "raises" case below answered a quiet false). Now every
     *  value of such a list is read by a statement of its own; two sides that both hold no value are the same, as
     *  {@code [] == []} is. */
    @Test
    void aComparisonOfTwoTypesStillComputesAListThatFails() throws Exception {
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        String fails = "[1, 2]->map(x | [1, 2]->at($x + 5))";
        String quiet = "[1, 2]->map(x | [1, 2]->at($x - 1))";
        java.util.Map<String, Object> answered = new java.util.LinkedHashMap<>();
        answered.put("|x::sameNums(" + quiet + ", [1.5, 2.5])", false);
        answered.put("|x::sameAnys(" + quiet + ", ['a', 'b'])", false);
        answered.put("|x::among(1.5, " + quiet + ")", false);
        answered.put("|x::whereIn(1.5, " + quiet + ")", -1L);
        // an empty computed list against a side of another type that holds values: not the same
        answered.put("|x::sameNums([1, 2]->filter(x | $x > 5), [1.5, 2.5])", false);
        answered.put("|" + quiet + "->cast(@Number) == [1.5, 2.5]->cast(@Number)", false);
        answered.put("|[1, 2]->filter(x | $x > 5)->cast(@Number) == [1.5]->filter(x | $x > 5.0)->cast(@Number)", true);
        for (java.util.Map.Entry<String, Object> c : answered.entrySet()) {
            try (Connection connection = backend.open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(List.of(List.of(c.getValue())), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
        for (String raises : List.of("|x::sameNums(" + fails + ", [1.5, 2.5])",
                "|x::sameAnys(" + fails + ", ['a', 'b'])",
                "|x::among(1.5, " + fails + ")",
                "|x::whereIn(1.5, " + fails + ")",
                "|" + fails + "->cast(@Number) == [1.5, 2.5]->cast(@Number)")) {
            try (Connection connection = backend.open()) {
                RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                        () -> Lean.established(model, raises, "p::RT", connection), raises);
                org.junit.jupiter.api.Assertions.assertFalse(
                        e instanceof com.legend.error.NotImplementedException
                                || e instanceof com.legend.error.ModelException, raises + ": " + e);
            }
        }
    }

    /** A list HOLDING values drawn afresh, one of which is read again by its place, is refused by name, as one such
     *  value read twice is: each reading would draw again, where Pure's binding is one list (measured 2026-11-27: the
     *  filter below answered no element, where the list's first is equal to itself). */
    @Test
    void aListOfFreshDrawsReadAgainByItsPlaceIsRefused() throws Exception {
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        String query = "{| let ks = [generateGuid(), generateGuid()]; $ks->filter(k | $k == $ks->at(0))->size(); }";
        try (Connection connection = backend.open()) {
            com.legend.error.NotImplementedException e = org.junit.jupiter.api.Assertions.assertThrows(
                    com.legend.error.NotImplementedException.class,
                    () -> Lean.established(model, query, "p::RT", connection));
            org.junit.jupiter.api.Assertions.assertTrue(String.valueOf(e.getMessage()).contains("drawn afresh"),
                    e.getMessage());
        }
        // the list read whole, once, is the program's own draws: answered
        try (Connection connection = backend.open()) {
            assertEquals(List.of(List.of(2L)), Lean.established(model,
                    "{| let ks = [generateGuid(), generateGuid()]; $ks->size(); }", "p::RT", connection).rows());
        }
    }

    /** TWO TYPES ARE NEVER EQUAL -- UNLESS BOTH SIDES ARE EMPTY, WHICH ARE ({@code [] == []} is true; the closing
     *  batch, 2026-11-27). Where neither side is known to hold a value the answer is "the same only where both are
     *  empty", each side read once. Until then that case was the database's own comparison across the two types: an
     *  Integer 1 against a Float 1.0 answered true. */
    @Test
    void twoTypesThatMayBothBeEmptyAreTheSameOnlyWhereBothAre() throws Exception {
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        String noInteger = "[1, 2]->filter(x | $x > 5)->first()";
        String noFloat = "[1.5]->filter(x | $x > 5.0)->first()";
        java.util.Map<String, Object> answered = new java.util.LinkedHashMap<>();
        answered.put("|x::sameOpt(" + noInteger + ", " + noFloat + ")", true);
        answered.put("|x::sameOpt([1, 2]->first(), [1.0, 2.0]->first())", false);
        answered.put("|x::sameOpt([1, 2]->first(), " + noFloat + ")", false);
        answered.put("|x::sameOpt(" + noInteger + ", [1.0, 2.0]->first())", false);
        // two exact numbers are one family: by value
        answered.put("|x::sameOpt([1, 2]->first(), [1.0D, 2.0D]->first())", true);
        for (java.util.Map.Entry<String, Object> c : answered.entrySet()) {
            try (Connection connection = backend.open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(List.of(List.of(c.getValue())), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
    }

    /** A COLUMN READ UNDER A forAll OR AN exists OVER THE ROWS IS ONE VALUE A ROW, however its side is declared (the
     *  final review of the closing batch, X1): handed to a function whose parameters hold many, each side's "holds
     *  no value" is its NULL, never a list's length -- asked for a length, a number was "empty" on SQLite (its JSON
     *  length is 0, so two different numbers were the same) and a database error on DuckDB and H2. ID is an INTEGER
     *  and RATIO a DOUBLE: an exact number and a Float are never equal, so no row's comparison holds. */
    @Test
    void aColumnHandedToAListParameterIsOneValueARow() throws Exception {
        String rows = "#>{p::DB.PARTY}#";
        List<String> wrong = new java.util.ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (String q : List.of(
                    "|" + rows + "->filter(r | " + rows + "->forAll(o | x::sameNums($o.ID, $o.RATIO)))->select(~[ID])",
                    "|" + rows + "->filter(r | " + rows + "->exists(o | x::sameNums($o.ID, $o.RATIO)))->select(~[ID])",
                    "|" + rows + "->filter(r | " + rows + "->exists(o | x::sameAnys($o.ID, $o.RATIO)))->select(~[ID])")) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, q, "p::RT", connection);
                    if (!List.of().equals(r.rows())) {
                        wrong.add(backend.type() + ": " + q + " -> " + r.rows());
                    }
                } catch (RuntimeException e) {
                    wrong.add(backend.type() + ": " + q + " -> " + e.getClass().getSimpleName() + ": "
                            + String.valueOf(e.getMessage()).lines().findFirst().orElse(""));
                }
            }
        }
        assertEquals(List.of(), wrong, "no row on any database");
    }
}
