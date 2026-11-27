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
        try (Connection connection = backend.open()) {
            org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> Lean.established(model,
                    "|[1, 2]->at(5)->cast(@Number) == 1.5", "p::RT", connection));
        }
        try (Connection connection = backend.open()) {
            // the same in a function whose author declared the sides Number
            org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () -> Lean.established(model,
                    "|x::same([1, 2]->at(5), 1.5)", "p::RT", connection));
        }
    }
}
