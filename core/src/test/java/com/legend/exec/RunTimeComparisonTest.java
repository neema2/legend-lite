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
 * lite's own route two values are equal only when they have the same type and the same value, so a function written
 * for {@code Number} or {@code Any} answers by the types its arguments have; an ordering is by value, then by type
 * (an Integer below a Float below a Decimal); and a number literal the function's author wrote reads as the type of
 * the value it is written against.
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
        // two values of two types are never equal; of one type, by value
        out.put("|x::same(1, 1.0)", false);
        out.put("|x::same(1.0, 1)", false);
        out.put("|x::same(1, 1D)", false);
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
        // a position among values of another type: none
        out.put("|x::whereIn(1.0, [1, 2])", -1L);
        out.put("|x::whereIn(2, [1, 2])", 1L);
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
}
