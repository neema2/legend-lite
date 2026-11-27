// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * NUMBERS ARE OF TWO FAMILIES (the user's ruling of 2026-11-27, "Yes I like the rule, keep ordering strict"; the rule
 * is the user's own, "exact with exact (int and decimal when fits) vs approx (float) only and let untyped literals
 * only when lossless"): exact (Integer, Decimal) and approximate (Float). Within a family a comparison is by value,
 * so an Integer and a Decimal compare as the numbers they are; across the two the writer converts -- a compile error
 * where both types are known, ordering included -- and a written literal reads as the other side's type only where
 * nothing is lost.
 */
class NumberFamiliesTest {

    private static final String MODEL = """
            ###Pure
            Class p::Party { id: Integer[1]; amount: Decimal[1]; ratio: Float[1]; }
            ###Relational
            Database p::DB ( Table PARTY (ID INTEGER PRIMARY KEY, AMOUNT DECIMAL(10, 2), RATIO DOUBLE) )
            ###Mapping
            Mapping p::M ( p::Party: Relational { ~mainTable [p::DB] PARTY id: PARTY.ID, amount: PARTY.AMOUNT,
                ratio: PARTY.RATIO } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 {
                    testDataSetupCSV: 'default\\nPARTY\\nID,AMOUNT,RATIO\\n1,1.00,1.0\\n2,2.50,2.5\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    private static Map<String, Object> values() {
        Map<String, Object> out = new LinkedHashMap<>();
        // an Integer against a Decimal: by value
        out.put("|1 == 1D", true);
        out.put("|1D == 1", true);
        out.put("|1 == 1.00D", true);
        out.put("|1 == 1.5D", false);
        out.put("|1 != 1D", false);
        out.put("|2 < 2.5D", true);
        out.put("|3 >= 3.00D", true);
        out.put("|3 > 3.00D", false);
        out.put("|let a = 4; let b = 4.0D; $a == $b;", true);
        out.put("|let a = 4; let b = 4.5D; $a >= $b;", false);
        // a written literal against a value of the other family reads as that value's type where nothing is lost
        out.put("|let a = 0.5; $a == 0.5;", true);
        out.put("|let a = 0.5D; $a == 0.5;", true);
        out.put("|let a = 2.0; $a > 1;", true);
        return out;
    }

    @Test
    void anIntegerAndADecimalCompareByValueOnEveryDatabase() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Object> c : values().entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    assertEquals(List.of(List.of(c.getValue())), r.rows(), backend.type() + ": " + c.getKey() + "\n"
                            + r.text());
                }
            }
            try (Connection connection = backend.open()) {
                // over columns: id an Integer, amount a Decimal(10, 2) -- row 1 holds 1 and 1.00, row 2 holds 2 and 2.50
                Lean.Result r = Lean.established(model, "|p::Party.all()->project(~[id: p|$p.id,"
                        + " same: p|$p.id == $p.amount, less: p|$p.id < $p.amount, one: p|$p.amount == 1])"
                        + "->sort(~id)", "p::RT", connection);
                assertEquals(List.of(List.of(1L, true, false, true), List.of(2L, false, true, false)), r.rows(),
                        backend.type() + "\n" + r.text());
            }
        }
    }

    @Test
    void anExactNumberAgainstAFloatIsStillACompileError() throws Exception {
        String model = MODEL.formatted(TestBackends.platform().type());
        for (String q : List.of("|1 == 1.0", "|1D == 1.0", "|1.5D < 1.5", "|let a = 1; let b = 1.0; $a == $b;",
                "|let a = 1D; let b = 1.0; $a <= $b;", "|p::Party.all()->filter(p|$p.amount == $p.ratio)->size()",
                "|p::Party.all()->filter(p|$p.id > $p.ratio)->size()", "|[1D, 2D]->contains(1.0)")) {
            try (Connection connection = TestBackends.platform().open()) {
                RuntimeException e = assertThrows(RuntimeException.class,
                        () -> Lean.established(model, q, "p::RT", connection), q);
                assertTrue(String.valueOf(e.getMessage()).contains("two values are equal only when")
                        || String.valueOf(e.getMessage()).contains("compared"), q + ": " + e.getMessage());
            }
        }
    }

    @Test
    void inAListTwoExactNumbersOfOneValueAreOneElement() throws Exception {
        Map<String, Object> cases = new LinkedHashMap<>();
        // a list of one type, a value of the other exact type
        cases.put("|[1, 2]->contains(2D)", true);
        cases.put("|[1, 2]->contains(2.5D)", false);
        cases.put("|2D->in([1, 2])", true);
        cases.put("|2.5D->in([1, 2])", false);
        cases.put("|[1.5D, 2D]->contains(2)", true);
        cases.put("|[1.5D, 2.5D]->contains(2)", false);
        cases.put("|[1, 2]->filter(x | $x > 0)->contains(2D)", true);
        cases.put("|[1, 2]->filter(x | $x > 0)->contains(2.5D)", false);
        cases.put("|[1.5D, 2D]->filter(x | $x > 0D)->contains(2)", true);
        // a mixed list: an Integer and a Decimal of one value are duplicates; a Float of that value is not
        cases.put("|[2, 2D, 2.0]->removeDuplicates()->size()", 2L);
        cases.put("|[1, 2D, 2.0]->removeDuplicates()->size()", 3L);
        // a sort places a tie of value by family, an exact number before a Float; two equal exact numbers keep the
        // order they were written in
        cases.put("|[3.0, 3D, 1]->sort()->map(x | $x->toString())->joinStrings(',')", "1,3D,3.0");
        cases.put("|[3D, 3.0, 3]->sort()->map(x | $x->toString())->joinStrings(',')", "3D,3,3.0");
        cases.put("|[3, 3.0, 3D]->sort()->map(x | $x->toString())->joinStrings(',')", "3,3D,3.0");
        // PROBES, to be pinned at what the run answers: which of two equal exact numbers max keeps, and what
        // arithmetic of a Decimal with a written Float gives
        cases.put("|[3, 3D]->max()->toString()", "?");
        cases.put("|[3D, 3]->max()->toString()", "?");
        cases.put("|(2.5D * 0.5)->toString()", "?");
        cases.put("|(2.5D * 2)->toString()", "?");
        List<String> wrong = new java.util.ArrayList<>();
        for (Map.Entry<String, Object> c : cases.entrySet()) {
            try (Connection connection = TestBackends.platform().open()) {
                String model = MODEL.formatted(TestBackends.platform().type());
                Object got;
                try {
                    got = Lean.established(model, c.getKey(), "p::RT", connection).rows();
                } catch (RuntimeException e) {
                    got = e.getClass().getSimpleName() + ": " + e.getMessage();
                }
                if (!List.of(List.of(c.getValue())).equals(got)) {
                    wrong.add(c.getKey() + " -> " + got + " (wanted " + c.getValue() + ")");
                }
            }
        }
        assertTrue(wrong.isEmpty(), "\n  " + String.join("\n  ", wrong));
    }
}
