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
            Class p::Loose { n: Number[1]; k: Number[1]; m: Number[1]; }
            function p::where<T>(r: Relation<T>[1], f: Function<{T[1]->Boolean[1]}>[1]): Relation<T>[1] { $r->filter($f) }
            ###Relational
            Database p::DB ( Table PARTY (ID INTEGER PRIMARY KEY, AMOUNT DECIMAL(10, 2), RATIO DOUBLE) )
            ###Mapping
            Mapping p::M ( p::Party: Relational { ~mainTable [p::DB] PARTY id: PARTY.ID, amount: PARTY.AMOUNT,
                ratio: PARTY.RATIO }
              p::Loose: Relational { ~mainTable [p::DB] PARTY n: PARTY.ID, k: PARTY.AMOUNT, m: PARTY.RATIO } )
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
    void numbersTypedOnlyByTheirColumnsAreRefusedAcrossTheTwoFamilies() throws Exception {
        // the user's ruling of 2026-11-27 ("great do it"): a property declared Number has no type its author wrote,
        // but its column holds one. An exact number's column against a Float's can never be equal, so the query is
        // refused where it is planned, as two known types are where it is compiled -- never false for every row. A
        // written literal still reads as the column's type, and an Integer against a Decimal is by value.
        // n is over an INTEGER column, k over a DECIMAL(10, 2), m over a DOUBLE: rows (1, 1.00, 1.0), (2, 2.50, 2.5)
        String model = MODEL.formatted(TestBackends.platform().type());
        Map<String, Object> holds = new LinkedHashMap<>();
        holds.put("|p::Loose.all()->filter(x|$x.n == $x.k)->project(~[n: x|$x.n])", List.of(List.of(1L)));
        holds.put("|p::Loose.all()->filter(x|$x.n < $x.k)->project(~[n: x|$x.n])", List.of(List.of(2L)));
        holds.put("|p::Loose.all()->filter(x|$x.m == 1)->project(~[n: x|$x.n])", List.of(List.of(1L)));
        holds.put("|p::Loose.all()->filter(x|$x.m > 2)->project(~[n: x|$x.n])", List.of(List.of(2L)));
        holds.put("|p::Loose.all()->filter(x|$x.m->in([1, 7]))->project(~[n: x|$x.n])", List.of(List.of(1L)));
        for (Map.Entry<String, Object> c : holds.entrySet()) {
            try (Connection connection = TestBackends.platform().open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(c.getValue(), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
        Map<String, String> refused = new LinkedHashMap<>();
        refused.put("|p::Loose.all()->filter(x|$x.n == $x.m)->project(~[n: x|$x.n])", "== compares Integer with Float");
        refused.put("|p::Loose.all()->filter(x|$x.m == $x.k)->project(~[n: x|$x.n])", "== compares Float with Decimal");
        refused.put("|p::Loose.all()->filter(x|$x.n < $x.m)->project(~[n: x|$x.n])", "< compares Integer with Float");
        refused.put("|p::Loose.all()->filter(x|$x.n == 1.5)->project(~[n: x|$x.n])", "== compares Integer with Float");
        refused.put("|p::Loose.all()->filter(x|$x.n->in([1.5, 2.5]))->project(~[n: x|$x.n])",
                "in compares Integer with Float");
        // an upcast of a value already typed only by its column says nothing new: refused as without it (the third
        // review of 2026-11-27, M2: it had escaped, and the database compared 1 with 1.0)
        refused.put("|p::Loose.all()->filter(x|$x.n->cast(@Number) == $x.m)->project(~[n: x|$x.n])",
                "== compares Integer with Float");
        for (Map.Entry<String, String> c : refused.entrySet()) {
            try (Connection connection = TestBackends.platform().open()) {
                RuntimeException e = assertThrows(RuntimeException.class,
                        () -> Lean.established(model, c.getKey(), "p::RT", connection), c.getKey());
                assertTrue(String.valueOf(e.getMessage()).contains(c.getValue())
                        && e.getMessage().contains("the types of the columns its two sides read"),
                        c.getKey() + ": " + e.getMessage());
            }
        }
    }

    /** A comparison the query's author writes inside a lambda handed to a function generic over a table's columns is
     *  still the query's own (the outside audit of 2026-11-27, MEDIUM 12): the function's body is typed into the call
     *  with the lambda in it, and until this fix the lambda was typed there as the function's code -- its comparison
     *  of two columns' types skipped the planning-time refusal, and an upcast in it was the database's comparison.
     *  Each case answers as the same lambda does in a plain {@code filter}. */
    @Test
    void aLambdaHandedToAGenericFunctionIsStillTheQuerys() throws Exception {
        String model = MODEL.formatted(TestBackends.platform().type());
        String loose = "|p::Loose.all()->project(~[n: x|$x.n, m: x|$x.m])";
        // two values typed only by their columns, an INTEGER's against a DOUBLE's: refused where the query is planned
        for (String q : List.of(loose + "->filter(r|$r.n == $r.m)", loose + "->p::where(r|$r.n == $r.m)")) {
            try (Connection connection = TestBackends.platform().open()) {
                RuntimeException e = assertThrows(RuntimeException.class,
                        () -> Lean.established(model, q, "p::RT", connection), q);
                assertTrue(String.valueOf(e.getMessage()).contains("== compares Integer with Float"),
                        q + ": " + e.getMessage());
            }
        }
        // an upcast the query's author wrote: answered by the types the two values hold (an Integer and a Float are
        // never equal), not by the database's comparison of 1 and 1.0
        String held = "$r.n->toOne()->cast(@Number) == $r.f->toOne()->cast(@Number)";
        for (String q : List.of("|#TDS\nn, f\n1, 1.0\n#->filter(r|" + held + ")->size()",
                "|#TDS\nn, f\n1, 1.0\n#->p::where(r|" + held + ")->size()")) {
            try (Connection connection = TestBackends.platform().open()) {
                Lean.Result r = Lean.established(model, q, "p::RT", connection);
                assertEquals(List.of(List.of(0L)), r.rows(), q + "\n" + r.text());
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
        // max of two equal exact numbers keeps the one whose type the list holds first (the value is the same)
        cases.put("|[3, 3D]->max()->toString()", "3");
        cases.put("|[3D, 3]->max()->toString()", "3D");
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

    /** A Float literal written into a Decimal's sum, difference or product is the Decimal its digits write (the user,
     *  2026-11-27: "shouldn't this match comparison?", "good let's do it"): the arithmetic is a Decimal's by Pure's
     *  rule, and the database computes it as one. Until then the database computed a double: a DECIMAL(10,2) column's
     *  0.10 times 0.1 answered 0.010000000000000002. A Float VALUE in the arithmetic keeps it a double's. */
    @Test
    void aFloatLiteralInADecimalsArithmeticIsReadAsADecimal() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            try (Connection connection = backend.open()) {
                // the model's rows: amounts 1.00 and 2.50 (as doubles, 0.1 * 1.0 * 3 is 0.30000000000000004 and
                // 0.1 * 2.5 * 3 is 0.7500000000000001: column d tells the two arithmetics apart)
                Lean.Result r;
                try {
                    r = Lean.established(model, "|p::Party.all()->project(~[id: p|$p.id, a: p|$p.amount * 0.1,"
                            + " b: p|$p.amount + 0.2, c: p|$p.amount - 0.1, d: p|0.1 * $p.amount * 3,"
                            + " e: p|$p.amount * -0.1])->sort(~id->ascending())", "p::RT", connection);
                } catch (RuntimeException e) {
                    // SQLite holds no exact decimal and refuses a Decimal's arithmetic by name, never approximating
                    // it (D2a, 2026-11-17): as before this rule, whatever the literal reads as
                    if (String.valueOf(e.getMessage()).contains("reached SQLite, which holds no exact")) {
                        continue;
                    }
                    throw e;
                }
                List<List<String>> got = r.rows().stream().map(row -> row.stream().map(c -> c instanceof Number n
                        ? new java.math.BigDecimal(n.toString()).stripTrailingZeros().toPlainString()
                        : String.valueOf(c)).toList()).toList();
                assertEquals(List.of(List.of("1", "0.1", "1.2", "0.9", "0.3", "-0.1"),
                        List.of("2", "0.25", "2.7", "2.4", "0.75", "-0.25")), got, backend.type() + "\n" + r.text());
                // a Float value among the operands: a double's arithmetic, as before (0.5 is exact in a double)
                Lean.Result f = Lean.established(model, "|p::Party.all()->project(~[id: p|$p.id,"
                        + " a: p|$p.ratio * 0.5])->sort(~id->ascending())", "p::RT", connection);
                assertEquals(2, f.rows().size(), backend.type() + "\n" + f.text());
            }
        }
    }
}
