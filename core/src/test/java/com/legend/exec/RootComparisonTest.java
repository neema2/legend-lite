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
 * A QUERY WHOSE ANSWER IS A COMPARISON OF TWO WHOLE SIDES (the edition program's step 6, the one equality,
 * 2026-11-27): each side is planned once as rows and the product's equality over two sides compares them
 * ({@code planner.RootComparison}, {@code lowering.SideEqualities}), the code a test's {@code assertEquals} is judged
 * by. So the answers below are the same on every in-process database: no list is compared as a value where a database
 * has no list equality, a computed empty list is empty, a Decimal is compared by its number, and a value whose type is
 * known only when the query runs is compared by the type it holds.
 */
class RootComparisonTest {

    private static final String MODEL = """
            ###Pure
            Class p::Party { id: Integer[1]; name: String[1]; }
            Class p::K { name: String[1]; }
            ###Relational
            Database p::DB ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(64)) )
            ###Mapping
            Mapping p::M ( p::Party: Relational { ~mainTable [p::DB] PARTY id: PARTY.ID, name: PARTY.NAME } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 {
                    testDataSetupCSV: 'default\\nPARTY\\nID,NAME\\n1,Meridian\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    private static Map<String, Boolean> cases() {
        Map<String, Boolean> out = new LinkedHashMap<>();
        // a computed list against a written one, in order
        out.put("|[1, 2]->filter(x | $x > 0) == [1, 2]", true);
        out.put("|[1, 2]->filter(x | $x > 1) == [1, 2]", false);
        out.put("|[2, 1]->filter(x | $x > 0) == [1, 2]", false);
        out.put("|!([1, 2]->filter(x | $x > 1) == [1, 2])", true);
        // a computed empty list is empty
        out.put("|[] == [1, 2]->filter(x | $x > 5)", true);
        out.put("|[1, 2]->filter(x | $x > 5) == [1, 2]->filter(x | $x > 7)", true);
        out.put("|[1] == [1, 2]->filter(x | $x > 5)", false);
        // one value against a list of one
        out.put("|1 == [1, 2]->filter(x | $x < 2)", true);
        out.put("|2 == [1, 2]->filter(x | $x < 2)", false);
        // a Decimal by its number, whatever its written scale
        out.put("|[1.0D, 10.125D]->filter(x | $x > 0D) == [1.000D, 10.125D]->filter(x | $x > 0D)", true);
        // strings and booleans
        out.put("|['a', 'b']->filter(x | $x != 'c') == ['a', 'b']", true);
        out.put("|[true, false]->filter(x | $x || true) == [true]", false);
        // a value typed only when the query runs is compared by the type it holds
        out.put("|[1, 2]->cast(@Number) == [1, 2]->cast(@Number)", true);
        out.put("|[1, 2]->cast(@Number) == [1.0, 2.0]->cast(@Number)", false);
        // an Integer side against a Decimal side: two exact numbers, by value (the user's ruling of 2026-11-27)
        out.put("|[1, 2]->filter(x | $x > 0) == [1.0D, 2.00D]", true);
        out.put("|[1, 2]->filter(x | $x > 0) == [1.0D, 2.50D]", false);
        out.put("|[1.0D, 2.00D]->filter(x | $x > 0D) == [1, 2]->filter(x | $x > 0)", true);
        out.put("|[1, 2]->cast(@Number) == [1D, 2D]->cast(@Number)", true);
        out.put("|[1, 2]->cast(@Number) == [1D, 2.5D]->cast(@Number)", false);
        // two values of one known type stay the plain comparison
        out.put("|1 == 1", true);
        out.put("|'a' == 'b'", false);
        return out;
    }

    @Test
    void aComparisonOfTwoWholeSidesAnswersTheSameOnEveryDatabase() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Boolean> c : cases().entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    assertEquals(List.of(List.of(c.getValue())), r.rows(), backend.type() + ": " + c.getKey() + "\n"
                            + r.text());
                }
            }
        }
    }

    /** Two tables (step 6, 2026-11-27): the same where their columns have the same names in the same order and their
     *  rows are the same in order, the comparison an {@code assertEquals} of two tables is judged by. Until then a
     *  query's {@code ==} read neither the columns' names nor the rows' order. */
    private static Map<String, Boolean> tables() {
        Map<String, Boolean> out = new LinkedHashMap<>();
        String ab = "#TDS\n id, s\n 1, x\n 2, y\n#";
        out.put("|" + ab + " == " + ab, true);
        out.put("|!(" + ab + " == " + ab + ")", false);
        // the rows in another order: another table
        out.put("|" + ab + " == #TDS\n id, s\n 2, y\n 1, x\n#", false);
        // a cell that differs; a row more
        out.put("|" + ab + " == #TDS\n id, s\n 1, x\n 2, z\n#", false);
        out.put("|" + ab + " == #TDS\n id, s\n 1, x\n#", false);
        // a column of another name, or a column more: never equal, by the types alone
        out.put("|#TDS\n id\n 1\n# == #TDS\n ix\n 1\n#", false);
        out.put("|!(#TDS\n id\n 1\n# == #TDS\n ix\n 1\n#)", true);
        out.put("|#TDS\n id\n 1\n# == #TDS\n id, s\n 1, x\n#", false);
        // a sorted table against the rows written in that order
        out.put("|#TDS\n id, s\n 2, y\n 1, x\n#->sort(~id->ascending()) == " + ab, true);
        out.put("|#TDS\n id, s\n 2, y\n 1, x\n#->sort(~id->descending()) == " + ab, false);
        // a store's rows, sorted, against written ones
        out.put("|p::Party.all()->project(~[id: p|$p.id, name: p|$p.name])->sort(~id->ascending())"
                + " == #TDS\n id, name\n 1, Meridian\n#", true);
        out.put("|p::Party.all()->project(~[id: p|$p.id, name: p|$p.name])->sort(~id->ascending())"
                + " == #TDS\n id, name\n 1, Other\n#", false);
        return out;
    }

    @Test
    void twoTablesAreTheSameByTheirColumnsAndTheirRowsInOrder() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Boolean> c : tables().entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    assertEquals(List.of(List.of(c.getValue())), r.rows(), backend.type() + ": " + c.getKey() + "\n"
                            + r.text());
                }
            }
        }
    }

    /** A column of Integers against a column of Decimals is compared by value (two exact numbers; the user's ruling
     *  of 2026-11-27); against a column of Floats never equal. */
    @Test
    void aTablesNumbersAreComparedByTheTwoFamilies() throws Exception {
        Map<String, Boolean> cases = new LinkedHashMap<>();
        String one = "#TDS\n id\n 1\n#";
        cases.put("|" + one + "->extend(~v: x|2) == " + one + "->extend(~v: x|2D)", true);
        cases.put("|" + one + "->extend(~v: x|2) == " + one + "->extend(~v: x|2.50D)", false);
        cases.put("|" + one + "->extend(~v: x|2.5D) == " + one + "->extend(~v: x|2.50D)", true);
        cases.put("|" + one + "->extend(~v: x|2) == " + one + "->extend(~v: x|2.0)", false);
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Boolean> c : cases.entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    assertEquals(List.of(List.of(c.getValue())), r.rows(), backend.type() + ": " + c.getKey() + "\n"
                            + r.text());
                }
            }
        }
    }

    /** A date built to the hour or the minute keeps how it prints (a text), and is compared by its instant (register
     *  S399) wherever the comparison stands: as a query's answer, and inside an expression, where until step 6 the
     *  database was handed the text against a TIMESTAMP and refused it. */
    @Test
    void aDateBuiltToTheHourIsComparedByItsInstant() throws Exception {
        Map<String, Object> cases = new LinkedHashMap<>();
        cases.put("|%2015-04-16T14 == date(2015, 4, 16, 14)", true);
        cases.put("|%2015-04-16T15 == date(2015, 4, 16, 14)", false);
        cases.put("|if(%2015-04-16T14 == date(2015, 4, 16, 14), |'same', |'other')", "same");
        cases.put("|if(date(2015, 4, 16, 14) == %2015-04-16T14, |'same', |'other')", "same");
        cases.put("|if(%2015-04-16T15 == date(2015, 4, 16, 14), |'same', |'other')", "other");
        cases.put("|if(%2015-04-16T14:30 == date(2015, 4, 16, 14, 30), |'same', |'other')", "same");
        cases.put("|if(%2015-04-16T14:30:00 == date(2015, 4, 16, 14, 30), |'same', |'other')", "same");
        List<String> wrong = new java.util.ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Object> c : cases.entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    if (!List.of(List.of(c.getValue())).equals(r.rows())) {
                        wrong.add(backend.type() + ": " + c.getKey() + " -> " + r.rows());
                    }
                } catch (RuntimeException e) {
                    wrong.add(backend.type() + ": " + c.getKey() + " -> " + e.getClass().getSimpleName() + ": "
                            + String.valueOf(e.getMessage()).lines().findFirst().orElse(""));
                }
            }
        }
        assertEquals(List.of(), wrong, "every case on every database");
    }

    /** Two maps are the same where they hold the same entries, whatever order the entries were written in (Pure's
     *  map; the comparison an assert of two maps is judged by). On the platform's database: H2 holds no map. */
    @Test
    void twoMapsAreTheSameWhateverOrderTheirEntriesWereWrittenIn() throws Exception {
        Map<String, Boolean> cases = new LinkedHashMap<>();
        cases.put("|newMap([pair('a', 1), pair('b', 2)]) == newMap([pair('b', 2), pair('a', 1)])", true);
        cases.put("|newMap([pair('a', 1), pair('b', 2)]) == newMap([pair('a', 1), pair('b', 2)])", true);
        cases.put("|newMap([pair('a', 1), pair('b', 2)]) == newMap([pair('a', 1), pair('b', 3)])", false);
        cases.put("|newMap([pair('a', 1), pair('b', 2)]) == newMap([pair('a', 1)])", false);
        cases.put("|!(newMap([pair('a', 1), pair('b', 2)]) == newMap([pair('b', 2), pair('a', 1)]))", false);
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        for (Map.Entry<String, Boolean> c : cases.entrySet()) {
            try (Connection connection = backend.open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(List.of(List.of(c.getValue())), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
    }

    /** One value read twice is one evaluation (2026-11-27): the objects a let's value built are the same objects at
     *  every reading of the let, so an object with no key is equal to itself, and to no other object. Until then each
     *  reading numbered its objects anew and the first two cases answered false. */
    @Test
    void anObjectWithNoKeyIsEqualToItselfAndToNoOther() throws Exception {
        Map<String, Boolean> cases = new LinkedHashMap<>();
        String xs = "|let xs = [1, 2]->map(x|^p::K(name=$x->toString())); ";
        cases.put(xs + "$xs->at(0) == $xs->at(0);", true);
        cases.put(xs + "$xs == $xs;", true);
        cases.put(xs + "$xs->at(0) == $xs->at(1);", false);
        cases.put("|^p::K(name='a') == ^p::K(name='a')", false);
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        List<String> wrong = new java.util.ArrayList<>();
        for (Map.Entry<String, Boolean> c : cases.entrySet()) {
            try (Connection connection = backend.open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                if (!List.of(List.of(c.getValue())).equals(r.rows())) {
                    wrong.add(c.getKey() + " -> " + r.rows());
                }
            } catch (RuntimeException e) {
                wrong.add(c.getKey() + " -> " + e.getClass().getSimpleName() + ": "
                        + String.valueOf(e.getMessage()).lines().findFirst().orElse(""));
            }
        }
        assertEquals(List.of(), wrong, "every case");
    }

    /** {@code ==} compares two tables in order: a side whose order the query does not define is refused, naming the
     *  two fixes, as {@code assertEquals} refuses it (the user, 2026-11-27). */
    @Test
    void anUnsortedTableIsRefusedUnderEquals() throws Exception {
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        try (Connection connection = backend.open()) {
            RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                    () -> Lean.established(model, "|p::Party.all()->project(~[id: p|$p.id]) == #TDS\n id\n 1\n#",
                            "p::RT", connection));
            org.junit.jupiter.api.Assertions.assertTrue(String.valueOf(e.getMessage()).contains("== compares two"
                    + " tables in order") && e.getMessage().contains("hasSameElements"), e.getMessage());
        }
    }

    /** {@code hasSameElements(a, b)} (the user, 2026-11-27: {@code ==} on two tables is strict, and a separate function
     *  compares them when the order does not matter): two lists' elements, or two tables' rows under the same column
     *  names, the same in ANY order -- each as often on one side as on the other, each pair by the one equality. The
     *  same answer on every database, collected so one run shows every case that is wrong. */
    @Test
    void hasSameElementsComparesTwoSidesInAnyOrder() throws Exception {
        Map<String, Boolean> cases = new LinkedHashMap<>();
        // lists: any order; duplicates count
        cases.put("|hasSameElements([1, 2, 3], [3, 1, 2])", true);
        cases.put("|[1, 2, 3]->hasSameElements([3, 1, 2])", true);
        cases.put("|hasSameElements([1, 2, 3]->filter(x | $x > 1), [3, 2])", true);
        cases.put("|hasSameElements([1, 1, 2], [1, 2, 2])", false);
        cases.put("|hasSameElements([1, 2], [1, 2, 3])", false);
        cases.put("|!hasSameElements([1, 2], [2, 1])", false);
        cases.put("|!hasSameElements([1, 2], [2, 3])", true);
        cases.put("|hasSameElements(['a', 'b'], ['b', 'a'])", true);
        // two empty sides; one empty side
        cases.put("|hasSameElements([1, 2]->filter(x | $x > 5), [3]->filter(x | $x > 5))", true);
        cases.put("|hasSameElements([1, 2]->filter(x | $x > 5), [3])", false);
        // an Integer against a Decimal: two exact numbers, by value; a Decimal by its number
        cases.put("|hasSameElements([1, 2], [2.0D, 1D])", true);
        cases.put("|hasSameElements([1.50D, 2D], [2.00D, 1.5D])", true);
        // values typed only when the query runs: by the type each holds
        cases.put("|hasSameElements([1, 2]->cast(@Number), [2, 1]->cast(@Number))", true);
        cases.put("|hasSameElements([1, 2]->cast(@Number), [2.0, 1.0]->cast(@Number))", false);
        // tables: the rows in any order, the columns' names the same
        String ab = "#TDS\n id, s\n 1, x\n 2, y\n#";
        cases.put("|hasSameElements(" + ab + ", #TDS\n id, s\n 2, y\n 1, x\n#)", true);
        cases.put("|hasSameElements(" + ab + ", #TDS\n id, s\n 2, y\n 1, z\n#)", false);
        cases.put("|hasSameElements(" + ab + ", #TDS\n id, s\n 2, y\n 1, x\n 1, x\n#)", false);
        cases.put("|hasSameElements(#TDS\n id\n 1\n#, #TDS\n ix\n 1\n#)", false);
        // a store's rows, read with no sort: the comparison == refuses (anUnsortedTableIsRefusedUnderEquals)
        cases.put("|hasSameElements(p::Party.all()->project(~[id: p|$p.id, name: p|$p.name]),"
                + " #TDS\n id, name\n 1, Meridian\n#)", true);
        cases.put("|let rows = p::Party.all()->project(~[id: p|$p.id]); hasSameElements($rows, #TDS\n id\n 2\n#);",
                false);
        List<String> wrong = new java.util.ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Boolean> c : cases.entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    if (!List.of(List.of(c.getValue())).equals(r.rows())) {
                        wrong.add(backend.type() + ": " + c.getKey() + " -> " + r.rows());
                    }
                } catch (RuntimeException e) {
                    wrong.add(backend.type() + ": " + c.getKey() + " -> " + e.getClass().getSimpleName() + ": "
                            + String.valueOf(e.getMessage()).lines().findFirst().orElse(""));
                }
            }
        }
        assertEquals(List.of(), wrong, "every case on every database");
    }

    /** A LIST whose order the query does not define is refused under {@code ==} as a table is (the third review of
     *  2026-11-27, M1: a store's rows mapped to a list had been compared in the order the database returned them,
     *  where an assert of the same side is refused); sorted it is compared, and {@code hasSameElements} takes it as it
     *  is. */
    @Test
    void anUnsortedListIsRefusedUnderEquals() throws Exception {
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        String names = "p::Party.all()->map(p | $p.name)";
        try (Connection connection = backend.open()) {
            RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                    () -> Lean.established(model, "|" + names + " == ['Meridian', 'Other']", "p::RT", connection));
            org.junit.jupiter.api.Assertions.assertTrue(String.valueOf(e.getMessage()).contains("== compares two"
                    + " lists in order") && e.getMessage().contains("hasSameElements"), e.getMessage());
        }
        Map<String, Boolean> answered = new LinkedHashMap<>();
        answered.put("|" + names + "->sort() == ['Meridian', 'Other']", false);
        answered.put("|hasSameElements(" + names + ", ['Meridian'])", true);
        answered.put("|hasSameElements(" + names + ", ['Meridian', 'Other'])", false);
        // (against one value an ordered and an order-free comparison agree: not refused, as the assert's rule)
        answered.put("|" + names + " == ['Meridian']", true);
        for (Map.Entry<String, Boolean> c : answered.entrySet()) {
            try (Connection connection = backend.open()) {
                Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                assertEquals(List.of(List.of(c.getValue())), r.rows(), c.getKey() + "\n" + r.text());
            }
        }
    }

    /** What {@code hasSameElements} refuses, each by name: two sides of two known types (the equality rule: different
     *  types never compare), a table against a list, and sides that read the row or element it stands in. */
    @Test
    void hasSameElementsRefusesTwoTypesAndAnyPlaceButTheAnswer() throws Exception {
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        Map<String, String> refused = new LinkedHashMap<>();
        // two types: the type-checker's refusal, by the words == has (the closing batch: it is checked where == is)
        refused.put("|hasSameElements([1, 2], ['a', 'b'])", "hasSameElements compares Integer with String");
        refused.put("|hasSameElements([1, 2], [1.0, 2.0])", "hasSameElements compares Integer with Float");
        refused.put("|if(hasSameElements([1, 2], ['a', 'b']), |1, |2)", "hasSameElements compares Integer with String");
        // two unrelated classes (the third review of 2026-11-27, M6)
        refused.put("|hasSameElements([^p::K(name='a')], [^p::Party(id=1, name='a')])",
                "hasSameElements compares p::K with p::Party");
        refused.put("|[1, 2]->map(x | hasSameElements([$x, 2], [2, 1]))", "hasSameElements over values of the row or element");
        refused.put("|hasSameElements(#TDS\n id\n 1\n#, [1])", "hasSameElements: a table against a side that is no table");
        for (Map.Entry<String, String> c : refused.entrySet()) {
            try (Connection connection = backend.open()) {
                RuntimeException e = org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class,
                        () -> Lean.established(model, c.getKey(), "p::RT", connection), c.getKey());
                org.junit.jupiter.api.Assertions.assertTrue(String.valueOf(e.getMessage()).contains(c.getValue()),
                        c.getKey() + " -> " + e.getMessage());
            }
        }
        // in a function's body it is the model's compile error, as == of two types is: no query need plan it
        List<String> errors = com.legend.Compiler.compileErrors(model
                + "\n###Pure\nfunction p::bad(): Boolean[1] { hasSameElements([1, 2], ['a', 'b']) }\n");
        org.junit.jupiter.api.Assertions.assertTrue(errors.stream()
                .anyMatch(e -> e.contains("hasSameElements compares Integer with String")), String.valueOf(errors));
        // two exact numbers are one family: compared by value, in any order
        try (Connection connection = backend.open()) {
            assertEquals(List.of(List.of(true)), Lean.established(model, "|hasSameElements([1, 2], [2.0D, 1D])",
                    "p::RT", connection).rows());
        }
    }

    /** ONE EQUALITY WHEREVER THE COMPARISON STANDS (the user, 2026-11-27: "why cant we do the work we promised to get
     *  to one equality?"): a comparison of two whole sides answers the same as a query's last statement, as an
     *  {@code if}'s condition and as an operand of {@code &&}. Inside an expression it is the same statement, read
     *  as one value ({@code planner.RootComparison.over}). Until then it was the database's own comparison of two
     *  list values there: measured 2026-11-27, two tables with other column names or rows in another order answered
     *  "the same" on DuckDB and SQLite, two maps written in another order "not the same", two Decimal lists of other
     *  written scales "not the same" on SQLite, and H2 answered a database error or refused. Where a database refuses
     *  the comparison as an answer (a map on H2 and SQLite), it refuses it inside an expression too, by the same
     *  name. Collected, so one run shows every case that differs. */
    @Test
    void aComparisonAnswersTheSameInsideAnExpression() throws Exception {
        java.util.Set<String> comparisons = new java.util.LinkedHashSet<>();
        for (String q : cases().keySet()) {
            comparisons.add(q.substring(1));
        }
        for (String q : tables().keySet()) {
            comparisons.add(q.substring(1));
        }
        comparisons.add("newMap([pair('a', 1), pair('b', 2)]) == newMap([pair('b', 2), pair('a', 1)])");
        comparisons.add("newMap([pair('a', 1)]) == newMap([pair('a', 2)])");
        comparisons.add("[1.0D, 2.5D]->filter(x | $x > 0D) == [1.00D, 2.50D]");
        comparisons.add("[1, 'a']->filter(x | true) == [1, 'a']");
        comparisons.add("[1, 'a']->filter(x | true) == [1, 'b']");
        comparisons.add("hasSameElements([1, 2, 3], [3, 1, 2])");
        comparisons.add("hasSameElements([1, 1, 2], [1, 2, 2])");
        comparisons.add("hasSameElements(#TDS\n id, s\n 1, x\n 2, y\n#, #TDS\n id, s\n 2, y\n 1, x\n#)");
        List<String> differs = new java.util.ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (String comparison : comparisons) {
                String root = outcome(backend, model, "|" + comparison);
                for (String inside : List.of("|if(" + comparison + ", |true, |false)",
                        "|(" + comparison + ") && true")) {
                    String got = outcome(backend, model, inside);
                    if (!root.equals(got)) {
                        differs.add(backend.type() + ": " + comparison.replace('\n', ' ') + " -> " + root
                                + ", but " + inside.replace('\n', ' ') + " -> " + got);
                    }
                }
            }
        }
        assertEquals(List.of(), differs, "the same comparison, the same answer");
    }

    /** A SIDE THAT READS A {@code let} (the closing batch, 2026-11-27: one entry). The lowering asks the planner
     *  for the body's last statement and for a comparison inside an expression alike ({@code lowering
     *  .WholeComparisons}), and each side is planned with the lets it reads -- a list's, a sorted table's -- so the
     *  two answer the same. Until then the last statement was planned by a pass of the planner's own, before the
     *  lowering, and a comparison inside an expression by the lowering's call: two entries for one comparison. */
    @Test
    void aSideThatReadsALetAnswersTheSameWhereverTheComparisonStands() throws Exception {
        String list = "let a = [1, 2]->filter(x | $x > 0); ";
        String table = "let t = #TDS\n id, s\n 1, x\n 2, y\n#->sort(~id->ascending()); ";
        Map<String, Object> cases = new LinkedHashMap<>();
        cases.put("{| " + list + "$a == [1, 2]; }", true);
        cases.put("{| " + list + "!($a == [2, 1]); }", true);
        cases.put("{| " + list + "if($a == [1, 2], |'same', |'other'); }", "same");
        cases.put("{| " + list + "if($a == [2, 1], |'same', |'other'); }", "other");
        cases.put("{| " + table + "$t == $t; }", true);
        cases.put("{| " + table + "if($t == $t, |'same', |'other'); }", "same");
        List<String> wrong = new java.util.ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Object> c : cases.entrySet()) {
                String got = outcome(backend, model, c.getKey());
                if (!String.valueOf(List.of(List.of(c.getValue()))).equals(got)) {
                    wrong.add(backend.type() + ": " + c.getKey().replace('\n', ' ') + " -> " + got);
                }
            }
        }
        assertEquals(List.of(), wrong, "every case on every database");
    }

    /** WHERE LITE CANNOT TELL A SIDE'S ORDER IS FIXED, IT SAYS SO (the closing batch; the user, 2026-11-27: "maybe
     *  okay if sometimes says \"use hasSameElements\" when it doesn't know"). Store rows passed through a function
     *  the order rule does not name -- {@code removeDuplicates}, {@code reverse}, {@code tail}, an {@code if} of two
     *  reads -- are in arrival order still, and {@code ==} over them is refused, naming the two fixes. Until then the
     *  rule answered "ordered" for every function it did not name, and the comparison was made in the order the rows
     *  arrived. The same functions over a written or a sorted list are compared: their order is the language's. */
    @Test
    void aSideWhoseOrderLiteCannotTellIsRefusedUnderEquals() throws Exception {
        String names = "p::Party.all()->map(p | $p.name)";
        List<String> refused = List.of(
                "|" + names + "->removeDuplicates() == ['A', 'B']",
                "|" + names + "->reverse() == ['A', 'B']",
                "|" + names + "->tail() == ['A', 'B']",
                "|if(" + names + "->reverse() == ['A', 'B'], |'same', |'other')");
        Map<String, Object> compared = new LinkedHashMap<>();
        compared.put("|[3, 1, 2]->filter(x | $x > 0)->reverse() == [2, 1, 3]", true);
        compared.put("|[1, 1, 2]->filter(x | $x > 0)->removeDuplicates() == [1, 2]", true);
        compared.put("|[1, 2, 3]->filter(x | $x > 0)->tail() == [2, 3]", true);
        compared.put("|" + names + "->sort()->reverse() == ['B', 'A']", false);
        compared.put("|" + names + "->removeDuplicates()->sort() == ['A', 'B']", false);
        List<String> wrong = new java.util.ArrayList<>();
        var backend = TestBackends.platform();
        String model = MODEL.formatted(backend.type());
        for (String q : refused) {
            try (Connection connection = backend.open()) {
                wrong.add(q + " -> " + Lean.established(model, q, "p::RT", connection).rows());
            } catch (com.legend.error.ModelException e) {
                if (!String.valueOf(e.getMessage()).contains("hasSameElements")) {
                    wrong.add(q + " -> " + e.getMessage());
                }
            }
        }
        for (Map.Entry<String, Object> c : compared.entrySet()) {
            String got = outcome(backend, model, c.getKey());
            if (!String.valueOf(List.of(List.of(c.getValue()))).equals(got)) {
                wrong.add(c.getKey() + " -> " + got);
            }
        }
        // a query's PARAMETER is a list its caller gives, in the caller's order: never refused for its order (it was:
        // a variable no let binds had been taken for an execution frame's rows)
        try {
            com.legend.Compiler.query(com.legend.Compiler.compileModel(model),
                    "{names: String[*] | $names == ['a', 'b']}").plan("p::RT");
        } catch (RuntimeException e) {
            if (String.valueOf(e.getMessage()).contains("order")) {
                wrong.add("a parameter's side -> " + e.getMessage());
            }
        }
        assertEquals(List.of(), wrong, "every case");
    }

    /** WHERE NO STATEMENT OF ITS OWN ANSWERS A COMPARISON, AND THE DATABASE'S OWN IS NOT THE RULE, IT IS REFUSED BY
     *  NAME (the closing batch; the user, 2026-11-27: "per row refused okay"). A map compared where it reads the
     *  element it stands in was the database's own map comparison (its entries by the order they were written in);
     *  two lists of Decimals there, on a database that carries a list as JSON, were compared as their texts (1.0
     *  against 1.00 not the same). The fix is the database follow-up's; until then never a wrong answer. */
    @Test
    void aComparisonNoStatementAnswersIsRefusedByNameWhereTheDatabasesOwnIsNotTheRule() throws Exception {
        List<String> wrong = new java.util.ArrayList<>();
        String maps = "|[1, 2]->map(x | newMap([pair('a', $x)]) == newMap([pair('a', 1)]))";
        String decimals = "|[1, 2]->map(x | [1.0D * $x, 2.0D] == [1.00D, 2.00D])";
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            try (Connection connection = backend.open()) {
                wrong.add(backend.type() + ": " + maps + " -> " + Lean.established(model, maps, "p::RT", connection)
                        .rows());
            } catch (RuntimeException e) {
                if (!(e instanceof com.legend.error.NotImplementedException
                        || e instanceof com.legend.sql.DialectCapability)) {
                    wrong.add(backend.type() + ": " + maps + " -> " + e);
                }
            }
            if ("SQLite".equals(String.valueOf(backend.type()))) {
                try (Connection connection = backend.open()) {
                    wrong.add("SQLite: " + decimals + " -> " + Lean.established(model, decimals, "p::RT", connection)
                            .rows());
                } catch (RuntimeException e) {
                    if (!String.valueOf(e.getMessage()).contains("has no typed form")) {
                        wrong.add("SQLite: " + decimals + " -> " + e);
                    }
                }
            }
        }
        assertEquals(List.of(), wrong, "each refused by name");
    }

    /** {@code query}'s rows, or the name of what refused it. */
    private static String outcome(com.legend.testbackend.TestBackend backend, String model, String query)
            throws Exception {
        try (Connection connection = backend.open()) {
            return String.valueOf(Lean.established(model, query, "p::RT", connection).rows());
        } catch (RuntimeException e) {
            return e.getClass().getSimpleName();
        }
    }
}
