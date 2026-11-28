// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE LIFT (B3 step 2b, the user's option (B) of 2026-11-24): a relation's printed side is read before the body through
 * lite's public test door, printed by the edge's printer and substituted as a String literal; lite's judge compares.
 * Pinned with a stand-in printer (B2b's legend-pure printer plugs into the same slot).
 */
class UpstreamPrintsTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";

    private static final String REL = "#TDS\n  id, name\n  1, A\n  2, B\n#";

    private static com.legend.compiler.element.ModelContext ctx() {
        return com.legend.Compiler.compileModel(com.legend.testbackend.StorelessRuntime.with(MODEL,
                com.legend.database.Databases.PLATFORM));
    }

    /** The first printed side in {@code body}'s statements, or null. */
    private static PrintedRelations.Side first(com.legend.TypedQuery q) {
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>(q.body());
        while (!work.isEmpty()) {
            TypedSpec n = work.pop();
            PrintedRelations.Side p = PrintedRelations.side(n);
            if (p != null) {
                return p;
            }
            work.addAll(n.children());
        }
        return null;
    }

    @Test
    @DisplayName("the printed sides: a relation's toString and legend-engine's toCSV, each with the literal replaces"
            + " after it, in order; nothing else")
    void theShapes() {
        com.legend.compiler.element.ModelContext ctx = ctx();
        PrintedRelations.Side p = first(com.legend.Compiler.query(ctx,
                "|let r = " + REL + "; assertEquals('x', $r->toString()->replace('\\n', ',')->replace('A', 'a'));"));
        assertNotNull(p);
        assertEquals(PureRelationTextWriter.Form.TO_STRING, p.form());
        assertEquals(List.of(new PrintTail.Replace("\n", ","), new PrintTail.Replace("A", "a")),
                p.tail());
        // a String's toString, and a replace whose argument is not a literal, are not printed sides
        assertNull(first(com.legend.Compiler.query(ctx, "|assertEquals('x', 'x'->toString());")));
        assertNull(first(com.legend.Compiler.query(ctx,
                "|let s = 'n'; assertEquals('x', 'x'->replace($s, ','));")));
    }

    @Test
    @DisplayName("a printed side is read through the public door on the session given, printed, and substituted: the"
            + " read is the lets before it and the relation; the judge then compares the literal")
    void readPrintedAndSubstituted() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        String body = "|let r = " + REL + "; assertEquals('2 rows', $r->toString());";
        com.legend.TypedQuery q = com.legend.Compiler.query(ctx, body);
        List<Integer> rowsRead = new ArrayList<>();
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            // the stand-in printer counts the rows the read answers
            UpstreamPrints.Printer printer = (side, read, r) -> {
                assertEquals(1, read.body().size(), "the read: the relation, its let's value in its place");
                int[] n = {0};
                r.read(read, new com.legend.result.Output.Rows(new com.legend.result.ResultSink() {
                    @Override
                    public void columns(List<com.legend.executionplan.ExecutionPlan.Column> columns) {
                    }

                    @Override
                    public void batch(com.legend.result.Batch batch) {
                        n[0] += batch.size();
                    }

                    @Override
                    public void end() {
                    }
                }));
                rowsRead.add(n[0]);
                return n[0] + " rows";
            };
            com.legend.TypedQuery read = UpstreamPrints.read(q, false, reads, printer);
            assertEquals(List.of(2), rowsRead);
            TypedSpec assertCall = read.body().get(1);
            assertTrue(assertCall.children().stream().anyMatch(k -> k instanceof TypedCString s
                    && s.value().equals("2 rows")), "the side substituted: " + assertCall);
            // the substituted body runs and passes on lite's judge
            com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
        }
    }

    @Test
    @DisplayName("a Float's print and a print's tail (the edition program's step 8, 2026-11-26): a value side is read and"
            + " printed by legend-pure's rule in Java, and the calls of the tail's closed set after a print are applied"
            + " in Java too, the side substituted by a literal of the tail's type")
    void aFloatsPrintAndAPrintsTailAreJavas() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            // the read runs in the product's style here: the product's own text of this Float is 0.0, so the texts
            // below are the Java printer's
            java.util.Map<String, Object> bodies = new java.util.LinkedHashMap<>();
            bodies.put("|assertEquals('0.000000013421', (1.3421e-8 * 1.0)->toString());", "0.000000013421");
            bodies.put("|assertEquals(14, (1.3421e-8 * 1.0)->toString()->length());", 14L);
            // + with literal Strings, a step of the tail since 2026-11-28
            bodies.put("|assertEquals('<0.000000013421>', '<' + (1.3421e-8 * 1.0)->toString() + '>');",
                    "<0.000000013421>");
            bodies.put("|let r = " + REL + "; assertEquals(31, $r->toString()->length());", 31L);
            bodies.put("|let r = " + REL + "; assert($r->toString()->replace('\n', ';')->contains(';   1,A;'));",
                    true);
            bodies.put("|let r = " + REL + "; assertEquals(0, $r->toString()->trim()->indexOf('#TDS'));", 0L);
            for (var e : bodies.entrySet()) {
                com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, e.getKey());
                com.legend.TypedQuery read = UpstreamPrints.read(typed, false, reads);
                assertTrue(read != typed, e.getKey());
                // the print and its tail are gone from the body: no toString call is left for the database
                assertEquals(0, toStrings(read), "the print is read before the body, its answer " + e.getValue()
                        + ": " + e.getKey());
                com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
        }
    }

    /** A computed DateTime's print at an assert's own argument is read and printed in Java, as a Float's is (the
     *  edition program's step 8, 2026-11-26); one that reads the clock is left as written: read as a query of its
     *  own it would draw its own instant, where the assert's statement draws one for both its sides (the independent
     *  review of that day). A literal's is left too: its written text is the lowering's constant. */
    @Test
    void aComputedDateTimesPrintIsJavas_andOneThatReadsTheClockIsLeft() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        String computed = "%2014-01-01T10:20:30.250+0000->adjust(1, meta::pure::functions::date::DurationUnit.HOURS)"
                + "->cast(@DateTime)";
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            for (String body : List.of("|assertEquals('2014-01-01T11:20:30.25+0000', " + computed + "->toString());",
                    "|assertEquals(27, " + computed + "->toString()->length());",
                    // through its let, with no other print in the body (the outside audit of slices 3 and 4, its B1:
                    // the door looked for a print with no lets in hand and returned this body untouched)
                    "|let d = " + computed + "; assertEquals('2014-01-01T11:20:30.25+0000', $d->toString());")) {
                com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, body);
                com.legend.TypedQuery read = UpstreamPrints.read(typed, false, reads);
                assertTrue(read != typed, body);
                assertEquals(0, toStrings(read), body);
                com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
            for (String left : List.of(
                    "|assertEquals(now()->toString(), now()->toString());",
                    "|let n = now(); assertEquals($n->adjust(1, meta::pure::functions::date::DurationUnit.HOURS)"
                            + "->cast(@DateTime)->toString(), 'x');",
                    "|assertEquals('2014-01-01T00:00:00.000+0000', %2014-01-01T00:00:00.000+0000->toString());")) {
                com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, left);
                assertTrue(UpstreamPrints.read(typed, false, reads) == typed, left);
            }
        }
    }

    /** A LAST STEP'S FORMAT HAS ITS COMPUTED FLOATS UNDER %s AND %r PRINTED IN JAVA (the user's ruling of 2026-11-28
     *  on decision D7): each is read as a value's print and put back as its text, so the template is filled with
     *  legend-pure's digits -- {@code 0.3333333333333333} and {@code 0.000000015}, where lite's own texts of the two
     *  values are {@code 0.333333} and {@code 0.0}. A Float under {@code %f}, a Float literal, a format the program
     *  goes on computing with, and an argument from an unrepeatable source are left as written, nothing read. */
    @Test
    void aLastStepsFormatHasItsComputedFloatsPrintedInJava() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            for (String body : List.of(
                    "|assertEquals('0.3333333333333333', format('%r', [(1.0 / 3.0)]));",
                    "|assertEquals('0.000000015', format('%s', [(1.5e-8 * 1.0)]));",
                    "|assertEquals('0.3333333333333333 is 0.000000015 of 3 (x)',"
                            + " format('%s is %r of %d (%s)', [(1.0 / 3.0), (1.5e-8 * 1.0), 3, 'x']));",
                    "|let x = 1.0 / 3.0; assertEquals('0.3333333333333333;', format('%r;', [$x]));")) {
                com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, body);
                com.legend.TypedQuery read = UpstreamPrints.read(typed, false, reads);
                assertTrue(read != typed, body);
                com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
            // a query's last statement, as a query door reads it: the same texts
            com.legend.TypedQuery query = com.legend.Compiler.query(ctx, "|format('%r', [(1.0 / 3.0)])");
            com.legend.TypedQuery read = UpstreamPrints.readLastFormat(query, reads);
            assertTrue(read != query);
            PrintedFormats.Read filled = PrintedFormats.of(read.body().get(0));
            assertEquals("%s", filled.template().value());
            assertEquals("0.3333333333333333", ((TypedCString) filled.args().get(0)).value());
        }
        UpstreamPrints.Reads never = (query, answer) -> {
            throw new AssertionError("nothing is read: " + query.body());
        };
        for (String left : List.of(
                "|assertEquals('0.333333', format('%f', [(1.0 / 3.0)]));",
                "|assertEquals('0.33', format('%.2f', [(1.0 / 3.0)]));",
                "|assertEquals('1.5', format('%s', [1.5]));",
                "|assertEquals(8, format('%s', [(1.0 / 3.0)])->length());",
                "|assertEquals('0.333333;', format('%s', [(1.0 / 3.0)]) + ';');",
                "|assertEquals('0.5', format('%s', [(random() * 1.0)]));")) {
            com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, left);
            assertSame(typed, UpstreamPrints.read(typed, false, never), left);
            com.legend.TypedQuery last = com.legend.Compiler.query(ctx,
                    "|" + left.substring(left.indexOf("format("), left.lastIndexOf(");")));
            assertSame(last, UpstreamPrints.readLastFormat(last, never), left);
        }
        // a template that is no literal is not read
        com.legend.TypedQuery computedTemplate = com.legend.Compiler.query(ctx,
                "|let t = '%s'; assertEquals('0.333333', format($t, [(1.0 / 3.0)]));");
        assertSame(computedTemplate, UpstreamPrints.read(computedTemplate, false, never));
    }

    /** A Float's print the program goes on computing with is left to the database (tenet 1 as the user amended it on
     *  2026-11-25; the independent review of step 8's slice 2, 2026-11-26): joined to a text, bound by a let, in a list,
     *  or under a call outside the tail's set, it is no last step, and the body is the door's as it was. */
    @Test
    @DisplayName("a Float's print used further is not read: only an assert's own argument is a last step")
    void aFloatsPrintUsedFurtherIsLeftToTheDatabase() {
        com.legend.compiler.element.ModelContext ctx = ctx();
        UpstreamPrints.Reads never = (query, answer) -> {
            throw new AssertionError("nothing is read: " + query.body());
        };
        for (String body : List.of(
                // (joined to a LITERAL text it is a step of the tail since 2026-11-28, and lifted: below)
                "|assertEquals('3.0;4', (1.5 * 2.0)->toString() + ';' + (2 + 2)->toString());",
                "|let s = (1.5 * 2.0)->toString(); assertEquals('3.0', $s);",
                "|assertEquals(['3.0'], [(1.5 * 2.0)->toString()]);",
                "|assertEquals('3.0', (1.5 * 2.0)->toString()->toUpper());",
                "|assert((1.5 * 2.0)->toString()->length() > 2);")) {
            com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, body);
            org.junit.jupiter.api.Assertions.assertSame(typed, UpstreamPrints.read(typed, false, never), body);
        }
    }

    /** A VALUE WHOSE SOURCE MAY ANSWER ANOTHER VALUE ON A SECOND READING IS NOT LIFTED (the independent audit of step
     *  8's slice 2, 2026-11-26, its S2): a printed side is read as a query of its own before the body runs, so a draw
     *  ({@code random()}), the clock, or a row taken from rows whose order the program does not define (here past a
     *  grouping: the order guard a printed relation is held to) would be read twice, the print's reading and the
     *  body's. Such a Float's print is left as written, the database's, and nothing is read. */
    @Test
    void aValueFromAnUnrepeatableSourceIsLeftToTheDatabase() {
        com.legend.compiler.element.ModelContext ctx = ctx();
        UpstreamPrints.Reads never = (query, answer) -> {
            throw new AssertionError("nothing is read: " + query.body());
        };
        String grouped = "let g = " + REL + "->groupBy(~name, ~n: x | $x.id : y | $y->sum());";
        for (String body : List.of(
                "|let x = random(); assertEquals($x->toString(), $x->toString());",
                "|assertEquals('0.5', (random() * 1.0)->toString());",
                // a draw inside a lambda is still a draw
                "|assertEquals('0.5', ([1.0]->map(x|$x * random())->at(0))->toString());",
                // a value taken by position from rows past a grouping: their order is the database's
                "|" + grouped + " assertEquals('1.5', ($g->map(r|$r.n->toOne() * 1.5)->at(0))->toString());",
                // ...by a limit, a take or a slice, which the typer writes as nodes of their own (the review of the
                // re-check's fixes, 2026-11-26: a list of natives never matched them)
                "|" + grouped + " assertEquals('1.5', $g->map(r|$r.n->toOne() * 1.5)->limit(1)->toOne()->toString());",
                "|" + grouped + " assertEquals('1.5', $g->map(r|$r.n->toOne() * 1.5)->take(1)->toOne()->toString());",
                "|" + grouped + " assertEquals('1.5', $g->map(r|$r.n->toOne() * 1.5)->slice(0, 1)->toOne()->toString());",
                // ...by a pick inside a lambda, which reads the same rows
                "|" + grouped + " assertEquals('1.5', [1]->map(i|$g->map(r|$r.n->toOne() * 1.5)->at(0))->toOne()"
                        + "->toString());",
                // a Float's sum is its rows' order's
                "|" + grouped + " assertEquals('4.5', $g->map(r|$r.n->toOne() * 1.5)->sum()->toString());",
                // a text of unordered rows is written in the order they arrive: a value parsed from one is that
                // order's (the second independent review of slices 3 and 4, its finding 3: makeString, a relation's
                // toString and toCSV were taken for order-free, being natives the order view reads through)
                "|" + grouped + " assertEquals('12.0', $g->map(r|$r.n->toOne())->makeString()->parseFloat()"
                        + "->toString());",
                "|" + grouped + " assertEquals('1.0', $g->toString()->length()->toFloat()->toString());")) {
            com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, body);
            org.junit.jupiter.api.Assertions.assertSame(typed, UpstreamPrints.read(typed, false, never), body);
        }
        // a user's function in the operand: its body is not in sight at the door and may draw
        com.legend.compiler.element.ModelContext drawing = com.legend.Compiler.compileModel(
                com.legend.testbackend.StorelessRuntime.with(MODEL + "function t::draw(): Float[1] { random() }\n",
                        com.legend.database.Databases.PLATFORM));
        com.legend.TypedQuery called = com.legend.Compiler.query(drawing,
                "|let x = t::draw(); assertEquals($x->toString(), $x->toString());");
        org.junit.jupiter.api.Assertions.assertSame(called, UpstreamPrints.read(called, false, never));
        // ...and behind a second function, a lambda inside one, and a derived property (the audit's re-check, R1:
        // the scan is over the whole tree, the compiled bodies the operand calls included)
        com.legend.compiler.element.ModelContext behind = com.legend.Compiler.compileModel(
                com.legend.testbackend.StorelessRuntime.with(MODEL
                        + "function t::draw(): Float[1] { random() }\n"
                        + "function t::outer(): Float[1] { t::draw() * 1.0 }\n"
                        + "function t::mapped(): Float[1] { [1.0]->map(x|$x * random())->toOne() }\n"
                        + "function t::head(xs: Float[*]): Float[1] { $xs->at(0) }\n"
                        + "Class t::Box { v: Float[1]; drawn() { $this.v * random() }: Float[1]; }\n",
                        com.legend.database.Databases.PLATFORM));
        for (String body : List.of(
                "|assertEquals('0.5', t::outer()->toString());",
                "|assertEquals('0.5', t::mapped()->toString());",
                "|assertEquals('0.5', [1.0]->map(x|$x * t::outer())->toOne()->toString());",
                "|let b = ^t::Box(v = 1.0); assertEquals('0.5', $b.drawn->toString());",
                // a pick of a function's own parameter: the rows it is given are not in sight in its body
                "|assertEquals('1.0', t::head([1.0, 2.0])->toString());")) {
            com.legend.TypedQuery typed = com.legend.Compiler.query(behind, body);
            org.junit.jupiter.api.Assertions.assertSame(typed, UpstreamPrints.read(typed, false, never), body);
        }
    }

    /** A LEGACY TABLE'S ROWS ARE ROWS (the second independent review of slices 3 and 4, 2026-11-26, its finding 1):
     *  legend-engine's {@code TabularDataSet[1]} is one value of a class type, so a {@code limit} over an unsorted
     *  projection was not seen as a pick from unordered rows, and the Float's print behind it was read twice, the
     *  print's reading and the body's. It is left as written; the same pick from a SORTED projection is repeatable
     *  and is lifted. */
    @Test
    void aPickFromALegacyTablesUnorderedRowsIsLeftToTheDatabase() {
        com.legend.compiler.element.ModelContext stored = com.legend.Compiler.compileModel(String.join("\n",
                "###Relational\nDatabase t::DB ( Table T ( ID INTEGER PRIMARY KEY, PAY DOUBLE ) )",
                "###Pure\nClass t::P { id: Integer[1]; salary: Float[1]; }",
                "###Mapping\nMapping t::M ( t::P: Relational { ~mainTable [t::DB]T id: [t::DB]T.ID,"
                        + " salary: [t::DB]T.PAY } )",
                "###Connection\nRelationalDatabaseConnection t::Conn { store: t::DB; type: H2;"
                        + " specification: LocalH2 { }; auth: DefaultH2; }",
                "###Runtime\nRuntime t::RT { mappings: [t::M]; connections: [ t::DB: [ c: t::Conn ] ]; }"));
        UpstreamPrints.Reads never = (query, answer) -> {
            throw new AssertionError("nothing is read: " + query.body());
        };
        String read = " assertEquals('1.5', $r.values->toOne().rows->toOne().getFloat('s')->toString());";
        for (String picked : List.of("->limit(1)", "->take(1)", "->slice(0, 1)", "->drop(1)")) {
            String body = "|let r = execute(|t::P.all()->project([p|$p.salary], ['s'])" + picked
                    + ", t::M, t::RT.runtimeValue, []);" + read;
            com.legend.TypedQuery typed = com.legend.Compiler.query(stored, body);
            assertSame(typed, UpstreamPrints.read(typed, false, never), body);
        }
        // sorted first, the pick is the sort's: read, and printed (a stand-in printer: no database is run here)
        List<String> printed = new ArrayList<>();
        UpstreamPrints.Printer printer = (side, query, reads) -> {
            printed.add(side.form().name());
            return "1.5";
        };
        String sorted = "|let r = execute(|t::P.all()->project([p|$p.salary], ['s'])->sort('s')->limit(1)"
                + ", t::M, t::RT.runtimeValue, []);" + read;
        com.legend.TypedQuery typed = com.legend.Compiler.query(stored, sorted);
        assertTrue(UpstreamPrints.read(typed, false, never, printer) != typed, sorted);
        assertEquals(List.of("VALUE"), printed);
    }

    /** ...AND A VALUE WHOSE SOURCE IS REPEATABLE IS LIFTED (the review of the repair, 2026-11-26: the rule must not
     *  refuse a value merely because it reads rows or holds a lambda): pure values through a lambda are in the
     *  language's order, and a value taken by position from SORTED rows is in the sort's. Each is read, printed by
     *  legend-pure's rule in Java -- {@code 0.0000001}, where lite's own text of the value is {@code 0.0} -- and the
     *  body then passes. */
    @Test
    void aValueFromARepeatableSourceIsLifted() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        String sorted = "let s = " + REL + "->sort(~id->descending());";
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            // a function and a derived property that draw nothing are in sight, and repeatable (the re-check's R1)
            com.legend.compiler.element.ModelContext pure = com.legend.Compiler.compileModel(
                    com.legend.testbackend.StorelessRuntime.with(MODEL
                            + "function t::twice(x: Float[1]): Float[1] { $x * 2.0 }\n"
                            + "Class t::Box { v: Float[1]; twice() { $this.v * 2.0 }: Float[1]; }\n",
                            com.legend.database.Databases.PLATFORM));
            for (String body : List.of(
                    "|assertEquals('0.0000002', t::twice(1.0E-7)->toString());",
                    // a pick from sorted rows in a call's argument is this body's, read with its lets
                    "|" + sorted + " assertEquals('0.0000004', t::twice($s->map(r|$r.id->toOne() * 1.0E-7)->at(0))"
                            + "->toString());",
                    "|let b = ^t::Box(v = 1.0E-7); assertEquals('0.0000002', $b.twice->toString());")) {
                com.legend.TypedQuery typed = com.legend.Compiler.query(pure, body);
                com.legend.TypedQuery read = UpstreamPrints.read(typed, false, reads);
                assertTrue(read != typed, body);
                assertEquals(0, toStrings(read), body);
                com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
            for (String body : List.of(
                    // the re-check's two examples: pure values through a lambda, in the language's order
                    "|assertEquals('0.30000000000000004', [0.1]->map(x|$x + 0.2)->toOne()->toString());",
                    "|assertEquals('0.0000001', [1.0E-7]->map(x|$x)->toOne()->toString());",
                    "|assertEquals('0.0000001', ([1.0E-7]->map(x|$x * 1.0)->at(0))->toString());",
                    // a limit and a sum of values in the language's own order, and of sorted rows
                    "|assertEquals('0.0000001', [1.0E-7, 2.0]->map(x|$x * 1.0)->limit(1)->toOne()->toString());",
                    "|assertEquals('0.75', [0.5, 0.25]->sum()->toString());",
                    // a text of values in the language's own order is that order's (finding 3's other way)
                    "|assertEquals('21.0', [2, 1]->makeString()->parseFloat()->toString());",
                    "|" + sorted + " assertEquals('0.0000002', $s->map(r|$r.id->toOne() * 1.0E-7)->take(1)->toOne()"
                            + "->toString());",
                    "|" + sorted + " assertEquals('0.0000002', ($s->map(r|$r.id->toOne() * 1.0E-7)->at(0))->toString());")) {
                com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, body);
                com.legend.TypedQuery read = UpstreamPrints.read(typed, false, reads);
                assertTrue(read != typed, body);
                assertEquals(0, toStrings(read), body);
                com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
        }
    }

    /** A FLOAT LITERAL'S PRINT IS WRITTEN AT THE DOOR, NOTHING READ (the audit's S6): the text is a fact of the
     *  program, legend-pure's print of the literal's double, where the literal stands or through the let it is bound
     *  to, with its tail applied at the door; no database parses the literal. A computed value is read. */
    @Test
    void aFloatLiteralsPrintIsWrittenAtTheDoorWithoutARead() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        UpstreamPrints.Reads never = (query, answer) -> {
            throw new AssertionError("nothing is read: " + query.body());
        };
        java.util.Map<String, Object> bodies = new java.util.LinkedHashMap<>();
        bodies.put("|assertEquals('0.000000013421', 1.3421e-8->toString());", "0.000000013421");
        bodies.put("|let x = 1.3421e8; assertEquals('134210000.0', $x->toString());", "134210000.0");
        bodies.put("|assertEquals(14, 1.3421e-8->toString()->length());", 14L);
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            for (var e : bodies.entrySet()) {
                com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, e.getKey());
                com.legend.TypedQuery read = UpstreamPrints.read(typed, false, never);
                assertTrue(read != typed, e.getKey());
                assertEquals(0, toStrings(read), e.getKey());
                com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
        }
    }

    /** ONE VALUE, PRINTED AT A LAST STEP AND ONE CALL FURTHER, IN ONE BODY (the audit's S1): the side that is the
     *  assert's own argument is legend-pure's text, Java's; the same print under another call is lite's own text, the
     *  database's -- the design the user ruled on 2026-11-26 ("yes that's right but remember we have a list of
     *  chained functions that are okay after that you came up with"). The two agree wherever lite's six decimals hold
     *  the value ({@code 0.00001}: 7 and 7; until the deletion of the edge's Float pass the second was the
     *  database's cast, {@code 1e-05}, 5), and part beyond them, as the register row says ({@code 1.0E-7}: Java's
     *  {@code 0.0000001}, 9, and lite's {@code 0.0}, 3). */
    @Test
    void aPrintAtALastStepAndTheSamePrintUsedFurtherInOneBody() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            for (String body : List.of(
                    "|let x = 1.0E-5 * 1.0; assertEquals($x->toString()->length(), $x->toString()->length() + 0);",
                    "|let x = 12345678.5 * 1.0; assertEquals('v=12345678.5', 'v=' + $x->toString());",
                    "|let x = 1.0E-7 * 1.0; assertEquals(9, $x->toString()->length());"
                            + " assertEquals(3, $x->toString()->length() + 0);")) {
                com.legend.TestExecution.runProgram(UpstreamPrints.read(com.legend.Compiler.query(ctx, body), false,
                        reads), com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
        }
    }

    /** The {@code toString} calls left anywhere in {@code q}'s body. */
    private static int toStrings(com.legend.TypedQuery q) {
        int n = 0;
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>(q.body());
        while (!work.isEmpty()) {
            TypedSpec node = work.pop();
            if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                    && c.callee().qualifiedName().endsWith("::toString")) {
                n++;
            }
            work.addAll(node.children());
        }
        return n;
    }

    @Test
    @DisplayName("the edge's printer: legend-pure's text of the rows read (B2b's writer), a literal replace applied in"
            + " Java; lite's judge compares it with upstream's golden, and the body passes")
    void legendPuresTextThroughTheEdgesPrinter() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            for (String body : List.of(
                    "|let r = " + REL + "; assertEquals('#TDS\n   id,name\n   1,A\n   2,B\n#', $r->toString());",
                    "|let r = " + REL + "; assertEquals('#TDS,   id;name,   1;A,   2;B,#',"
                            + " $r->toString()->replace('\n', ',')->replace(',A', ';A')->replace(',B', ';B')"
                            + "->replace('id,', 'id;'));")) {
                com.legend.TypedQuery read = UpstreamPrints.read(com.legend.Compiler.query(ctx, body), false, reads);
                com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
        }
    }

    @Test
    @DisplayName("a printed relation with no defined order (past a grouping) against a golden: an explicit"
            + " assertSameElements of the two texts' lines, either order passing (S131's multiset, made explicit)")
    void anUnorderedPrintedTableIsComparedLineByLine() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        String grouped = "let g = " + REL + "->groupBy(~name, ~n: x | $x.id : y | $y->count());";
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            for (String golden : List.of("'#TDS\n   name,n\n   A,1\n   B,1\n#'", "'#TDS\n   name,n\n   B,1\n   A,1\n#'")) {
                com.legend.TypedQuery read = UpstreamPrints.read(com.legend.Compiler.query(ctx,
                        "|" + grouped + " assertEquals(" + golden + ", $g->toString());"), false, reads);
                assertEquals(com.legend.builtin.NativeFn.Verdict.ASSERT_SAME_ELEMENTS,
                        com.legend.builtin.NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(
                                read.body().get(1))).orElseThrow(), "the assert made order-free");
                com.legend.TestExecution.runProgram(read, com.legend.testbackend.StorelessRuntime.RUNTIME,
                        com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                        com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            }
        }
    }

    @Test
    @DisplayName("an unordered table joined on one line by replace('\\n', ','): rows compared in any order, never cells"
            + " (the review of step 2b's M3)")
    void aJoinedUnorderedTableIsComparedByRows() throws Exception {
        com.legend.compiler.element.ModelContext ctx = ctx();
        String grouped = "let g = " + REL + "->groupBy(~name, ~n: x | $x.id : y | $y->count());";
        try (java.sql.Connection c = com.legend.testbackend.TestBackends.platform().open()) {
            UpstreamPrints.Reads reads = (query, answer) -> com.legend.TestExecution.runProgram(query,
                    com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE, answer);
            // the rows in either order hold; a cell moved between rows does not
            com.legend.TypedQuery holds = UpstreamPrints.read(com.legend.Compiler.query(ctx, "|" + grouped
                    + " assertEquals('#TDS,   name,n,   B,1,   A,1,#', $g->toString()->replace('\\n', ','));"), false,
                    reads);
            com.legend.TestExecution.runProgram(holds, com.legend.testbackend.StorelessRuntime.RUNTIME,
                    com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                    com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE);
            com.legend.TypedQuery fails = UpstreamPrints.read(com.legend.Compiler.query(ctx, "|" + grouped
                    + " assertEquals('#TDS,   name,n,   B,A,   1,1,#', $g->toString()->replace('\\n', ','));"), false,
                    reads);
            org.junit.jupiter.api.Assertions.assertThrows(com.legend.error.AssertFailed.class,
                    () -> com.legend.TestExecution.runProgram(fails, com.legend.testbackend.StorelessRuntime.RUNTIME,
                            com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)), null,
                            com.legend.testexec.NoEdgeRun.NONE, com.legend.ExecuteOptions.NONE));
        }
    }

    @Test
    @DisplayName("a body that writes, and a print inside a lambda, are left as written (the review of step 2b's M1)")
    void writesAndLambdasAreLeftAsWritten() {
        com.legend.compiler.element.ModelContext ctx = ctx();
        UpstreamPrints.Reads never = (query, answer) -> {
            throw new AssertionError("nothing is read");
        };
        UpstreamPrints.Printer printer = (side, read, r) -> "printed";
        com.legend.TypedQuery printed = com.legend.Compiler.query(ctx,
                "|let r = " + REL + "; assertEquals('x', $r->toString());");
        assertSame(printed, UpstreamPrints.read(printed, true, never, printer));
        com.legend.TypedQuery inLambda = com.legend.Compiler.query(ctx,
                "|let r = " + REL + "; let f = {|$r->toString()}; assertEquals('x', $f->eval());");
        assertSame(inLambda, UpstreamPrints.read(inLambda, false, never, printer));
    }

    @Test
    @DisplayName("no printer, or a body with no printed side: the query itself")
    void nothingSubstituted() {
        com.legend.compiler.element.ModelContext ctx = ctx();
        UpstreamPrints.Reads never = (query, answer) -> {
            throw new AssertionError("nothing is read");
        };
        com.legend.TypedQuery printed = com.legend.Compiler.query(ctx,
                "|let r = " + REL + "; assertEquals('x', $r->toString());");
        assertSame(printed, UpstreamPrints.read(printed, false, never, UpstreamPrints.Printer.NONE));
        com.legend.TypedQuery none = com.legend.Compiler.query(ctx, "|assertEquals(1, 1);");
        assertSame(none, UpstreamPrints.read(none, false, never));
    }
}
