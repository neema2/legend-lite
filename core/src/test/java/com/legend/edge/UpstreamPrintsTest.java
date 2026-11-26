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
        assertEquals(List.of(new PureRelationText.Replace("\n", ","), new PureRelationText.Replace("A", "a")),
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
                    "|assertEquals(27, " + computed + "->toString()->length());")) {
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
                "|assertEquals('3.0;', (1.5 * 2.0)->toString() + ';');",
                "|let s = (1.5 * 2.0)->toString(); assertEquals('3.0', $s);",
                "|assertEquals(['3.0'], [(1.5 * 2.0)->toString()]);",
                "|assertEquals('3.0', (1.5 * 2.0)->toString()->toUpper());",
                "|assert((1.5 * 2.0)->toString()->length() > 2);")) {
            com.legend.TypedQuery typed = com.legend.Compiler.query(ctx, body);
            org.junit.jupiter.api.Assertions.assertSame(typed, UpstreamPrints.read(typed, false, never), body);
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
