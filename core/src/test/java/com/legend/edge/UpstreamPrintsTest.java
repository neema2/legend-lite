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
                p.replaces());
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
