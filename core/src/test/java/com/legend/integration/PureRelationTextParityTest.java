// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.edge.PureRelationTextWriter;
import com.legend.edge.PureText;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE JAVA PRINTER AGAINST THE EDGE'S SQL TEXT (stage B's B2, B2b S1, 2026-11-24): for each relation, legend-pure's
 * {@code toString()} as the edge composes it in SQL today ({@code edge.RelationTextLowering}, the text PCT and the
 * corpus pass with) and as {@link com.legend.edge.PureRelationText} prints it in Java from the same relation's typed
 * rows, run on the same session in the edge's style -- equal on each lane's database (DuckDB, H2, SQLite). B2b's door
 * and the harness's lift replace the first with the second; this is the parity they rest on, carrier by carrier.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class PureRelationTextParityTest {

    /** The relations: each carrier a relation's cell can hold, an empty cell, a name that needs quotes, no rows. */
    private static final List<String> RELATIONS = List.of(
            "#TDS\n  i:Integer, s:String, b:Boolean\n  1, a, true\n  2, b c, false\n  3, , \n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, f:Float\n  1, 2.0\n  2, 0.00001\n  3, 1.5\n  4, 100000000000000000000.0\n  5, -0.25\n"
                    + "  6, 0.1\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, d:StrictDate, t:DateTime\n  1, 2014-01-02, 2014-01-01T10:20:30.123\n"
                    + "  2, 2020-02-29, 2020-02-29T00:00:00\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, d:Date\n  1, 2014-01-02T10:20:30\n  2, 2020-02-29T23:59:59.5\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, d:Date\n  1, 2014-01-02\n  2, 2020-02-29\n#->sort(~i->ascending())",
            "#TDS\n  i:Integer, s:String\n  1, x\n  2, y\n#->sort(~i->ascending())"
                    + "->extend(~braced: r|'{' + $r.s->toOne() + '}', ~bracketed: r|'[it\\'s ' + $r.s->toOne() + ']')",
            "#TDS\n  i:Integer, s:String\n  1, x\n#->extend(~'my col': r|$r.i->toOne() * 2)",
            "#TDS\n  i:Integer, s:String\n  1, x\n#->filter(r|$r.i == 2)",
            "#TDS\n  g:String, f:Float\n  a, 1.5\n  a, 2.25\n  b, 3.0\n#->groupBy(~g, ~total: r|$r.f : s|$s->sum())"
                    + "->sort(~g->ascending())");

    @Test
    void theJavaPrinterWritesTheEdgesSqlText() throws Exception {
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        List<String> differ = new ArrayList<>();
        try (Connection c = backend.open(PureText.UPSTREAM)) {
            for (String rel : RELATIONS) {
                String sql = Lean.runInStyle(model, "|" + rel + "->toString()", StorelessRuntime.RUNTIME, c,
                        PureText.UPSTREAM).rows().get(0).get(0).toString();
                String java = printed(model, rel, c);
                if (!sql.equals(java)) {
                    differ.add(rel.replace("\n", "\\n") + "\n  SQL:  " + sql.replace("\n", "\\n") + "\n  Java: "
                            + java.replace("\n", "\\n"));
                }
            }
        }
        assertEquals(List.of(), differ, backend.name());
    }

    /** {@code rel}'s rows, run on {@code c} in the edge's style, printed by the Java printer. */
    private static String printed(String model, String rel, Connection c) throws Exception {
        List<String> text = new ArrayList<>();
        com.legend.Execution.run(com.legend.Compiler.query(com.legend.Compiler.compileModel(LaneFloor.model(model, c)),
                                "|" + rel).withOptions(Lean.TEST_OPTIONS), StorelessRuntime.RUNTIME,
                com.legend.exec.SessionSource.given(com.legend.testbackend.TestSessions.over(c)),
                com.legend.exec.PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                com.legend.exec.ProductRun.Establish.DECLARED_SETUP, java.util.Set.of(), PureText.UPSTREAM,
                new com.legend.result.Output.Rows(new PureRelationTextWriter(PureRelationTextWriter.Form.TO_STRING,
                        List.of(), text::add)), com.legend.result.Batcher.DEFAULT_CAPACITY);
        if (text.size() != 1) {
            throw new IllegalStateException("the printer answered " + text.size() + " texts");
        }
        return text.get(0);
    }
}
