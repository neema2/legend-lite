// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A RELATION'S {@code toString()} IN THE PRODUCT: lite's own text, computed by the database (stage B's B2b S5a,
 * 2026-11-24; the user's rulings of 2026-11-19 ~18:05, "Agree with all four table printing recommendations", and ~18:36,
 * "Null is good"; {@code lowering.ProductRelationText}): legend-pure's {@code #TDS} layout, each cell the product's own
 * text of its value -- a whole Float {@code 2.0} and six decimals trimmed, a DateTime lite's nine digits and {@code Z}, an
 * empty cell {@code null}, a String holding a brace quoted, a name needing quotes quoted, no rows a blank line -- one text
 * on each lane's database (DuckDB, H2, SQLite; measured 2026-11-24). An abstract Date held as a timestamp prints the
 * database's own text of a timestamp ({@code 2014-01-02 10:20:30}, the same on these three), the product's scalar
 * {@code toString} gap (G2) that waits for B2a's held item 4 (the orchestrator's Q3, 2026-11-24).
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class ProductRelationTextTest {

    @Test
    void aRelationsTextIsLitesOwnComputedByTheDatabase() throws Exception {
        Map<String, String> texts = new LinkedHashMap<>();
        texts.put("#TDS\n  i:Integer, s:String, b:Boolean\n  1, a, true\n  2, b c, false\n  3, , \n#->sort(~i->ascending())",
                "#TDS\n   i,s,b\n   1,a,true\n   2,b c,false\n   3,null,null\n#");
        texts.put("#TDS\n  i:Integer, f:Float\n  1, 2.0\n  2, 0.00001\n  3, 1.5\n  4, 25000000.0\n  5, -0.25\n#"
                + "->sort(~i->ascending())", "#TDS\n   i,f\n   1,2.0\n   2,0.00001\n   3,1.5\n   4,25000000.0\n   5,-0.25\n#");
        texts.put("#TDS\n  i:Integer, d:StrictDate, t:DateTime\n  1, 2014-01-02, 2014-01-01T10:20:30.123\n"
                + "  2, 2020-02-29, 2020-02-29T00:00:00\n#->sort(~i->ascending())",
                "#TDS\n   i,d,t\n   1,2014-01-02,2014-01-01T10:20:30.123000000Z\n   2,2020-02-29,2020-02-29T00:00:00.000000000Z\n#");
        texts.put("#TDS\n  i:Integer, d:Date\n  1, 2014-01-02T10:20:30\n#->sort(~i->ascending())",
                "#TDS\n   i,d\n   1,2014-01-02 10:20:30\n#");
        texts.put("#TDS\n  i:Integer, s:String\n  1, x\n#->extend(~[braced: r|'{' + $r.s->toOne() + '}',"
                + " 'my col': r|$r.i->toOne() * 2])", "#TDS\n   i,s,braced,'my col'\n   1,x,'{x}',2\n#");
        texts.put("#TDS\n  i:Integer, s:String\n  1, x\n#->filter(r|$r.i == 2)", "#TDS\n   i,s\n\n#");
        // the review of S5a (2026-11-24): a sort by a column the text drops (the order's hidden key, Render.liftOrder),
        // a braced String holding a quote and a backslash, and a Number column -- each cell the product's scalar text of
        // its column: a Number column of integers and fractions as the database writes its common type (21.0, the record's
        // known item: a Number's text, outside B2a). (A Decimal column's cell is its database's scale, register S400, RULED
        // 2026-11-23: 1.50 beside -0.125 prints 1.500 on DuckDB, 1.50 on H2, 1.5 on SQLite, measured 2026-11-24 -- so it is
        // not one of this test's one-text-on-every-lane cases.)
        texts.put("#TDS\n  i:Integer, s:String\n  2, b\n  3, c\n  1, a\n#->sort(~i->descending())->select(~s)",
                "#TDS\n   s\n   c\n   b\n   a\n#");
        texts.put("#TDS\n  i:Integer, s:String\n  1, x\n#->extend(~braced: r|'{it\\'s \\\\ ' + $r.s->toOne() + '}')"
                + "->select(~braced)", "#TDS\n   braced\n   '{it\\'s \\\\ x}'\n#");
        texts.put("#TDS\n  i:Integer, n:Number\n  1, 21\n  2, 2.5\n#->sort(~i->ascending())",
                "#TDS\n   i,n\n   1,21.0\n   2,2.5\n#");
        // a group's sum (an Integer) and average (a Float), the deleted parity test's group carriers
        texts.put("#TDS\n  g:String, x:Integer\n  a, 1\n  a, 2\n  b, 3\n#->groupBy(~g, ~[s: r|$r.x : y|$y->sum(),"
                + " m: r|$r.x : y|$y->average()])->sort(~g->ascending())", "#TDS\n   g,s,m\n   a,3,1.5\n   b,3,3.0\n#");
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            java.util.List<String> differ = new java.util.ArrayList<>();
            for (var e : texts.entrySet()) {
                String got = Lean.run(model, "|" + e.getKey() + "->toString()", StorelessRuntime.RUNTIME, c).rows().get(0)
                        .get(0).toString();
                if (!got.equals(e.getValue())) {
                    differ.add(e.getKey() + "\n  expected: " + e.getValue() + "\n  got:      " + got);
                }
            }
            assertEquals(java.util.List.of(), differ, backend.name());
        }
    }

    /** The shapes the product's text refuses by name (the review of S5a, 2026-11-24): the typed header, and a column
     *  whose cells a mixed selection tags by kind. */
    @Test
    void theShapesItCannotWriteAreRefusedByName() throws Exception {
        Map<String, String> refusals = new LinkedHashMap<>();
        refusals.put("|#TDS\n  i:Integer\n  1\n#->toString(true)", "a relation's toString with its types and"
                + " multiplicities (toString(rel, true)) is not written by the product");
        refusals.put("|#TDS\n  i:Integer\n  1\n#->extend(~m:x|[1, 2.5]->max())->toString()->length()",
                "a relation's toString over a mixed column: its cells' kinds differ by row");
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            for (var e : refusals.entrySet()) {
                Exception refused = org.junit.jupiter.api.Assertions.assertThrows(Exception.class,
                        () -> Lean.run(model, e.getKey(), StorelessRuntime.RUNTIME, c), e.getKey());
                String chain = "";
                for (Throwable t = refused; t != null; t = t.getCause()) {
                    chain += t.getMessage() + " | ";
                }
                org.junit.jupiter.api.Assertions.assertTrue(chain.contains(e.getValue()), e.getKey() + ": " + chain);
            }
        }
    }
}
