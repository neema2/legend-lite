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

    /** THE HELPER'S {@code toCSV} IN THE PRODUCT (stage B's B2b S5b, 2026-11-25; the user's ruling of 2026-11-19 ~20:05,
     *  "toCSV yes toString or another easy database sql that makes sense"): the helper's layout, each cell the product's
     *  own text of its value -- a whole Float {@code 2.0}, a DateTime lite's nine digits and {@code Z} (S312; the
     *  orchestrator's relay of 2026-11-25, Q1), a cell or a name holding a comma or a quote quoted with its quotes
     *  doubled, every row ending with its own newline so that no rows is the header line alone (the user's ruling of
     *  2026-11-25, Q-C, "Okay to both csv ones"); the nulls and Strings by the user's ruling of 2026-11-25 (register
     *  S420): under {@code toCSV(t)} a null an empty field and an empty String {@code ""}, under {@code toCSV(t, true)}
     *  a null the word {@code null} and every String quoted -- one text on each lane's database (DuckDB, H2, SQLite). */
    @Test
    void aRelationsCsvIsLitesOwnComputedByTheDatabase() throws Exception {
        String csv = "->meta::relational::tests::csv::toCSV()";
        Map<String, String> texts = new LinkedHashMap<>();
        String basic = "#TDS\n  i:Integer, s:String, b:Boolean\n  1, a, true\n  2, b c, false\n  3, , \n#"
                + "->sort(~i->ascending())";
        texts.put(basic + csv, "i,s,b\n1,a,true\n2,b c,false\n3,,\n");
        texts.put(basic + "->meta::relational::tests::csv::toCSV(true)",
                "i,s,b\n1,\"a\",true\n2,\"b c\",false\n3,null,null\n");
        String strings = "#TDS\n  i:Integer\n  1\n#->extend(~[e: r|'', n: r|'null', q: r|'a,b'])";
        texts.put(strings + csv, "i,e,n,q\n1,\"\",null,\"a,b\"\n");
        texts.put(strings + "->meta::relational::tests::csv::toCSV(true)", "i,e,n,q\n1,\"\",\"null\",\"a,b\"\n");
        texts.put("#TDS\n  i:Integer, f:Float\n  1, 2.0\n  2, 0.00001\n  3, 1.5\n  4, 25000000.0\n  5, -0.25\n#"
                + "->sort(~i->ascending())" + csv, "i,f\n1,2.0\n2,0.00001\n3,1.5\n4,25000000.0\n5,-0.25\n");
        // a Float cell is program text, S311's six decimals (its amendment of 2026-11-25, B2b's design audit): the CSV
        // output format writes a returned Float exactly, toCSV writes the product's text of it
        texts.put("#TDS\n  i:Integer, f:Float\n  1, 0.1234567\n  2, 0.0000001\n#->sort(~i->ascending())" + csv,
                "i,f\n1,0.123457\n2,0.0\n");
        texts.put("#TDS\n  i:Integer, d:StrictDate, t:DateTime\n  1, 2014-01-02, 2014-01-01T10:20:30.123\n"
                + "  2, 2020-02-29, 2020-02-29T00:00:00\n#->sort(~i->ascending())" + csv,
                "i,d,t\n1,2014-01-02,2014-01-01T10:20:30.123000000Z\n2,2020-02-29,2020-02-29T00:00:00.000000000Z\n");
        texts.put("#TDS\n  i:Integer\n  1\n#->extend(~[c: r|'a,b', q: r|'say \"hi\"', 'my, col': r|$r.i->toOne() * 2])"
                + csv, "i,c,q,\"my, col\"\n1,\"a,b\",\"say \"\"hi\"\"\",2\n");
        // no rows: the header line alone; a one-column relation's one null row, an empty field and its own newline
        // (the user's ruling of 2026-11-25, Q-C, "Okay to both csv ones"; register S420)
        texts.put("#TDS\n  i:Integer, s:String\n  1, x\n#->filter(r|$r.i == 2)" + csv, "i,s\n");
        texts.put("#TDS\n  i:Integer, s:String\n  1, \n#->select(~s)" + csv, "s\n\n");
        texts.put("#TDS\n  i:Integer, s:String\n  1, x\n#->filter(r|$r.i == 2)->meta::relational::tests::csv::toCSV(true)",
                "i,s\n");
        texts.put("#TDS\n  i:Integer, s:String\n  2, b\n  3, c\n  1, a\n#->sort(~i->descending())->select(~s)" + csv,
                "s\nc\nb\na\n");
        // the review of S5b (2026-11-25): a line feed and a carriage return inside a String, a String of spaces (never ""),
        // a header name holding a quote, and an enumeration's cell (as a non-String cell: quoted where needed)
        texts.put("#TDS\n  i:Integer\n  1\n#->extend(~[l: r|'a\\nb', r: r|'c\\rd', sp: r|' ', 'say \"q\"': r|2])" + csv,
                "i,l,r,sp,\"say \"\"q\"\"\"\n1,\"a\nb\",\"c\rd\", ,2\n");
        texts.put("#TDS\n  i:Integer\n  1\n#->extend(~e: r|t::Color.RED)" + csv, "i,e\n1,RED\n");
        texts.put("#TDS\n  i:Integer\n  1\n#->extend(~e: r|t::Color.RED)->meta::relational::tests::csv::toCSV(true)",
                "i,e\n1,RED\n");
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("Enum t::Color { RED, GREEN }");
        try (Connection c = backend.open()) {
            java.util.List<String> differ = new java.util.ArrayList<>();
            for (var e : texts.entrySet()) {
                String got = Lean.run(model, "|" + e.getKey(), StorelessRuntime.RUNTIME, c).rows().get(0).get(0)
                        .toString();
                if (!got.equals(e.getValue())) {
                    differ.add(e.getKey() + "\n  expected: " + e.getValue() + "\n  got:      " + got);
                }
            }
            assertEquals(java.util.List.of(), differ, backend.name());
        }
    }

    /** The shapes the product's text refuses by name (the review of S5a, 2026-11-24): the typed header, and a column
     *  whose cells a mixed selection tags by kind; the CSV's (S5b, 2026-11-25): a renderTdsNull that is no literal. */
    @Test
    void theShapesItCannotWriteAreRefusedByName() throws Exception {
        Map<String, String> refusals = new LinkedHashMap<>();
        refusals.put("|#TDS\n  i:Integer\n  1\n#->toString(true)", "a relation's toString with its types and"
                + " multiplicities (toString(rel, true)) is not written by the product");
        refusals.put("|#TDS\n  i:Integer\n  1\n#->extend(~m:x|[1, 2.5]->max())->toString()->length()",
                "a relation's toString over a mixed column: its cells' kinds differ by row");
        refusals.put("|#TDS\n  i:Integer\n  1\n#->extend(~m:x|[1, 2.5]->max())->meta::relational::tests::csv::toCSV()"
                + "->length()", "a relation's toCSV over a mixed column: its cells' kinds differ by row");
        refusals.put("|#TDS\n  i:Integer\n  1\n#->meta::relational::tests::csv::toCSV('%t{yyyy-MM-dd HH:mm:ss}',"
                + " '%t{yyyy-MM-dd}', false)", "a relation's toCSV with date formats: the product writes a date by its own"
                + " text; toCSV(t) or toCSV(t, renderTdsNull)");
        refusals.put("|#TDS\n  i:Integer\n  1\n#->meta::relational::tests::csv::toCSV(today() > %2000-01-01)",
                "a relation's toCSV whose renderTdsNull is no literal is not written by the product");
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
