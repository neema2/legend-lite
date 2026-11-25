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
        TestBackend backend = LaneFloor.backend();
        String model = backend.withStorelessRuntime("");
        try (Connection c = backend.open()) {
            for (var e : texts.entrySet()) {
                assertEquals(e.getValue(), Lean.run(model, "|" + e.getKey() + "->toString()", StorelessRuntime.RUNTIME, c)
                        .rows().get(0).get(0).toString(), backend.name() + ": " + e.getKey());
            }
        }
    }
}
