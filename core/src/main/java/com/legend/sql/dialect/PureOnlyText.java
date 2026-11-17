// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

/**
 * A TEXT ONLY LEGEND-PURE WRITES ({@link SqlFn#INSTANCE_TEXT}; format's padding left with B8 (i), 2026-11-17, refused at lowering; the user's rulings of 2026-11-10, D_BRIEF §1: "format:
 * %s %d %f %t{} in the product; %r and padding at the edge", "objects and relations are refused, pointing to graph
 * fetch"; phase D's D1.3, 2026-11-15): the product refuses it by name, a pass of the base renderer run after the
 * compatibility edge's spelling passes (which write the text for it) and before every other.
 */
final class PureOnlyText {

    private PureOnlyText() {
    }

    /** The base renderer's pass: a text only legend-pure writes refused, naming what it is and the product's way. */
    static final class Refusal extends com.legend.sql.SqlRewriter {
        @Override
        protected SqlExpr expr(SqlExpr e) {
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.RELATION_TEXT) {
                throw refused(String.valueOf(c.args().get(1) instanceof SqlExpr.StringLit s ? s.value() : "?")
                        + " of a relation", "the product writes a relation's rows by its result writers (JSON, CSV, "
                        + "Arrow)");
            }
            if (e instanceof SqlExpr.Call c && c.fn() == SqlFn.INSTANCE_TEXT) {
                throw refused("toString of an instance", "the product writes an object by graph fetch");
            }
            return super.expr(e);
        }

        private static UnsupportedOperationException refused(String what, String instead) {
            return new UnsupportedOperationException(what + " is legend-pure's text, the compatibility edge's: "
                    + instead);
        }
    }
}
