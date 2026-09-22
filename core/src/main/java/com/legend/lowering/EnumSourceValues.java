// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

import java.util.ArrayList;
import java.util.List;

/**
 * Enum toSourceValues (engine pureToSQLQuery:5866): the mapping's enum
 * DECODE emission compares by SOURCE value, never by decoded name.
 */
final class EnumSourceValues {

    private EnumSourceValues() {
    }

    /**
     * Enum toSourceValues (engine pureToSQLQuery:5866): comparing a
     * DECODE case-chain (every branch a distinct string literal, null
     * terminal — the mapping's enum decode emission) against a string
     * literal inverts to the matching branch's SOURCE condition
     * ({@code "root".active = 0}) — the engine never compares decoded
     * names. Pure algebra: valid for any literal-decode case shape.
     */
    static @com.legend.base.Nullable SqlExpr decodeInvert(
            com.legend.compiler.spec.typed.TypedSpec ta,
            com.legend.compiler.spec.typed.TypedSpec tb,
            SqlExpr a, SqlExpr b) {
        SqlExpr lit = b instanceof SqlExpr.StringLit ? b
                : a instanceof SqlExpr.StringLit ? a : null;
        if (lit != null) {
            // Which SPACE does the literal live in? An ENUM literal
            // (Type.FTE) names a decoded value — invert by NAME to its
            // source condition(s). A plain STRING compares in RAW space:
            // the engine's golden pins `"root".type = 'FTE'` for
            // getEnum('Type') == 'FTE' — the string meets the raw store
            // column verbatim, never the decoded name (C1.4).
            com.legend.compiler.spec.typed.TypedSpec litSpec = lit == b ? tb : ta;
            if (!(litSpec instanceof
                    com.legend.compiler.spec.typed.TypedEnumValue)) {
                SqlExpr chainSide = lit == b ? a : b;
                return com.legend.sql.DecodeShapes.sourceExpr(chainSide)
                        .map(src -> (SqlExpr) SqlExpr.Call.of(
                                com.legend.sql.SqlFn.EQUAL, src, lit))
                        .orElse(null);
            }
        }
        if (lit == null) {
            // BOTH sides decode chains over the SAME enum table (C1.4:
            // if($p.type == $q.type, ...) with one shared enum mapping):
            // equal names iff equal SOURCE values — compare the raw
            // sources, never the decoded strings (engine parity: the
            // engine holds raw columns here and compares them directly)
            List<SqlExpr.Case.When> fa =
                    com.legend.sql.DecodeShapes.flattenDecode(a).orElse(null);
            List<SqlExpr.Case.When> fb =
                    com.legend.sql.DecodeShapes.flattenDecode(b).orElse(null);
            if (fa == null || fb == null || !sameDecodeTable(fa, fb)) {
                return null;
            }
            SqlExpr sa = com.legend.sql.DecodeShapes.sourceExpr(a).orElse(null);
            SqlExpr sb = com.legend.sql.DecodeShapes.sourceExpr(b).orElse(null);
            return sa == null || sb == null ? null
                    : SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL, sa, sb);
        }
        SqlExpr chain = lit == b ? a : b;
        List<SqlExpr.Case.When> flat =
                com.legend.sql.DecodeShapes.flattenDecode(chain).orElse(null);
        if (flat == null) {
            return null;
        }
        // MULTIPLE branches may decode to the same name (CONTRACT from
        // FTC and FTO — C1.4): the inversion is the OR of their source
        // conditions, never a bail-out (bailing kept the decoded-name
        // compare, which matches nothing). A name NO branch produces
        // can never compare equal (engine spells '0 = 1').
        SqlExpr match = null;
        String want = ((SqlExpr.StringLit) lit).value();
        for (var w : flat) {
            if (((SqlExpr.StringLit) w.then()).value().equals(want)) {
                match = match == null ? w.condition()
                        : SqlExpr.Call.of(com.legend.sql.SqlFn.OR,
                                match, w.condition());
            }
        }
        return match != null ? match : new SqlExpr.BoolLit(false);
    }

    /** Same (source literal -> name) rows, in order — one enum mapping
     * emitted twice. Source EXPRESSIONS may differ (two columns). */
    private static boolean sameDecodeTable(List<SqlExpr.Case.When> a,
            List<SqlExpr.Case.When> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!(a.get(i).condition() instanceof SqlExpr.Call ca
                    && ca.fn() == com.legend.sql.SqlFn.EQUAL
                    && ca.args().size() == 2
                    && b.get(i).condition() instanceof SqlExpr.Call cb
                    && cb.fn() == com.legend.sql.SqlFn.EQUAL
                    && cb.args().size() == 2
                    && ca.args().get(1).equals(cb.args().get(1))
                    && a.get(i).then().equals(b.get(i).then()))) {
                return false;
            }
        }
        return true;
    }

}
