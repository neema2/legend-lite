// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SqlFn#FLOAT_SHORTEST_TEXT} IS LITE'S OWN TEXT OF A FLOAT (the audit's K1, 2026-11-17, the orchestrator's
 * ruling): the product writes it. Lite's message text of a Float ({@link LiteText}, the database judge's half of the
 * sentence table under LITE's rules) is that node, so the node is a product node, not a compatibility spelling. If the
 * product stopped writing it, this test fails and the node is the edge's again (census B9 reopened).
 */
class LiteFloatTextTest {

    @Test
    void litesMessageTextOfAFloatIsTheShortestRoundTripText() {
        SqlExpr x = new SqlExpr.StringLit("0.1");   // any Float operand: the rule reads its type, not its form
        SqlExpr text = LiteText.of(Type.Primitive.FLOAT, x);
        assertTrue(writes(text, SqlFn.FLOAT_SHORTEST_TEXT),
                "lite's own message text of a Float writes FLOAT_SHORTEST_TEXT: " + text);
    }

    /** Whether {@code e} holds a call of {@code fn} anywhere in its tree (a record's text names its components). */
    private static boolean writes(SqlExpr e, SqlFn fn) {
        return e.toString().contains("fn=" + fn.name() + ",") || e.toString().contains("fn=" + fn.name() + "]");
    }
}
