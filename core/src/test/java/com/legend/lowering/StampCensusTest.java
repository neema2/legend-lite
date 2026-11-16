// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.sql.SqlExpr;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The stamp-vs-shape invariant ({@link StampCensus#check}) on a NULL under a CASE: SQL NULL is shape-ambiguous by the
 * carrier convention (it carries pure's EMPTY in scalar and list positions), and so is a CASE whose every branch is
 * NULL; before 2026-10-14 such a CASE counted as list-shaped and fired under a scalar stamp (the tenth line's find,
 * reached from both islands). A CASE of real list branches under a scalar stamp still fires.
 */
class StampCensusTest {

    private static final TypedCString ONE_OR_NONE = new TypedCString("x",
            new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ZERO_ONE));

    private static SqlExpr caseOf(SqlExpr then, SqlExpr otherwise) {
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(new SqlExpr.BoolLit(true), then)), otherwise, SqlExpr.Reason.USER);
    }

    @Test
    void anAllNullCaseUnderAScalarStampIsNoLie() {
        assertDoesNotThrow(() -> StampCensus.check(ONE_OR_NONE, caseOf(new SqlExpr.NullLit(), new SqlExpr.NullLit())));
        assertDoesNotThrow(() -> StampCensus.check(ONE_OR_NONE, caseOf(new SqlExpr.NullLit(), null)),
                "no ELSE is SQL's NULL (pins what held before too: listShaped asks for an ELSE)");
        assertDoesNotThrow(() -> StampCensus.check(ONE_OR_NONE,
                caseOf(caseOf(new SqlExpr.NullLit(), new SqlExpr.NullLit()), new SqlExpr.NullLit())), "nested");
    }

    @Test
    void aCaseOfListsUnderAScalarStampStillFires() {
        SqlExpr lists = caseOf(new SqlExpr.ArrayLit(List.of()), new SqlExpr.ArrayLit(List.of()));
        assertThrows(IllegalStateException.class, () -> StampCensus.check(ONE_OR_NONE, lists));
        SqlExpr mixed = caseOf(new SqlExpr.ArrayLit(List.of()), new SqlExpr.NullLit());
        assertThrows(IllegalStateException.class, () -> StampCensus.check(ONE_OR_NONE, mixed),
                "a NULL branch beside a list branch does not make the CASE ambiguous");
    }
}
