// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;

/**
 * NO COMPARISON OF TWO JSON TEXTS THAT WRITE ONE NUMBER TWO WAYS (step 6B part 2, 2026-11-28): a database that
 * carries a list, a struct or a map as JSON text compares two of them as their texts, where {@code 1.0} and
 * {@code 1.00} are two texts of one number. An equality, a membership or a position over two such composites whose
 * exact numbers are written at two scales is refused by name before anything is rewritten
 * ({@link SameValuesForm#refuseExactNumbersOfTwoScales}, {@link SameValuesForm#refuseMemberAtTwoScales}): it
 * answered false for two that are the same (measured by the equality measure's grid: {@code indexOf} of a Pair of a
 * whole number and a Decimal on SQLite, a membership over maps of Decimals, {@code list([1.0D]) == list([1.00D])} on
 * H2). The first pass of each such dialect; it changes nothing.
 */
final class JsonTextComparisons extends SqlRewriter {

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (e instanceof SqlExpr.Membership member) {
            // (a membership whose list's rows ride is read among the rows, leaf by typed leaf: no text is compared)
            if (member.rows() == null) {
                SameValuesForm.refuseMemberAtTwoScales(member.needle(), member.collection());
            }
        } else if (e instanceof SqlExpr.Call call && call.args().size() == 2) {
            if (call.fn() == SqlFn.LIST_POSITION) {
                SameValuesForm.refuseMemberAtTwoScales(call.args().get(1), call.args().get(0));
            } else if (call.fn() == SqlFn.EQUAL || call.fn() == SqlFn.NOT_EQUAL || call.fn() == SqlFn.NULL_SAFE_EQUAL
                    || call.fn() == SqlFn.NULL_SAFE_NOT_EQUAL || call.fn() == SqlFn.IS_DISTINCT_FROM) {
                SameValuesForm.refuseExactNumbersOfTwoScales(call.args());
            }
        }
        return e;
    }
}
