// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;
import com.legend.sql.SqlTyping;
import com.legend.sql.TypeFact;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * M4 §3.2 — comparator-site binding: a comparator ranges over ONE
 * list, so the construction site stamps BOTH its operands as that
 * list's element (and the kept accumulator as a list of elements).
 * This is the attachment-door convention that replaces the parked
 * branch's comparator FQN registry; fold stays excluded (its second
 * parameter is the accumulator, not an element).
 */
class DedupBindingTest {

    @Test
    void comparatorOperandsCarryTheElementStamp() {
        SqlExpr carried = new SqlExpr.Cast(
                new SqlExpr.ArrayLit(List.of(new SqlExpr.StringLit("1"))),
                new SqlType.Array(SqlType.Scalar.VARCHAR));
        List<TypeFact> seen = new ArrayList<>();
        Dedup.keptDedup(carried, 0, (prior, cand) -> {
            seen.add(prior.type());
            seen.add(cand.type());
            return SqlExpr.Call.of(SqlFn.EQUAL, prior, cand);
        });
        // §E3: element stamps are MAY-BE-NULL — presence is not
        // provable from Array(T) (the Column.param doctrine)
        assertEquals(List.of(
                        new TypeFact.Typed(SqlType.Scalar.VARCHAR, true),
                        new TypeFact.Typed(SqlType.Scalar.VARCHAR, true)),
                seen);
    }

    @Test
    void untypedCollectionLeavesOperandsUnknown() {
        // the door never guesses — an unstamped collection binds nothing
        SqlExpr plain = new SqlExpr.Column("t", "c");
        List<TypeFact> seen = new ArrayList<>();
        Dedup.keptDedup(plain, 0, (prior, cand) -> {
            seen.add(prior.type());
            seen.add(cand.type());
            return SqlExpr.Call.of(SqlFn.EQUAL, prior, cand);
        });
        assertEquals(List.of(SqlTyping.UNKNOWN, SqlTyping.UNKNOWN), seen);
    }
}
