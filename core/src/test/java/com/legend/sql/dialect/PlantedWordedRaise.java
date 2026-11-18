// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/** Planted (HookRulingTest.theRaisePinBitesWordsBesideTheToken; the review of 091b9bde36, 2026-11-18, SHOULD-FIX 5): a
 *  product dialect writing words beside its raise's token ({@code CONCAT(token, ' Division by zero')}): the token is
 *  there, quoted, and the statement is not the bare token's. It lives here, with the dialects, because it names one
 *  (DatabaseOwners). */
public final class PlantedWordedRaise extends DuckDb {
    @Override
    protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
        return com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.CONCAT, site(r, written -> null),
                new com.legend.sql.SqlExpr.StringLit(" Division by zero"));
    }

    /** The same dialect, unplanted: its raise is the token's. */
    public static SqlDialect plain() {
        return new DuckDb();
    }
}
