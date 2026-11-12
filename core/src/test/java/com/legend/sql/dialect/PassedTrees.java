// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlQuery;

/** The cast-and-case census's reader of a dialect's tree after its passes (E-4, 2026-11-12; test code, in the
 *  dialects' package for their package-private {@code passed}). A dialect that is no {@link AnsiSqlRenderer} has no
 *  passes: its tree is the lowering's. */
public final class PassedTrees {

    private PassedTrees() {
    }

    /** {@code query} as {@code dialect} renders it, before its text is written. */
    public static SqlQuery passed(SqlDialect dialect, SqlQuery query) {
        return dialect instanceof AnsiSqlRenderer a ? a.passed(query) : query;
    }
}
