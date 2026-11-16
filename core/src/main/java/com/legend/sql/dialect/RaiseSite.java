// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;

import java.util.List;
import java.util.Objects;

/**
 * ONE RAISE SITE OF A STATEMENT (phase D's Phase 2, raises as SITES, 2026-11-16:
 * docs/records/d-phase2-raise-marker-2026-11-16.md, the revised design): the {@code n}th raise the dialect wrote into a
 * statement, numbered as it was written ({@code AnsiSqlRenderer.raiseMessage}, the one writer). The statement's SQL
 * raises the bare token {@link RaiseKind#token} ({@code LG#<n>}) -- no kind name, no where, no value -- and the plan carries
 * what the token means: its kind, its where (a fixed text known at planning, or none) and the operands its raise was
 * built from (the edge words upstream's sentence from them at planning, never at run). {@code upstream}: legend-pure's
 * sentence where the statement was written in the compatibility edge's style ({@code database.PlanStyle#siteWords}),
 * null in the product's.
 */
public record RaiseSite(int n, RaiseKind kind, @com.legend.base.Nullable String where, List<SqlExpr> operands,
        @com.legend.base.Nullable String upstream) {

    public RaiseSite {
        if (n < 1) {
            throw new IllegalArgumentException("a raise site is numbered from 1: " + n);
        }
        Objects.requireNonNull(kind, "a raise site's kind");
        operands = List.copyOf(operands);
        if (kind == RaiseKind.USER_FAIL) {
            throw new IllegalArgumentException("a user's fail raises its own message, never a site's token");
        }
    }

    /** This site, worded {@code words} upstream's way (the style's, at planning). */
    public RaiseSite worded(@com.legend.base.Nullable String words) {
        return new RaiseSite(n, kind, where, operands, words);
    }
}
