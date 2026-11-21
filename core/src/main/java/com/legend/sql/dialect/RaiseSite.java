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
 * raises the bare token {@link RaiseKind#token} ({@code LG#<n>}) -- no kind name, no value -- and the plan carries
 * what the token means: its kind and the operands its raise was built from (its where went 2026-11-21, stage B's BE:
 * the operands hold its facts). A site carries no words: each audience words it after the run from the plan's record of
 * it (stage B's
 * B1, 2026-11-20: lite's sentence when the raise is read back, legend-pure's the compatibility edge's formatter's).
 */
public record RaiseSite(int n, RaiseKind kind, List<SqlExpr> operands) {

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
}
