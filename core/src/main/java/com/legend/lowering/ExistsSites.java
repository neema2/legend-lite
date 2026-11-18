// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A statement's sites an EXISTS join reads (the lens audit's item 1, 2026-11-18): the lowering's alias minter and each
 * conjunct's resolver zones, in the sql layer's terms. The lowering hands them to its forms' {@link LoweringForms#existsJoin}
 * and never the rewrite; the edge's form writes legend-engine's join to the distinct keys over them
 * ({@code sql.ExistsJoins}). A record of the lowering's own data, outside the two-form kind (criterion (a): types only).
 */
public record ExistsSites(Supplier<String> alias,
        Function<SqlExpr, com.legend.sql.ExistsJoins.@com.legend.base.Nullable Zones> zones) {
}
