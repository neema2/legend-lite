// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A statement's sites an EXISTS join reads (the lens audit's item 1, 2026-11-18): the lowering's alias minter and how a
 * DISTINCT-key subselect's WHERE is written from the statement's WHERE merges ({@link WhereZones#keysWhere}). The lowering hands them to its forms' {@link LoweringForms#existsJoin}
 * and never the rewrite; the edge's form writes legend-engine's join to the distinct keys over them
 * ({@code sql.ExistsJoins}). A record of the lowering's own data, outside the two-form kind (criterion (a): types only).
 */
public record ExistsSites(Supplier<String> alias,
        java.util.function.BiFunction<java.util.List<SqlExpr>, SqlExpr, @com.legend.base.Nullable SqlExpr> keysWhere) {
}
