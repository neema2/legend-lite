// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;

/**
 * THE PRODUCT'S TEXT PARTS A REPRESENTATION IS COMPOSED FROM (C5's step A, the orchestrator's ruling of 2026-11-17):
 * the one explicit facade through which the compatibility edge's legend-pure representation ({@code toRepresentation},
 * the judges' {@code %r}; {@link PureOnlyTexts#representationOf}, {@link PureOnlyTexts#formatR}) reaches the lowering's
 * own semantics -- each member the product's rule as it is, nothing upstream's. Listed by name in
 * {@code ArchitectureTest}'s entries (no wildcard).
 */
public final class ReprParts {

    private ReprParts() {
    }

    /** The parts concatenated (the lowering's text concatenation, {@code Scalars.cat}). */
    public static SqlExpr cat(SqlExpr... parts) {
        return Scalars.cat(parts);
    }

    /** {@code x}'s text by its type under {@code styled} (the product's {@code toString} rule,
     *  {@code Scalars.pureToString}). */
    public static SqlExpr pureToString(Styled styled, Type t, SqlExpr x) {
        return Scalars.pureToString(styled, t, x);
    }

    /** A date literal's text as written, or null where {@code spec} is none ({@code Scalars.dateLiteralText}). */
    public static @com.legend.base.Nullable SqlExpr dateLiteralText(TypedSpec spec) {
        return Scalars.dateLiteralText(spec);
    }
}
