// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * A PRESENT NULL CELL in an expected value (census C5's step C, C-2, 2026-11-17, the orchestrator's ruling (B)): Pure's
 * {@code ^TDSNull()} instance, or a golden grid's null cell read as input, standing as an element of an assert's
 * EXPECTED side. Minted by the compiler alone ({@code VerdictQueries}: Invariant 7), never for a user expression. A
 * present null cell is not SQL NULL, which is an ABSENT element in a verdict statement: it lowers to lite's null-cell
 * marker, which the judges' canon writes as the bare token {@code null} ({@code lowering.CanonicalRenderSql.NULL_CELL}).
 */
public record TypedNullCell(ExprType info) implements TypedSpec {
    @Override
    public List<TypedSpec> children() {
        return List.of();
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 0, "TypedNullCell");
        return this;
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedNullCell(info);
    }
}
