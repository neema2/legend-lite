// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

/**
 * THE WORD A RENDERED GOLDEN WRITES FOR A NULL CELL (census C1, the orchestrator's ruling of 2026-11-17:
 * legend-engine's {@code TDSNull} read as input by the edge alone): a two-form kind ({@code PlanStyle.twoForm}, {@code
 * testplan.PlanStyleFacets.TWO_FORMS}). The product's form, {@link LiteGoldenNullWord#NONE}: lite's texts write a null
 * cell empty, so no word names one; the edge's: legend-engine's {@code TDSNull}. Signatures only; the product's form is
 * {@code LiteGoldenNullWord}.
 */
public interface GoldenNullWord {

    /** Whether {@code cell}, a golden's cell or element text, is the word for a null cell. */
    boolean isNullWord(String cell);
}
