// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * THE PLAN STYLE'S TEXT FACETS, AS THE LOWERING'S STATIC HELPERS READ THEM (the separation census's B8, 2026-11-17;
 * the seam agreed with E-Java and E-SQL, the orchestrator's rule: an explicit style from every caller, no implicit
 * default). The Lowerer builds one from the planner's style ({@code PlanEnv.configure}) and hands it to the native
 * rules ({@code Scalars.lower}); a judge's lowering gets the planning environment's ({@code PlanEnv.styled}); the
 * product's own text helpers name {@link #PRODUCT}. Each facet is asked with the call's own refusal.
 *
 * @param pureOnly the texts only legend-pure writes ({@link PureOnlyTexts}): the edge's answers, the product's style
 *                 throwing the call's refusal
 */
public record Styled(Function<Supplier<? extends RuntimeException>, PureOnlyTexts> pureOnly) {

    /** The product's own style: each text only legend-pure writes refused, by its call's own refusal. */
    public static final Styled PRODUCT = new Styled(refusal -> {
        throw refusal.get();
    });
}
