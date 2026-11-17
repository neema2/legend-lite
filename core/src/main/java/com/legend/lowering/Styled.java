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
 * @param pureOnly  the texts only legend-pure writes ({@link PureOnlyTexts}): the edge's answers, the product's style
 *                  throwing the call's refusal
 * @param forms     the lowering's two-form choices ({@link LoweringForms}, the audit's K5, 2026-11-17; until then the
 *                  set of raises the style wrote plain, B7): the product's {@link LoweringForms#PRODUCT}, every check
 *                  written; the edge's pure/v1 style legend-engine's census class A plain
 * @param dateTimes a DateTime's text in a message, a two-form hook ({@link DateTimeTexts}, census B9, 2026-11-17): the
 *                  product's lite text, the edge's legend-pure's
 * @param lists     a list's text, a two-form kind ({@link ListTexts}, census B8, 2026-11-17): the product's JSON array,
 *                  the edge's {@code [a, b]}
 */
public record Styled(Function<Supplier<? extends RuntimeException>, PureOnlyTexts> pureOnly,
        LoweringForms forms, DateTimeTexts dateTimes, ListTexts lists) {

    /** The product's form of a DateTime's text: lite's canonical text, {@code DATETIME_TO_TEXT}, the value alone. A
     *  literal's written text is the edge's alone (legend-pure prints a literal at its written precision): the product's
     *  node carries no operand only the edge reads (the audit's K15, 2026-11-17). The {@code written} argument is the edge's
     *  form's alone: the product's ignores it by design (the edge audit's J26, 2026-11-17). */
    public static final DateTimeTexts LITE_DATE_TIMES = (x, written) ->
            com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.DATETIME_TO_TEXT, x);

    /** The product's own style: each text only legend-pure writes refused, by its call's own refusal; every check of
     *  lite's written (no plain raise, {@link LoweringForms#PRODUCT}); lite's text of a DateTime. */
    public static final Styled PRODUCT = new Styled(refusal -> {
        throw refusal.get();
    }, LoweringForms.PRODUCT, LITE_DATE_TIMES, ProductListTexts.INSTANCE);
}
