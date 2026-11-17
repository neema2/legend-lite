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
 * @param plain     the raises the style writes as the plain operation ({@code PlanStyle.plainRaises}, B7, 2026-11-17):
 *                  the product's none; the edge's pure/v1 style legend-engine's census class A
 * @param dateTimes a DateTime's text in a message, a two-form hook ({@link DateTimeTexts}, census B9, 2026-11-17): the
 *                  product's lite text, the edge's legend-pure's
 */
public record Styled(Function<Supplier<? extends RuntimeException>, PureOnlyTexts> pureOnly,
        java.util.Set<com.legend.sql.RaiseKind> plain, DateTimeTexts dateTimes) {

    public Styled {
        plain = plain.isEmpty() ? java.util.Set.of() : java.util.Collections.unmodifiableSet(java.util.EnumSet.copyOf(plain));
    }

    /** The product's form of a DateTime's text: lite's canonical text, {@code DATETIME_TO_TEXT}, the value alone (a
     *  literal's written text is carried as the node's second operand, which the product never reads). */
    public static final DateTimeTexts LITE_DATE_TIMES = (x, written) -> written == null
            ? com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.DATETIME_TO_TEXT, x)
            : com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.DATETIME_TO_TEXT, x,
                    new com.legend.sql.SqlExpr.StringLit(written));

    /** The product's own style: each text only legend-pure writes refused, by its call's own refusal; every check of
     *  lite's written (no plain raise, {@code PlanStyle.plainRaises}); lite's text of a DateTime. */
    public static final Styled PRODUCT = new Styled(refusal -> {
        throw refusal.get();
    }, java.util.Set.of(), LITE_DATE_TIMES);
}
