// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

/**
 * THE TEXT OF A GRAPH FETCH'S KEY ARGUMENT (phase D's D1.2, 2026-11-15; the orchestrator under the user's ruling of
 * 2026-11-10, register S312: "a string means one simple text per type", Pure's forms at the edge): a qualified
 * property's key ({@code synonyms(2023-10-15T00:00:00.000000000Z)}) spells each literal argument in the plan's style --
 * the product's own text of its type, the compatibility edge legend-engine's ({@code +0000}, a quoted String). Handed
 * to the resolver by its planner from the plan's style ({@code BodyPlanner.resolver}); a resolver given none refuses a
 * key that needs it, by name. {@code date}: a date argument's text from the literal's written text
 * ({@code PureDateLiteral.canonicalText}, no zone); {@code string}: a String argument's text.
 */
public record GraphKeyText(java.util.function.UnaryOperator<String> date,
        java.util.function.UnaryOperator<String> string) {

    public GraphKeyText {
        java.util.Objects.requireNonNull(date, "a date argument's text");
        java.util.Objects.requireNonNull(string, "a String argument's text");
    }
}
