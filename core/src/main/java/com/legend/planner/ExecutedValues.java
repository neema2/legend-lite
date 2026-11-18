// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE STRING ENTRY'S VALUE, A TWO-FORM KIND (train 17 item 2, the orchestrator's ruling of 2026-11-18: until then
 * the compiler minted legend-engine's envelopes as TypedJsonResult's VALUES, TDS and CLASS kinds, and the planner
 * rendered the activity's SQL eagerly for every string entry): the product refuses each by name
 * ({@link RefusedExecutedValues}), the edge writes legend-engine's envelope -- a relation's and a class root's with
 * the activity's SQL, which only the edge's form asks for ({@code activitySql}, rendered when asked). Signatures only
 * (HookRulingTest).
 */
public interface ExecutedValues {

    /** The string entry's value over {@code chain}, a serialize under its from() envelopes: one String, typed. */
    TypedSpec values(TypedSpec chain, ModelContext model);

    /** The string entry's value over {@code chain}, a relation: its envelope with the activity's SQL, or none. */
    TypedSpec relation(TypedSpec chain, java.util.function.Supplier<@com.legend.base.Nullable String> activitySql);

    /** The string entry's value over {@code chain}, a class root's objects: its envelope with the activity's SQL. */
    TypedSpec classes(TypedSpec chain, java.util.function.Supplier<@com.legend.base.Nullable String> activitySql);
}
