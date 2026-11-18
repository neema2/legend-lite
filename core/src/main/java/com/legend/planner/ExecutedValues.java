// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE STRING ENTRY'S VALUE OF A SERIALIZED ROOT, A TWO-FORM KIND (train 17 item 2, the orchestrator's ruling of
 * 2026-11-18: until then the compiler minted legend-engine's json-builder envelope as TypedJsonResult's VALUES kind):
 * the product refuses it by name ({@link RefusedExecutedValues}), the edge writes legend-engine's envelope around the
 * serialized text. Signatures only (HookRulingTest).
 */
public interface ExecutedValues {

    /** The string entry's value over {@code chain}, a serialize under its from() envelopes: one String, typed. */
    TypedSpec envelope(TypedSpec chain, ModelContext model);
}
