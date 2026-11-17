// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.compiler.element.type.Type;

/**
 * WHETHER TWO SINGLE NUMBERS OF DIFFERENT FINE KINDS compare by their texts (register S127, the user's ruling of
 * 2026-11-04; census C1, the orchestrator's ruling of 2026-11-17): a two-form kind ({@code PlanStyle.twoForm},
 * {@link PlanStyleFacets#TWO_FORMS}) -- the product's form answers never ({@link #NEVER}: the type is part of the
 * value), the edge's compiled legend-pure's rule ({@code CompiledSupport.eq(Number, Number)}: an Integer and a Decimal
 * by their texts). The database judge asks it at planning. Signatures only, and the product's form.
 */
public interface CrossKindNumbers {

    /** The product's form: two numbers of different fine kinds are never equal. */
    CrossKindNumbers NEVER = (e, a) -> false;

    /** Whether {@code e} and {@code a}, two different fine number kinds (Integer, Float, Decimal), compare by their
     *  texts; false: never equal. */
    boolean byText(Type e, Type a);
}
