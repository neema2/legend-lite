// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.type.Type;

/**
 * COMPILED LEGEND-PURE'S NUMBER EQUALITY across kinds (register S127; census C1, the orchestrator's ruling of
 * 2026-11-17): {@code CompiledSupport.eq(Number, Number)}, what a test's assert runs upstream -- an Integer and a
 * Decimal equal by their texts ({@code 1D == 1}), a Float equal to neither. The edge's form of the two-form kind
 * {@link com.legend.testplan.CrossKindNumbers}, moved from the database judge's compatibility branch.
 */
public enum CompiledPureNumbers implements com.legend.testplan.CrossKindNumbers {
    INSTANCE;

    @Override
    public boolean byText(Type e, Type a) {
        return e == Type.Primitive.INTEGER && a == Type.Primitive.DECIMAL
                || e == Type.Primitive.DECIMAL && a == Type.Primitive.INTEGER;
    }
}
