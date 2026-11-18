// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE PRODUCT'S FORM OF {@link ExecutedValues} (train 17 item 2, 2026-11-18): refused by name -- executeLegendQuery's
 * result is legend-engine's JSON; the product writes a query's rows by its result writers.
 */
public enum RefusedExecutedValues implements ExecutedValues {
    REFUSED;

    @Override
    public TypedSpec values(TypedSpec chain, ModelContext model) {
        throw com.legend.lowering.ProductLoweringForms.envelopeRefused();
    }

    @Override
    public TypedSpec relation(TypedSpec chain, java.util.function.Supplier<@com.legend.base.Nullable String> activitySql) {
        throw com.legend.lowering.ProductLoweringForms.envelopeRefused();
    }

    @Override
    public TypedSpec classes(TypedSpec chain, java.util.function.Supplier<@com.legend.base.Nullable String> activitySql) {
        throw com.legend.lowering.ProductLoweringForms.envelopeRefused();
    }
}
