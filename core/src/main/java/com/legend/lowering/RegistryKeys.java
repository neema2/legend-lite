// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import java.util.Collections;
import java.util.Set;

/**
 * THE ONE PUBLIC FACE of the four catalog-keyed lowering registries — their
 * KEYS, read-only: which catalog overloads have a SQL expression rule, a
 * reducer, a window function, a window-only aggregate. The set of implemented
 * keys is the fact the platform claims to expose; the registries themselves
 * stay package-private. Read by the claims registry (spec,
 * {@code com.legend.claims.Claims}) to compute the claims ledger — until batch
 * 7 (2026-09-11) that reader was a helper in core's TEST tree of this package,
 * and the spec module would have needed core's test jar to see it.
 */
public final class RegistryKeys {

    private RegistryKeys() {
    }

    /** {@code Scalars.RULES} keys — SQL expression rules. */
    public static Set<com.legend.model.FunctionId> scalarRules() {
        return Collections.unmodifiableSet(Scalars.ruleKeys());
    }

    /** {@code Aggregates.REDUCERS} keys — SQL aggregates. */
    public static Set<com.legend.model.FunctionId> reducers() {
        return Collections.unmodifiableSet(Aggregates.reducerKeys());
    }

    /** {@code Windows.FNS} keys — window functions. */
    public static Set<com.legend.model.FunctionId> windowFunctions() {
        return Collections.unmodifiableSet(Windows.fnKeys());
    }

    /** {@code Windows.AGGREGATES} keys — window-only aggregates. */
    public static Set<com.legend.model.FunctionId> windowAggregates() {
        return Collections.unmodifiableSet(Windows.aggregateKeys());
    }

    /** {@code FeatureRules.UNDER} — per execution feature flag, the keys whose
     *  scalar rule the flag replaces. */
    public static java.util.Map<com.legend.platform.Feature, Set<com.legend.model.FunctionId>> featureOverrides() {
        java.util.Map<com.legend.platform.Feature, Set<com.legend.model.FunctionId>> out =
                new java.util.EnumMap<>(com.legend.platform.Feature.class);
        for (var e : FeatureRules.UNDER.entrySet()) {
            out.put(e.getKey(), Set.copyOf(e.getValue().keySet()));
        }
        return Collections.unmodifiableMap(out);
    }
}
