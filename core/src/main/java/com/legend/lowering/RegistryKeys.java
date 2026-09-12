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
    public static Set<String> scalarRules() {
        return Collections.unmodifiableSet(Scalars.ruleKeys());
    }

    /** {@code Aggregates.REDUCERS} keys — SQL aggregates. */
    public static Set<String> reducers() {
        return Collections.unmodifiableSet(Aggregates.reducerKeys());
    }

    /** {@code Windows.FNS} keys — window functions. */
    public static Set<String> windowFunctions() {
        return Collections.unmodifiableSet(Windows.fnKeys());
    }

    /** {@code Windows.AGGREGATES} keys — window-only aggregates. */
    public static Set<String> windowAggregates() {
        return Collections.unmodifiableSet(Windows.aggregateKeys());
    }
}
