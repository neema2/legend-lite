// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.model.FunctionId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Remediation T1.7 — aggregate membership is the reducer CATALOG, never
 * a parallel name list. The deleted AGG_FQNS list missed stdDev,
 * variance, mode, corr and friends: an aggregate the wall could not see
 * silently mis-resolved ("max() &gt; 30 becoming any-match" class).
 * Since execution plan step 2 (2026-09-26) the registry is keyed by
 * declaration identity and a test names the catalog's generated overload
 * groups, never a bare name.
 */
class AggregatesCatalogTest {

    @Test
    @DisplayName("names the old hand list missed are reducers")
    void previouslyMissedNamesAreReducers() {
        for (var group : List.of(Pure.AT_MATH_STD_DEV, Pure.AT_MATH_VARIANCE, Pure.AT_MATH_MODE,
                Pure.AT_MATH_CORR, Pure.AT_MATH_COVAR_SAMPLE, Pure.AT_MATH_COVAR_POPULATION)) {
            var ids = group;
            assertFalse(ids.isEmpty(), "an overload group is never empty");
            ids.forEach(k -> assertTrue(Aggregates.isReducerKey(k),
                    k + " must count as an aggregate"));
        }
    }

    @Test
    @DisplayName("non-aggregates stay out")
    void nonAggregatesStayOut() {
        Pure.AT_STRING_TO_UPPER.forEach(k ->
                assertFalse(Aggregates.isReducerKey(k)));
    }
}
