// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Lean;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A21 (Phase 8): the ONE keyed-result collector for the checker
 * integration tests — the three copy-pasted versions built a
 * {@code HashMap} whose {@code put} silently overwrote duplicate keys,
 * so a query wrongly returning two rows for one group key PASSED. A
 * duplicate key now throws; insertion order is kept (LinkedHashMap) so
 * a test that wants to assert order can.
 */
final class CheckerResults {

    private CheckerResults() {
    }

    static <K> Map<K, Object> collect(Lean.Result result,
            String keyCol, String valCol) {
        int keyIdx = columnIndex(result, keyCol);
        int valIdx = columnIndex(result, valCol);
        Map<K, Object> map = new LinkedHashMap<>();
        for (var row : result.rows()) {
            @SuppressWarnings("unchecked")
            K key = (K) row.get(keyIdx);
            if (map.containsKey(key)) {
                // a second row for one key is a result-shape defect this
                // collector must not repair (the old HashMap.put
                // silently kept the last row)
                throw new AssertionError("duplicate key '" + key + "' in "
                        + keyCol + " — the query returned more than one"
                        + " row for one key");
            }
            map.put(key, row.get(valIdx));
        }
        return map;
    }

    static int columnIndex(Lean.Result result, String name) {
        for (int i = 0; i < result.columns().size(); i++) {
            if (name.equals(result.columns().get(i).name())) {
                return i;
            }
        }
        throw new AssertionError("Column '" + name + "' not found in "
                + result.columns());
    }
}
