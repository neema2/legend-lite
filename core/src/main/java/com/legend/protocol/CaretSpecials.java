// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * The three classes legend-engine's grammar never writes a {@code ^X(...)} for: it writes the call that builds the same
 * object ({@code DomainParseTreeWalker}; ProbeWireShapes "caret specials") -- {@code ^Pair(first=a, second=b)} is
 * {@code pair(a, b)}, {@code ^BasicColumnSpecification(func=f, name=n[, documentation=d])} is {@code col(f, n[, d])},
 * {@code ^TdsOlapRank(func=f)} is {@code tds::func(f)}: the keys in that order, whatever order they were written in,
 * matched by the class's simple or full name exactly as spelled. One rule, for the emitter (which writes that call) and
 * the printer (which prints it).
 */
final class CaretSpecials {

    private CaretSpecials() {
    }

    /**
     * The call the engine writes for {@code ni}, or {@code null} when {@code ni} is no special class's. A required key
     * that is missing has no rule ({@code col}'s {@code documentation} alone is optional, the engine's
     * {@code select(nonNull)}).
     */
    static @com.legend.base.Nullable AppliedFunction call(NewInstance ni) {
        String spelled = ni.className();
        if ("Pair".equals(spelled) || "meta::pure::functions::collection::Pair".equals(spelled)) {
            return call(ni, "meta::pure::functions::collection::pair", new String[]{"first", "second"}, false);
        }
        if ("BasicColumnSpecification".equals(spelled) || "meta::pure::tds::BasicColumnSpecification".equals(spelled)) {
            return call(ni, "meta::pure::tds::col", new String[]{"func", "name", "documentation"}, true);
        }
        if ("TdsOlapRank".equals(spelled) || "meta::pure::tds::TdsOlapRank".equals(spelled)) {
            return call(ni, "meta::pure::tds::func", new String[]{"func"}, false);
        }
        return null;
    }

    private static AppliedFunction call(NewInstance ni, String function, String[] keys, boolean dropMissing) {
        List<ValueSpecification> params = new ArrayList<>(keys.length);
        for (String key : keys) {
            KeyExpression ke = ni.first(key);
            if (ke == null && dropMissing) {
                continue;              // engine's select(nonNull) -- col's documentation
            }
            if (ke == null) {
                throw new UnsupportedOperationException("no rule for a caret special missing key '" + key + "' (at "
                        + ni.className() + ")");
            }
            params.add(ke.value());
        }
        return new AppliedFunction(function, params, List.of());
    }
}
