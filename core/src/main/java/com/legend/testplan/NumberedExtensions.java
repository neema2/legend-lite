// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.executionplan.Extension;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * A BODY'S NUMBERED EDGE RECORDS (the plan-as-data design's step 8b, 2026-11-08; census C2, 2026-11-17: the edge's
 * appeals are opaque to the product): each record the edge planned for an assert, numbered in the body as its arm
 * planned it -- the number its verdict's judgment, or its database root's row, names -- and the plan's
 * {@code extensionsByVerdict} built from them. One per body, shared by every copy of its environment. The records are
 * the edge's ({@link Extension}); the product holds them by number and names no appeal type.
 */
final class NumberedExtensions {

    private final Map<Integer, Extension> planned = new LinkedHashMap<>();
    private int last;

    /** The next number, the record {@code numbered} makes with it held under it. */
    int add(IntFunction<Extension> numbered) {
        last++;
        planned.put(last, numbered.apply(last));
        return last;
    }

    /** The record numbered {@code n}. */
    Extension of(int n) {
        Extension e = planned.get(n);
        if (e == null) {
            throw new IllegalStateException("no edge record numbered " + n + " planned in the body");
        }
        return e;
    }

    /** The records {@code plan}'s verdicts name, by verdict id. */
    Map<Integer, List<Extension>> byVerdict(TestPlan plan) {
        Map<Integer, List<Extension>> out = new LinkedHashMap<>();
        com.legend.testexecutionplan.NamedExtensions.named(plan).forEach((verdict, numbers) -> {
            List<Extension> at = new ArrayList<>();
            for (int n : numbers) {
                at.add(of(n));
            }
            out.put(verdict, at);
        });
        return out;
    }
}
